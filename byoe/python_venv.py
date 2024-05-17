import os, logging, shutil
from pathlib import Path
from io import TextIOWrapper
from typing import Dict, List, Optional, Any

import sh

from .globals import ShellType, SnapId, SnapSpec
from .util import get_env_cmd, get_activated_envrion, make_app_act_script, stash_failed
from .conf import PythonConfig
from .spack import get_spack_env_cmds, unset_implicit_pypath


log = logging.getLogger(__name__)


# TODO: Take the 'python' sh.Command that is configured for spack instead of the
#       'spack_env', then just copy its _env
def get_venv_cmds(
    spack_env: Path,
    py_venv: Path,
    cmds: List[str],
    log_file: Optional[TextIOWrapper] = None,
) -> List[sh.Command]:
    """Get a command inside spack env / python venv"""
    act_scripts = [
        (spack_env.parent / f"{spack_env.name}-activate.sh").read_text(),
        (py_venv / "bin" / "activate").read_text(),
    ]
    act_env = get_activated_envrion(act_scripts)
    unset_implicit_pypath(spack_env, act_env)
    env_bin = py_venv / "bin"
    return [get_env_cmd(env_bin / cmd, act_env, log_file=log_file) for cmd in cmds]


# TODO: Make this more generic (i.e. don't pass in spack_snap), but do need some way
#       to pass in environment modifications too.
def update_python_env(
    env_name: str,
    python_config: PythonConfig,
    spack_snap: Path,
    locs: Dict[str, Path],
    snap_id: SnapId,
    log_file: Optional[TextIOWrapper] = None,
) -> Optional[SnapSpec]:
    wheels_dir = locs["python_cache"]
    wheels_dir.mkdir(parents=True, exist_ok=True)
    snap_path = locs["envs"] / "python" / env_name / str(snap_id)
    snap_path.parent.mkdir(exist_ok=True, parents=True)
    python = get_spack_env_cmds(spack_snap.snap_dir, ["python"], log_file=log_file)[0]
    kwargs = {}
    sys_pkgs = python_config.system_packages
    log.debug("Creating venv: %s", snap_path)
    if sys_pkgs:
        log.debug("Using --system-site-packages")
        kwargs["system_site_packages"] = True
    build_err: Optional[Exception] = None
    sys_req_path = main_req_path = lock_path = None
    try:
        python("-m", "venv", snap_path, **kwargs)
        pip = get_venv_cmds(spack_snap.snap_dir, snap_path, ["pip"], log_file)[0]
        pip.install("-U", "pip")
        pip.install("pip-tools")
        pip_compile, pip_sync = get_venv_cmds(
            spack_snap.snap_dir, snap_path, ["pip-compile", "pip-sync"], log_file
        )
        if sys_pkgs:
            sys_req_path = locs["envs"] / "python" / env_name / f"{snap_id}-sys-req.txt"
            with open(sys_req_path, "wt") as out_f:
                out_f.write(pip.list(format="freeze"))
        main_req_path = locs["envs"] / "python" / env_name / f"{snap_id}-main-req.in"
        with open(main_req_path, "wt") as out_f:
            if sys_req_path:
                out_f.write(f"-c {sys_req_path}\n")
            for spec in python_config.specs:
                out_f.write(f"{spec}\n")
        lock_path = locs["envs"] / "python" / env_name / f"{snap_id}-requirements.txt"
        log.info("Running pip-compile for venv: %s", snap_path)
        pip_compile(
            main_req_path,
            output_file=str(lock_path),
            generate_hashes=python_config.generate_hashes,
            allow_unsafe=True,
            verbose=True,
        )
        log.info("Running pip-sync to build venv: %s", snap_path)
        pip_sync(str(lock_path), pip_args=f"--find-links {wheels_dir}")
    except Exception as e:
        build_err = e
        log.exception("Python venv update failed: %s", snap_path)
    if snap_path.exists():
        log.debug("Updating python wheels dir")
        try:
            pip.wheel(find_links=str(wheels_dir), w=str(wheels_dir), r=str(lock_path))
        except:
            log.exception("Error while building wheels from env: %s", snap_path)
    if build_err is not None:
        stash_failed(sys_req_path, main_req_path, lock_path)
        if snap_path.exists():
            shutil.rmtree(snap_path)
        return None
    return SnapSpec.from_lock_path(lock_path)


def update_python_app(
    app_name: str,
    python_config: PythonConfig,
    pipx: sh.Command,
    python: sh.Command,
    locs: Dict[str, Path],
    snap_id: SnapId,
) -> Optional[SnapSpec]:
    """Create updated snapshot of an isolated Python app"""
    snap_dir = locs["apps"] / "python" / app_name / str(snap_id)
    snap_dir.mkdir(parents=True)
    bin_dir = snap_dir / "bin"
    bin_dir.mkdir()
    man_dir = snap_dir / "man"
    man_dir.mkdir()
    pipx._partial_call_args["env"].update(
        {"PIPX_HOME": snap_dir, "PIPX_BIN_DIR": bin_dir, "PIPX_MAN_DIR": man_dir}
    )
    kwargs = {
        "python": str(python),
        "pip_args": f"--find-links {locs['python_cache']}",
        "system_site_packages": python_config.system_packages,
    }
    log.info("Doing pipx install for app: %s", app_name)
    try:
        pipx.install(*python_config.specs, **kwargs)
    except Exception:
        log.exception("Error building python app: %s", app_name)
        shutil.rmtree(snap_dir)
        return None
    pipx_venv_dir = snap_dir / "venvs" / app_name
    env_dir = locs["envs"] / "python" / app_name / str(snap_id)
    env_dir.parent.mkdir(exist_ok=True, parents=True)
    env_dir.symlink_to(
        os.path.relpath(pipx_venv_dir, env_dir.parent), target_is_directory=True
    )
    act_path = env_dir / "bin" / "activate"
    app_lock_path = locs["apps"] / "python" / app_name / f"{snap_id}-requirements.txt"
    env_lock_path = locs["envs"] / "python" / app_name / f"{snap_id}-requirements.txt"
    env_python = get_env_cmd("python", get_activated_envrion([act_path.read_text()]))
    app_lock_path.write_text(env_python("-m", "pip", "freeze"))
    env_lock_path.symlink_to(os.path.relpath(app_lock_path, env_lock_path.parent))
    app_snap = SnapSpec.from_lock_path(app_lock_path)
    # Make app activation scripts
    for shell_type in ShellType:
        act_path = app_snap.get_activate_path(shell_type)
        act_path.write_text(make_app_act_script(snap_dir, shell_type))
    return app_snap
