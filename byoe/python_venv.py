"""Manage and build Python environments and apps"""
import os, logging, shutil
from pathlib import Path
from io import TextIOWrapper
from typing import Dict, List, Optional, Any, Tuple

import sh # type: ignore

from .globals import ShellType
from .snaps import SnapId, SnapSpec
from .util import get_cmd, get_activated_envrion, make_app_act_script
from .conf import PythonConfig


log = logging.getLogger(__name__)


def get_venv_cmds(
    python: sh.Command,
    py_venv: Path,
    cmds: List[str],
    log_file: Optional[TextIOWrapper] = None,
) -> List[sh.Command]:
    """Get commands inside a python venv"""
    env_bin = py_venv / "bin"
    return [get_cmd(env_bin / cmd, out_handler=log_file, err_handler=log_file, ref_cmd=python) for cmd in cmds]


def update_python_env(
    env_name: str,
    python_config: PythonConfig,
    python: sh.Command,
    locs: Dict[str, Path],
    snap_id: SnapId,
    best_effort: bool = False,
) -> Tuple[SnapSpec, bool]:
    """Create updated snapshot of python environment"""
    wheels_dir = locs["python_cache"]
    wheels_dir.mkdir(parents=True, exist_ok=True)
    env_dir = locs["envs"] / "python" / env_name
    env_dir.mkdir(exist_ok=True, parents=True)
    snap_path = env_dir / str(snap_id)
    lock_path = env_dir / f"{snap_id}-requirements.txt"
    snap = SnapSpec.from_lock_path(lock_path)
    kwargs = {}
    sys_pkgs = python_config.system_packages
    log.debug("Creating venv: %s", snap_path)
    if sys_pkgs:
        log.debug("Using --system-site-packages")
        kwargs["system_site_packages"] = True
    build_err: Optional[Exception] = None
    sys_req_path = main_req_path = None
    try:
        python("-m", "venv", snap_path, **kwargs)
        pip = get_venv_cmds(python, snap_path, ["pip"])[0]
        pip.install("pip-tools")
        pip_compile, pip_sync = get_venv_cmds(
            python, snap_path, ["pip-compile", "pip-sync"]
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
    except:
        log.error("Error initializing python venv: %s", snap_path)
        snap.stash_failed()
        raise
    log.info("Running pip-compile for venv: %s", snap_path)
    try:
        pip_compile(
            main_req_path,
            output_file=str(lock_path),
            generate_hashes=python_config.generate_hashes,
            allow_unsafe=True,
            verbose=True,
        )
        if snap.dedupe():
            return (snap, True)
    except:
        log.error("Error resolving dependencies for python venv: %s", snap_path)
        snap.stash_failed()
        raise
    log.info("Running pip-sync to build venv: %s", snap_path)
    no_errors = True
    try:
        pip_sync(str(lock_path), pip_args=f"--find-links {wheels_dir}")
    except Exception as e:
        if not best_effort:
            log.error("Python venv build failed: %s", snap_path)
            snap.stash_failed()
        else:
            log.warning("Error occured, but keeping best effort python venv: %s", snap_path)
            no_errors = False
        build_err = e
    if snap_path.exists():
        log.debug("Updating python wheels dir")
        try:
            pip.wheel(find_links=str(wheels_dir), w=str(wheels_dir), r=str(lock_path))
        except:
            log.exception("Error while building wheels from env: %s", snap_path)
    if build_err is not None and not best_effort:
        raise build_err
    return (snap, no_errors)


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
    env_python = get_cmd("python", get_activated_envrion([act_path.read_text()]))
    app_lock_path.write_text(env_python("-m", "pip", "freeze"))
    env_lock_path.symlink_to(os.path.relpath(app_lock_path, env_lock_path.parent))
    app_snap = SnapSpec.from_lock_path(app_lock_path)
    # Make app activation scripts
    for shell_type in ShellType:
        act_path = app_snap.get_activate_path(shell_type)
        act_path.write_text(make_app_act_script(snap_dir, shell_type))
    return app_snap
