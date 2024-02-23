import logging, shutil
from pathlib import Path
from copy import deepcopy
from io import TextIOWrapper
from typing import Dict, List, Optional, Any

import sh

from .util import get_env_cmd, get_activated_envrion
from .conf import PythonConfig
from .spack import get_spack_env_cmds


log = logging.getLogger(__name__)


def get_venv_cmds(
    spack_env: Path,
    py_venv: Path,
    cmds: List[str],
    log_file: Optional[TextIOWrapper] = None,
) -> List[sh.Command]:
    """Get a command inside spack env / python venv"""
    act_scripts = [
        (spack_env.parent / f"{spack_env.name}_activate.sh").read_text(),
        (py_venv / "bin" / "activate").read_text(),
    ]
    act_env = get_activated_envrion(act_scripts)
    env_bin = py_venv / "bin"
    return [get_env_cmd(env_bin / cmd, act_env, log_file=log_file) for cmd in cmds]


# TODO: Make thie more generic (i.e. don't pass in spack_snap), but do need some way
#       to pass in environment modifications too.
def update_python_env(
    env_name: str,
    python_config: PythonConfig,
    spack_snap: Path,
    locs: Dict[str, Path],
    update_ts: str,
    log_file: Optional[TextIOWrapper] = None,
):
    wheels_dir = locs["wheels_dir"]
    wheels_dir.mkdir(parents=True, exist_ok=True)
    snap_path = locs["envs_dir"] / "python" / f"{env_name}-{update_ts}"
    python = get_spack_env_cmds(spack_snap, ["python"], log_file)[0]
    kwargs = {}
    sys_pkgs = python_config.system_packages
    log.debug("Creating venv: %s", snap_path)
    build_err: Optional[Exception] = None
    try:
        python("-m", "venv", snap_path, **kwargs)
        pip = get_venv_cmds(spack_snap, snap_path, ["pip"], log_file)[0]
        pip.install("-U", "pip")
        pip.install("pip-tools")
        pip_compile, pip_sync = get_venv_cmds(
            spack_snap, snap_path, ["pip-compile", "pip-sync"], log_file
        )
        if sys_pkgs:
            sys_req_path = Path(f"{snap_path}-sys-req.txt")
            with open(sys_req_path, "wt") as out_f:
                out_f.write(pip.list(format="freeze"))
        else:
            sys_req_path = None
        main_req_path = Path(f"{snap_path}-main-req.in")
        with open(main_req_path, "wt") as out_f:
            if sys_req_path:
                out_f.write(f"-c {sys_req_path}\n")
            for spec in python_config.specs:
                out_f.write(f"{spec}\n")
        lock_path = Path(f"{snap_path}-requirements.txt")
        log.info("Running pip-compile for venv: %s", snap_path)
        pip_compile(
            main_req_path,
            output_file=str(lock_path),
            generate_hashes=True,
            allow_unsafe=True,
            verbose=True,
        )
        log.info("Running pip-sync to build venv: %s", snap_path)
        pip_sync(str(lock_path), pip_args=f"--find-links {wheels_dir}")
    except Exception as e:
        build_err = e
        log.error("Python venv update failed: %s", snap_path)
        if snap_path.exists():
            shutil.rmtree(snap_path)
    if snap_path.exists():
        try:
            log.debug("Updating python wheels dir")
            pip.wheel(find_links=str(wheels_dir), w=str(wheels_dir), r=str(lock_path))
        except:
            log.exception("Error while building wheels from env: %s", snap_path)
    if build_err is not None:
        raise build_err
