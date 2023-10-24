import logging, shutil
from pathlib import Path
from copy import deepcopy
from io import TextIOWrapper
from typing import Dict, List, Optional, Any

import sh

from .util import get_env_cmd, get_activated_envrion
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


def update_venv(
    snap_path: Path,
    env_info: Dict[str, Any],
    spack_snap: Path,
    wheels_dir: Path,
    log_file: Optional[TextIOWrapper] = None,
):
    """Update a python virtual environment"""
    python = get_spack_env_cmds(spack_snap, ["python"], log_file)[0]
    kwargs = {}
    sys_pkgs = env_info.get("system_packages", True)
    if sys_pkgs:
        kwargs["system-site-packages"] = True
    log.debug("Creating venv: %s", snap_path)
    try:
        python("-m", "venv", snap_path, **kwargs)
        pip = get_venv_cmds(spack_snap, snap_path, ["pip"], log_file)[0]
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
            for spec in env_info["specs"]:
                out_f.write(f"{spec}\n")
        lock_path = Path(f"{snap_path}-requirements.txt")
        log.info("Running pip-compile for venv: %s", snap_path)
        pip_compile(main_req_path, output_file=str(lock_path), generate_hashes=True)
        log.info("Running pip-sync to build venv: %s", snap_path)
        pip_sync(str(lock_path), pip_args=f"--find-links {wheels_dir}")
    except:
        #if snap_path.exists():
        #    shutil.rmtree(snap_path)
        raise
    try:
        log.debug("Updating python wheels dir")
        pip.wheel(find_links=str(wheels_dir), w=str(wheels_dir), r=str(lock_path))
    except:
        log.exception("Error while building wheels")


def update_all_venvs(
    update_ts: str,
    conf: Dict[str, Any],
    path_locs: Dict[str, Path],
    spack_snaps: Dict[str, Path],
    n_tasks: Optional[int] = None,
    log_file: Optional[TextIOWrapper] = None,
):
    """Update all python virtual environments"""
    py_info = conf.get("python")
    if not py_info:
        return
    wheels_dir = path_locs["wheels_dir"]
    wheels_dir.mkdir(parents=True, exist_ok=True)
    for env_name, env_info in py_info.get("envs", {}).items():
        env_info = deepcopy(env_info)
        if "specs" in env_info:
            env_info["specs"] += py_info.get("global_specs", [])
        else:
            env_info["specs"] = py_info["global_specs"]
        spack_snap = spack_snaps[env_info.get("spack_snap", "default")]
        snap_path = path_locs["venv_dir"] / f"{env_name}-{update_ts}"
        update_venv(
            snap_path,
            env_info,
            spack_snap,
            path_locs["wheels_dir"],
            log_file,
        )
