import os, logging
from pathlib import Path
from copy import deepcopy
from typing import Dict, Any

import sh
from sh import CommandNotFound
sh = sh.bake(_tty_out=False)
try:
    srun = sh.srun
    HAS_SLURM = True
except CommandNotFound:
    HAS_SLURM = False

from .util import get_env_cmd


log = logging.getLogger(__name__)


def get_micromamba(path_locs: Dict[str, Path]):
    mamba_activate_path = path_locs["conda_dir"] / "load_micromamba.sh"
    # TODO: this function expects a spack env, spack load also requires base spack env setup
    micromamba = get_env_cmd(micromamba)
    return micromamba


def update_conda_env(conda_cmd: sh.Command, snap_path: Path, env_info):
    """Create updated snapshot of a conda environment"""
    conda_cmd.create()
    # TODO


def update_all_conda_envs(
    update_ts: str, 
    conf_data: dict[str, Any], 
    path_locs: Dict[str, Path], 
):
    conda_info = conf_data.get("conda", {})
    if not conda_info:
        return
    for env_name, env_info in conda_info.get("envs", {}).items():
        env_info = deepcopy(env_info)
        snap_name = f"{env_name}-{update_ts}"
        pass # TODO
