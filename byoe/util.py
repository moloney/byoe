import os, shlex, json
from datetime import datetime
from pathlib import Path
from io import TextIOWrapper
from typing import List, Dict, Optional, Union

import sh

sh = sh.bake(_tty_out=False)

from ._globals import DEFAULT_SLURM_TASKS


bash = sh.bash
which = sh.which
try:
    srun = sh.srun
    HAS_SLURM = True
except sh.CommandNotFound:
    HAS_SLURM = False


def get_locations(base_dir: Path) -> Dict[str, Path]:
    """Get paths to key locations under base_dir"""
    return {
        "base_dir": base_dir,
        "startup_dir": base_dir / "startup",
        "log_dir": base_dir / "logs",
        "tmp_dir": base_dir / "tmp",
        "lic_dir": base_dir / "licenses",
        "spack_dir": base_dir / "spack",
        "spack_env_dir": base_dir / "spack_envs",
        "spack_pkg_dir": base_dir / "spack_pkgs",
        "conda_dir": base_dir / "conda",
        "conda_env_dir": base_dir / "conda" / "envs",
        "conda_pkg_dir": base_dir / "conda" / "pkgs",
        "python_dir": base_dir / "python",
        "venv_dir": base_dir / "python" / "venvs",
        "wheels_dir": base_dir / "python" / "wheels",
    }


def select_snap(
    snap_dates: List[datetime], period: int, now: Optional[datetime] = None
) -> datetime:
    """Select snapshot date base on the `period` (and `now`)"""
    if now is None:
        now = datetime.now()
    if period > 12:
        if period % 12 != 0:
            raise ValueError(
                "Update periods over 12 months must be evenly divisible by 12"
            )
        period_yrs = period // 12
        tgt_year = now.year - (now.year % period_yrs)
        return datetime(tgt_year)
    if 12 % period != 0:
        raise ValueError("Update periods under 12 months must evenly divide 12")
    tgt_month = now.month - (now.month % period)
    tgt = datetime(now.year, tgt_month, 1)
    min_delta = min_idx = None
    for snap_idx, snap_date in enumerate(snap_dates):
        delta = snap_date - tgt
        if min_delta is None or delta < min_delta:
            min_delta, min_idx = delta, snap_idx
    return snap_dates[min_idx]


def get_activated_envrion(
    activation_scripts: List[str], base_env: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """Get the environment after running one or more scripts in Bash

    DON'T RUN ON UNTRUSTED INPUT!
    """
    if base_env is None:
        base_env = os.environ.copy()
    bash_cmd = "\n".join(
        activation_scripts
        + ['python -c "import json, os ; print(json.dumps(dict(os.environ)))"']
    )
    env_json_str = bash(_in=bash_cmd, _env=base_env)
    return json.loads(env_json_str)


def get_env_cmd(
    cmd: Union[str, Path],
    env: Dict[str, str],
    log_file: Optional[TextIOWrapper] = None,
):
    """Get a command within a modified environment"""
    extra_sh_kwargs = {"_env": env}
    if log_file:
        extra_sh_kwargs["_out"] = log_file
        extra_sh_kwargs["_err"] = log_file
        extra_sh_kwargs["_tee"] = {"err", "out"}
    cmd = Path(cmd)
    if cmd.is_absolute():
        cmd_path = str(cmd)
    else:
        cmd_path = which(str(cmd), _env=env)
    return getattr(sh, cmd_path).bake(**extra_sh_kwargs)


def wrap_cmd(
    wrapper_cmd: sh.Command,
    inner_cmd: sh.Command,
    inject_env: Optional[Dict[str, str]] = None,
) -> sh.Command:
    """Call ``wrapper_cmd`` with ``inner_cmd`` as the final arguments"""
    args = [inner_cmd._path] + inner_cmd._partial_baked_args
    sh_kwargs = {}
    for kw, val in inner_cmd._partial_call_args.items():
        sh_kwargs[f"_{kw}"] = val if not hasattr(val, "copy") else val.copy()
    if inject_env:
        if "_env" not in sh_kwargs:
            sh_kwargs["_env"] = os.environ.copy()
        sh_kwargs["_env"].update(inject_env)
    return wrapper_cmd.bake(args, **sh_kwargs)


def srun_wrap(
    cmd: sh.Command,
    n_cpus: int = 1,
    base_args: str = "",
    tmp_dir: Optional[str] = None,
) -> sh.Command:
    """Wrap existing sh.Command to run on slurm with 'srun'"""
    srun_args = shlex.split(base_args) + ["--cpus-per-task=%s" % n_cpus]
    inject_env = None if tmp_dir is None else {"TMPDIR": tmp_dir}
    return wrap_cmd(srun.bake(srun_args), cmd, inject_env)
