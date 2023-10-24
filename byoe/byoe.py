"""Bring Your Own Environment"""
import logging
from io import TextIOWrapper
from enum import Enum
from pathlib import Path
from typing import Any, List, Dict, Optional

import typer
import yaml
import sh

sh = sh.bake(_tty_out=False)
git = sh.git

from ._globals import DEFAULT_CONF_PATHS
from .util import get_locations
from .spack import (
    update_compiler_conf,
    get_spack,
    get_spack_install,
    update_spack_envs,
)
from .python_venv import update_all_venvs
from .conda import update_all_conda_envs


log = logging.getLogger(__name__)


class UpdateChannels(Enum):
    BLOODY = "bloody"
    FRESH = "fresh"
    STABLE = "stable"
    STALE = "stale"
    OLD = "old"


DEFUALT_UPDATE_MONTHS = {
    UpdateChannels.BLOODY: 1,
    UpdateChannels.FRESH: 3,
    UpdateChannels.STABLE: 6,
    UpdateChannels.STALE: 12,
    UpdateChannels.OLD: 24,
}


def get_config(conf_paths: Optional[List[Path]] = None) -> Dict[str, Any]:
    if conf_paths is None:
        conf_paths = DEFAULT_CONF_PATHS
    res = {}
    for p in conf_paths:
        try:
            with open(p, "rt") as conf_f:
                log.debug("Loading config from: %s", p)
                res.update(yaml.safe_load(conf_f))
            break
        except FileNotFoundError:
            continue
        except:
            log.exception("Exception trying to read config: %s", p)
            continue
    res["base_dir"] = Path(res["base_dir"])
    return res


class NoCompilerFoundError(Exception):
    pass


def prep_base_dir(
    conf_data: Dict[str, Any],
    n_tasks: Optional[int] = None,
    log_file: Optional[TextIOWrapper] = None,
):
    """Make sure base_dir / spack / mamba are available and configured"""
    locs = get_locations(conf_data["base_dir"])
    locs["log_dir"].mkdir(exist_ok=True)
    locs["lic_dir"].mkdir(exist_ok=True)
    locs["tmp_dir"].mkdir(exist_ok=True)
    spack_dir = locs["spack_dir"]
    spack_url = conf_data["spack_repo"]["url"]
    spack_branch = conf_data["spack_repo"].get("branch")
    if not spack_dir.exists():
        log.info("Cloning spack into: %s", spack_dir)
        kwargs = {}
        if spack_branch:
            kwargs["branch"] = spack_branch
        git.clone(spack_url, spack_dir, **kwargs)
    else:
        log.info("Pulling updates into spack repo")
        git("-C", f"{locs['spack_dir']}", "pull", "-m", "BYOE Auto merge, don't commit!")
    spack_lic_dir = locs["spack_dir"] / "etc" / "spack" / "licenses"
    if not spack_lic_dir.exists():
        spack_lic_dir.symlink_to("../../../licenses", True)
    for sect_name, sect_data in conf_data["spack"].items():
        if sect_name in ("envs", "global_specs", "slurm_build", "externals"):
            continue
        conf_path = locs["spack_dir"] / "etc" / "spack" / f"{sect_name}.yaml"
        with open(conf_path, "wt") as conf_f:
            yaml.safe_dump({sect_name: sect_data}, conf_f)
    spack = get_spack(locs, log_file=log_file)
    use_slurm = conf_data.get("build_on_slurm", True)
    slurm_opts = conf_data["spack"].get("slurm_opts", {})
    use_slurm = use_slurm and slurm_opts.get("enabled", True)
    spack_install = get_spack_install(
        spack,
        n_tasks=n_tasks,
        use_slurm=use_slurm,
        slurm_opts=slurm_opts.get("install", {}),
    )
    log.info("Bootstrapping spack")
    spack.bootstrap.now()
    log.info("Looking for externals")
    for external in conf_data["spack"].get("externals", []):
        spack.external.find(external, scope="site")
    log.info("Checking binutils")
    binutils_vers = conf_data.get("binutils_version", "2.40")
    try:
        binutils_loc = spack.location(first=True, i=f"binutils@{binutils_vers}")
    except sh.ErrorReturnCode:
        log.info("Updating binutils")
        spack_install([f"binutils@{binutils_vers}", "+ld"])
        binutils_loc = spack.location(first=True, i=f"binutils@{binutils_vers}")
    binutils_loc = Path(binutils_loc.strip())
    log.info("Checking compilers")
    spack.compiler.find(scope="site")
    compilers = [x.strip() for x in spack.compiler.list().split("\n")[2:] if x]
    if len(compilers) == 0:
        raise NoCompilerFoundError()
    # Install compilers as needed, configuring them to use our updated binutils
    needed_compilers = []
    for comp in (
        conf_data["spack"].get("packages", {}).get("all", {}).get("compiler", [])
    ):
        needed_compilers.append(comp)
    for env_info in conf_data["spack"].get("envs", {}).values():
        for comp in env_info.get("packages", {}).get("all", {}).get("compiler", []):
            needed_compilers.append(comp)
    needed_compilers = [x for x in needed_compilers if x not in compilers]
    if needed_compilers:
        for comp in needed_compilers:
            log.info("Installing compiler: %s", comp)
            spack_install([comp])
            spack.compiler.find(
                spack.location(first=True, i=comp).strip(), scope="site"
            )
        update_compiler_conf(
            spack_dir / "etc" / "spack" / "compilers.yaml", binutils_loc
        )
    # Install micromamba for building our "conda" environments
    if "conda" in conf_data:
        locs["conda_dir"].mkdir(exist_ok=True)
        try:
            spack.find("micromamba")
        except sh.ErrorReturnCode:
            log.info("Installing micromamba")
            spack_install(["micromamba"])
            mamba_activate_path = locs["conda_dir"] / "load_micromamba.sh"
            mamba_activate_path.write_text(spack.load("--sh", "micromamba"))


def update_all(
    update_ts: str,
    conf_data: Dict[str, Any],
    n_tasks: Optional[int] = None,
    log_file: Optional[TextIOWrapper] = None,
):
    """Perform all updates to configured environments"""
    prep_base_dir(conf_data, n_tasks, log_file)
    locs = get_locations(conf_data["base_dir"])
    spack_snaps = update_spack_envs(
        update_ts, conf_data, locs, n_tasks=n_tasks, log_file=log_file
    )
    update_all_venvs(update_ts, conf_data, locs, spack_snaps, n_tasks, log_file)
    # update_all_conda_envs(update_ts, conf_data, locs)
