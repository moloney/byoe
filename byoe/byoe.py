"""Bring Your Own Environment"""
import sys, os, logging, re
from dataclasses import dataclass, asdict
from io import TextIOWrapper
from pathlib import Path
from datetime import datetime
from typing import Any, List, Dict, Optional

import yaml
import sh

sh = sh.bake(_tty_out=False)
git = sh.git

from ._globals import (
    LOCK_SUFFIXES,
    TS_FORMAT,
    CHANNEL_UPDATE_MONTHS,
    EnvType,
    UpdateChannel,
    ShellType,
)
from .util import get_locations, select_snap
from .conf import SiteConfig, EnvConfig, AppConfig, BuildConfig, IncludableConfig
from .spack import (
    get_spack,
    update_spack_env,
    get_compilers,
)
from .python_venv import update_python_env
from .conda import update_all_conda_envs


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SnapSpec:
    """Capture info about a environment snapshot"""

    env_type: EnvType

    env_name: str

    env_date: datetime

    env_dir: Path

    lock_file: Path

    @property
    def basename(self) -> str:
        return f"{self.env_name}-{self.env_date.strftime(TS_FORMAT)}"

    def get_activate_path(self, shell: ShellType = ShellType.SH) -> Path:
        """Get path to the activation script"""
        if self.env_type == EnvType.SPACK:
            return self.env_dir.parent / f"{self.env_dir.name}_activate.{shell.value}"
        elif self.env_type == EnvType.PYTHON:
            if shell == ShellType.SH:
                suffix = ""
            else:
                suffix = f".{shell.value}"
            return self.env_dir / "bin" / f"activate{suffix}"

    @classmethod
    def from_lock_path(cls, env_type: EnvType, lock_file: Path) -> "SnapSpec":
        """Generate a SnapSpec from the path to the lock file"""
        env_name, ts = re.match("(.+)-([0-9]+).*", lock_file.stem).groups()
        snap_name = f"{env_name}-{ts}"
        env_dt = datetime.strptime(ts, TS_FORMAT)
        env_dir = lock_file.parent / snap_name
        return cls(env_type, env_name, env_dt, env_dir, lock_file)

    def __lt__(self, other):
        return self.basename < other.basename


class NoCompilerFoundError(Exception):
    pass


def prep_base_dir(
    base_dir: Path,
    pull_spack: bool = True,
    log_file: Optional[TextIOWrapper] = None,
) -> SiteConfig:
    """Make sure the base_dir is initialized"""
    # Set the base_dir so our IncludableConfig subclasses can consume relative paths
    IncludableConfig.base_dir = base_dir
    locs = get_locations(base_dir)
    for loc in locs.values():
        loc.mkdir(exist_ok=True)
    # Build startup scripts
    for shell_type in ("sh",):
        startup_lines = [f"export PATH={Path(sys.argv[0]).parent}:$PATH"]
        startup_lines.append(
            f"export PIP_FIND_LINKS={' '.join([str(locs['wheels_dir'])] + os.environ.get('PIP_FIND_LINKS', []))}"
        )
        # TODO: Documentation is poor, but seems CONDA_PKG_DIRS doesn't support
        #       multiple paths?
        startup_lines.append(f"export CONDA_PKG_DIRS={locs['conda_pkg_dir']}")
        (locs["startup_dir"] / f"byoe_startup.{shell_type}").write_text(
            "\n".join(startup_lines + [""])
        )
    # Get site config
    spack_dir = locs["spack_dir"]
    site_conf_path = base_dir / "site_conf.yaml"
    if not site_conf_path.exists():
        if sys.stdout.isatty():
            site_conf = SiteConfig.build_interactive()
        else:
            log.warning("No site config found, creating default")
            site_conf = SiteConfig()
        site_conf_path.write_text(yaml.dump(site_conf.to_dict()))
    else:
        site_conf = SiteConfig.from_dict(yaml.safe_load(site_conf_path.read_text()))
    # Create / update spack git repo
    branch = site_conf.spack_global.repo_branch
    if not (spack_dir / ".git").exists():
        log.info("Cloning spack into: %s", spack_dir)
        kwargs = {}
        if branch:
            kwargs["branch"] = branch
        git.clone(site_conf.spack_global.repo_url, spack_dir, **kwargs)
    elif pull_spack:
        log.info("Updating local version of spack repo")
        git("--git-dir", f"{locs['spack_dir'] / '.git'}", "fetch")
        tgt = "origin"
        if branch:
            tgt = f"origin/{branch}"
        git(
            "--git-dir",
            f"{locs['spack_dir'] / '.git'}",
            "--work-tree",
            str(locs["spack_dir"]),
            "reset",
            "--hard",
            tgt,
        )
    spack_lic_dir = locs["spack_dir"] / "etc" / "spack" / "licenses"
    if not spack_lic_dir.exists():
        spack_lic_dir.symlink_to("../../../licenses", True)
    # Create / update global spack config
    spack_global_config_path = locs["spack_dir"] / "etc" / "spack"
    (spack_global_config_path / "config.yaml").write_text(
        yaml.dump({"config": {"install_tree": {"padded_length": 128}}})
    )
    # Configure default spack mirror
    mirrors = site_conf.spack_global.mirrors
    if mirrors is None:
        mirrors = {}
    if "default" not in mirrors:
        mirrors["default"] = str(locs["spack_pkg_dir"])
    (spack_global_config_path / "mirrors.yaml").write_text(
        yaml.dump({"mirrors": mirrors})
    )
    # Get basic "spack" command wrapper
    spack = get_spack(locs, log_file=log_file)
    # Setup GPG key for signing spack packages
    spack.gpg.init()
    def_gpg = {
        "name": "byoe_build",
        "email": "byoe@noreply.org",
        "comment": "Generated internally by BYOE for signing packages",
    }
    if not re.search(
        r"^uid\s+(\[.+\])?\s*byoe_build\s.+", spack.gpg.list(), flags=re.MULTILINE
    ):
        kwargs = {}
        if "comment" in def_gpg:
            kwargs["comment"] = def_gpg["comment"]
        spack.gpg.create(def_gpg["name"], def_gpg["email"], **kwargs)
    # TODO: At least for HTTP mirrors we can run "spack buildcache keys --install --trust"
    #       need to look into how to handle other GPG keys
    # Trust any other specified gpg keys for installing packages
    # for gpg_name, gpg_info in def_gpg_info.items():
    #     if gpg_name == "byoe_builder":
    #         continue
    #     # TODO: Need to test for existance first?
    #     spack.gpg.trust(gpg_info)
    # Make sure spack is bootstrapped
    log.info("Bootstrapping spack")
    spack.bootstrap.now()
    # We keep an updated list of system compilers in the spack site config
    log.info("Checking for system compilers")
    spack.compiler.find(scope="site")
    sys_compilers = get_compilers(spack)
    if len(sys_compilers) == 0:
        raise NoCompilerFoundError()
    # TODO: Try to push this into 'update_envs'
    # # Install micromamba for building our "conda" environments
    # try:
    #     spack.find("micromamba")
    # except sh.ErrorReturnCode:
    #     log.info("Installing micromamba")
    #     spack_install(["micromamba"])
    #     mamba_activate_path = locs["envs_dir"] / "conda" / "load_micromamba.sh"
    #     mamba_activate_path.write_text(spack.load("--sh", "micromamba"))
    return locs, site_conf


def _build_env(
    env_name: str, 
    env_conf: EnvConfig, 
    locs: Dict[str, Path], 
    update_ts: str,
    build_config: Optional[BuildConfig] = None,
    log_file: Optional[TextIOWrapper] = None,
):
    if env_conf.spack:
        spack_snap = update_spack_env(
            env_name, env_conf.spack, locs, update_ts, build_config, log_file
        )
        if spack_snap is None:
            return
        if env_conf.python:
            update_python_env(
                env_name, env_conf.python, spack_snap, locs, update_ts, log_file
            )
    elif env_conf.conda:
        pass # TODO: Build conda env


def do_update(
    base_dir: Path,
    update_ts: str,
    envs_or_apps: Optional[List[str]] = None,
    pull_spack: bool = True,
    log_file: Optional[TextIOWrapper] = None,
):
    """Perform updates to configured environments and apps"""
    locs, site_conf = prep_base_dir(base_dir, pull_spack, log_file)
    if site_conf.envs is None and site_conf.apps is None:
        log.warning("Nothing to do, no 'envs' or 'apps' defined")
        return
    if envs_or_apps is None:
        envs = [] if site_conf.envs is None else list(site_conf.envs.keys())
        apps = [] if site_conf.apps is None else list(site_conf.apps.keys())
    else:
        if site_conf.envs:
            envs = [x for x in site_conf.envs.keys() if x in envs_or_apps]
        if site_conf.apps:
            apps =  [x for x in site_conf.apps.keys() if x in envs_or_apps]
    for env_name in envs:
        env_conf = site_conf.envs[env_name]
        if site_conf.defaults:
            env_conf.set_defaults(site_conf.defaults)
        _build_env(env_name, env_conf, locs, update_ts, site_conf.build_opts, log_file)
    # TODO: Update apps


def get_snaps(
    base_dir: Path, env_type: EnvType, env_name: str, exists_only: bool = True
) -> List[SnapSpec]:
    """Get list of available snapshots"""
    locs = get_locations(base_dir)
    env_dir = locs['envs_dir'] / env_type.value
    lock_files = list(env_dir.glob(f"{env_name}-*{LOCK_SUFFIXES[env_type]}"))
    snap_specs = [SnapSpec.from_lock_path(env_type, f) for f in lock_files]
    if exists_only:
        snap_specs = [s for s in snap_specs if s.env_dir.exists()]
    snap_specs.sort()
    return snap_specs


def get_activate_script(
    base_dir: Path,
    name: Optional[str] = None,
    channel: Optional[UpdateChannel] = None,
    time_stamp: Optional[str] = None,
    skip_layer: Optional[List[EnvType]] = None,
    shell_type: Optional[ShellType] = None,
) -> str:
    """Get the script needed to activate the environment"""
    active_types = [x for x in EnvType]
    if EnvType.SPACK in skip_layer:
        if EnvType.PYTHON not in skip_layer:
            raise ValueError("Can't skip 'spack' and enable 'python'")
        active_types.remove(EnvType.SPACK)
        active_types.remove(EnvType.PYTHON)
    elif EnvType.PYTHON in skip_layer:
        active_types.remove(EnvType.PYTHON)
    if EnvType.CONDA in skip_layer:
        active_types.remove(EnvType.CONDA)
    if name is None:
        name = os.environ.get("BYOE_DEFAULT_ENV_NAME", "main")
    if channel is None:
        channel = UpdateChannel(os.environ.get("BYOE_CHANNEL", "stable"))
    if shell_type is None:
        curr_shell = Path(os.environ.get("SHELL", "/bin/bash"))
        curr_shell = curr_shell.stem
        if curr_shell == "bash":
            shell_type = ShellType.SH
        elif curr_shell == "csh":
            shell_type = ShellType.CSH
        elif curr_shell == "fish":
            shell_type = ShellType.FISH
        else:
            raise ValueError(f"Current shell is unsupported: {curr_shell}")
    locs = get_locations(base_dir)
    snaps = {}
    # TODO: This could enable different envs based on skip_layer, which we probably
    #       don't want?
    if EnvType.SPACK not in skip_layer:
        snaps["spack"] = get_snaps(base_dir, EnvType.SPACK, name)
    if EnvType.PYTHON not in skip_layer:
        snaps["python"] = get_snaps(base_dir, EnvType.PYTHON, name)
    # if EnvType.CONDA not in skip_layer:
    #     snaps["conda"] = get_snaps(base_dir, EnvType.CONDA, name)
    snap_sets = list(snaps.values())
    valid_dates = {x.env_date for x in snap_sets[0]}
    for snap_set in snap_sets[1:]:
        valid_dates &= {x.env_date for x in snap_set}
    if not valid_dates:
        raise ValueError("No valid snaps found")
    valid_dates = list(valid_dates)
    if time_stamp is None:
        sel_date = select_snap(valid_dates, CHANNEL_UPDATE_MONTHS[channel])
        log.info("Using channel %s selects snap date: %s", channel, sel_date)
    else:
        sel_date = datetime.strptime(time_stamp, TS_FORMAT)
    snaps = []
    for snap_set in snap_sets:
        for snap in snap_set:
            if snap.env_date == sel_date:
                snaps.append(snap)
                break
        else:
            assert False
    lines = []
    # TODO: We ultimately will need to provide our own deactivation, in particular to
    #       disable things like conda "apps" (or python "apps"). Seems like we could
    #       record env before each activation and then diff afterwards and save into
    #       an environment variable.
    # lines = [f"export BYOE_ENV={}", f"__BYOE_PRE_PATH={os.environ['PATH']}"]
    for snap in snaps:
        lines.append(snap.get_activate_path(shell_type).read_text())
    return "\n".join(lines)
