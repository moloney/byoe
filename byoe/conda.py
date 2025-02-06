"""Manage and build Conda environments and apps"""
import os, logging, io, tarfile, platform, json, re
from shutil import copyfileobj, rmtree
from pathlib import Path
from typing import Dict, Optional

import requests
import yaml
import sh # type: ignore
from sh import CommandNotFound

from .globals import EnvType, ShellType, LOCK_SUFFIXES
from .snaps import SnapId, SnapSpec
from .conf import CondaAppConfig, CondaConfig
from .util import make_app_act_script


sh = sh.bake(_tty_out=False)
try:
    srun = sh.srun
    HAS_SLURM = True
except CommandNotFound:
    HAS_SLURM = False


log = logging.getLogger(__name__)


class UnsupportedPlatformError(Exception):
    pass


def get_conda_platform() -> str:
    """Get the Conda platform matching our system"""
    sys_name = platform.system().lower()
    if sys_name == "darwin":
        sys_name = "osx"
    elif sys_name == "windows":
        sys_name = "win"
    arch_name = platform.machine()
    if arch_name in ("x86_64", "x64"):
        arch_name = "64"
    elif arch_name.startswith("arm") or arch_name.startswith("aarch"):
        if sys_name == "linux":
            arch_name = "aarch64"
        elif sys_name == "osx":
            arch_name = "arm64"
        else:
            raise UnsupportedPlatformError()
    return f"{sys_name}-{arch_name}"


def get_mm_url(base_url: str, version="latest") -> str:
    """Get URL to download micromamba from"""
    conda_platform = get_conda_platform()
    return f"{base_url}/{conda_platform}/{version}"


def fetch_prebuilt(
    bin_dir: Path,
    base_url: str = "https://micro.mamba.pm/api/micromamba",
    version="latest",
) -> Path:
    """Download and unpack prebuilt micromamba binary"""
    full_url = get_mm_url(base_url, version)
    log.info("Downloading prebuilt micromamba from: %s", full_url)
    tar_data = io.BytesIO(requests.get(full_url).content)
    tf = tarfile.open(fileobj=tar_data, mode="r:*")
    for mem in tf:
        fn = mem.name.split("/")[-1]
        out_path = bin_dir / fn
        if fn in ("micromamba", "micromamba.exe"):
            src = tf.extractfile(mem)
            if src is None:
                raise ValueError(f"Unable to find micromamba executable in tarfile from: {full_url}")
            with out_path.open("wb") as dest:
                copyfileobj(src, dest)
            out_path.chmod(out_path.stat().st_mode | 0o000550)
            return out_path
    raise ValueError("Unable to find micromamba executable")


def update_conda_env(
    env_name: str,
    conda_config: CondaConfig,
    conda_lock: sh.Command,
    micromamba: sh.Command,
    locs: Dict[str, Path],
    snap_id: SnapId,
) -> SnapSpec:
    """Create updated snapshot of a conda environment"""
    envs_dir = locs["envs"] / "conda"
    snap_dir = envs_dir / env_name / str(snap_id)
    conf_data = {}
    if not conda_config.channels:
        raise ValueError("No channels given for conda env '%s'", env_name)
    if not conda_config.specs:
        raise ValueError("No specs given for conda env '%s'", env_name)
    conf_data["channels"] = conda_config.channels
    conf_data["dependencies"] = conda_config.specs
    snap_dir.parent.mkdir(parents=True, exist_ok=True)
    abstract_conf = envs_dir / env_name / f"{snap_id}-in.yml"
    lock_path = envs_dir / env_name / f"{snap_id}-lock.yml"
    snap = SnapSpec.from_lock_path(lock_path)
    abstract_conf.write_text(yaml.dump(conf_data))
    platform_id = get_conda_platform()
    kwargs = {}
    if conda_config.virtual:
        virtual = {"subdirs" : {platform_id : {"packages" : conda_config.virtual}}}
        virtual_conf = envs_dir / env_name / f"{snap_id}-virtual.yml"
        virtual_conf.write_text(yaml.dump(virtual))
        kwargs["virtual_package_spec"] = str(virtual_conf)
    log.info("Running conda-lock on input: %s", abstract_conf)
    try:
        conda_lock.lock(
            micromamba=True,
            conda=str(micromamba),
            platform=platform_id,
            f=str(abstract_conf),
            lockfile=str(lock_path),
            **kwargs,
        )
        if snap.dedupe():
            return snap
    except sh.ErrorReturnCode:
        log.error("Failed to build conda lock file from spec: %s", abstract_conf)
        snap.stash_failed()
        raise
    log.info("Installing conda packages into dir: %s", snap_dir)
    try:
        conda_lock.install(
            str(lock_path), micromamba=True, conda=str(micromamba), prefix=str(snap_dir)
        )
        # Generate activation scripts
        for shell_type in ShellType:
            if shell_type == ShellType.SH:
                conda_sh = "bash"
            elif shell_type == ShellType.CSH:
                conda_sh = "tcsh"
            elif shell_type == ShellType.FISH:
                conda_sh = "fish"
            else:
                raise NotImplementedError()
            act_path = envs_dir / env_name / f"{snap_id}-activate.{shell_type.value}"
            act_path.write_text(
                micromamba.shell.activate(prefix=str(snap_dir), shell=conda_sh)
            )
    except sh.ErrorReturnCode:
        log.error("Failed to build conda snap: %s", snap_dir)
        snap.stash_failed()
        raise
    return snap


_CONDA_WRAP_SCRIPT = """\
#!/bin/sh
unset PYTHONPATH PYTHONHOME
{prelude}
{micromamba} -r {root_prefix} -p {env_path} run {cmd} "$@"
"""


def update_conda_app(
    app_name: str,
    app_config: CondaAppConfig,
    conda_lock: sh.Command,
    micromamba: sh.Command,
    locs: Dict[str, Path],
    snap_id: SnapId,
) -> Optional[SnapSpec]:
    """Create updated snapshot of isolated Conda app"""
    env_snap = update_conda_env(
        app_name, app_config.conda, conda_lock, micromamba, locs, snap_id
    )
    if env_snap is None:
        return None
    app_dir = locs["apps"] / "conda" / app_name / str(snap_id)
    meta_dir = env_snap.snap_path / "conda-meta"
    log.debug("Looking for package meta-data under: %s", meta_dir)
    export_filt = app_config.exported
    if export_filt is None:
        export_filt = {".*": {"bin": ".*", "man": ".+"}}
    export_filt_exprs = {
        re.compile(k1): {re.compile(k2): re.compile(v2) for k2, v2 in v1.items()}
        for k1, v1 in export_filt.items()
    }
    exec_preludes = {}
    if app_config.exec_prelude:
        for pattern, prelude_lines in app_config.exec_prelude.items():
            exec_preludes[re.compile(pattern)] = prelude_lines
    app_dir.mkdir(parents=True)
    try:
        for spec in app_config.conda.specs:
            mtch = re.match("([^\s<>=~!]+).*", spec)
            assert mtch is not None
            pkg_name = mtch.groups()[0]
            pkg_filt = None
            for pkg_expr, pfilt in export_filt_exprs.items():
                if re.match(pkg_expr, pkg_name):
                    pkg_filt = pfilt
                    log.debug(
                        "Exporting from package '%s' using filt: %s", pkg_name, pfilt
                    )
                    break
            else:
                log.debug("Not exporting from package: %s", pkg_name)
                continue
            for meta_path in meta_dir.glob(f"{pkg_name}-*"):
                log.debug("Checking package meta: %s", meta_path)
                if not re.match(f"{pkg_name}-[0-9].*", meta_path.name):
                    continue
                log.debug("Reading package meta: %s", meta_path)
                pkg_meta = json.loads(meta_path.read_text())
                for pkg_file in pkg_meta["files"]:
                    pkg_file_toks = pkg_file.split("/")
                    pkg_sub_dir = pkg_file_toks[0]
                    pkg_sub_path = "/".join(pkg_file_toks[1:])
                    file_expr = None
                    for sub_dir_expr, fexpr in pkg_filt.items():
                        if re.match(sub_dir_expr, pkg_sub_dir):
                            file_expr = fexpr
                            log.debug(
                                "Exporting from sub_dir '%s' using filter: %s", 
                                pkg_sub_dir, 
                                fexpr,
                            )
                            break
                    else:
                        continue
                    if not re.match(file_expr, pkg_sub_path):
                        continue
                    pkg_file = Path(pkg_file)
                    app_file = app_dir / pkg_file
                    if app_file.exists():
                        log.debug("Skipping already existing file: %s", app_file)
                        continue
                    app_file.parent.mkdir(exist_ok=True, parents=True)
                    if pkg_sub_dir == "bin":
                        log.debug("Making wrapper %s -> %s", app_file, pkg_file)
                        pre_lines = []
                        for expr, lines in exec_preludes.items():
                            if re.match(expr, pkg_file.name):
                                pre_lines.extend(lines)
                        app_file.write_text(
                            _CONDA_WRAP_SCRIPT.format(
                                root_prefix=locs["conda"],
                                micromamba=str(micromamba),
                                env_path=env_snap.snap_path,
                                cmd=pkg_file.name,
                                prelude="\n".join(pre_lines),
                            )
                        )
                        app_file.chmod(app_file.stat().st_mode | 0o000550)
                    else:
                        tgt = os.path.relpath(env_snap.snap_path / pkg_file, app_file.parent)
                        log.debug("Symlinking %s -> %s", app_file, tgt)
                        app_file.symlink_to(tgt)
        # Link to lock file
        lock_path = (
            locs["apps"] / "conda" / app_name / f"{snap_id}{LOCK_SUFFIXES[EnvType.CONDA]}"
        )
        lock_path.symlink_to(os.path.relpath(env_snap.lock_file, lock_path.parent))
    except Exception:
        log.exception("Error constructing conda app dir: %s", app_name)
        rmtree(app_dir)
        env_snap.remove(keep_lock=False)
        return None
    # Make app activation scripts
    app_snap = SnapSpec.from_lock_path(lock_path)
    for shell_type in ShellType:
        act_path = app_snap.get_activate_path(shell_type)
        act_path.write_text(make_app_act_script(app_dir, shell_type))
    return app_snap
