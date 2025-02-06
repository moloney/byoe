"""Manage apptainer images / apps"""
import logging, json, os, io, shutil
from pathlib import Path
from hashlib import blake2b
from typing import Dict, Optional, Any

import sh # type: ignore
from sh import bash
import requests

from .globals import EnvType, ShellType, LOCK_SUFFIXES
from .snaps import SnapId, SnapSpec
from .util import make_app_act_script, get_cmd, srun_wrap
from .conf import ApptainerConfig, ApptainerAppConfig, BuildConfig, get_job_build_info


log = logging.getLogger(__name__)


def install_unpriv_apptainer(src_url: str, install_dir: Path):
    """Install apptainer without suid binaries
    
    Runs the script provided by `src_url` through BASH, so make sure you trust it!
    """
    log.info("Installing non-suid version of apptainer")
    log.info("Downloading apptainer install script from: %s", src_url)
    install_script = requests.get(src_url).content.decode()
    log.debug("Apptainer install script:\n%s", install_script)
    bash(["-s", "-", str(install_dir)], _in=install_script)


def get_apptainer_build(apptainer: sh.Command, build_info: Dict[str, Any]):
    """Get the 'apptainer build' command setup to use configured num threads
    
    Will run through 'srun' if configured to use Slurm.
    """
    n_tasks = build_info["n_tasks"]
    cmd = get_cmd(apptainer.build, env={"APPTAINER_PYTHREADS" : str(n_tasks)})
    if build_info['use_slurm']:
        cmd = srun_wrap(
            cmd, n_tasks, build_info["srun_args"], build_info["tmp_dir"], True
        )
    return cmd


def get_apptainer_inspect(apptainer: sh.Command, build_info: Dict[str, Any]):
    """Get the 'apptainer inspect' command, potentially wrapped with srun
    
    The "inspect" command is ligthweight and generally it wouldn't make sense to run it 
    as a cluster job. This is only needed on systems where the "apptainer" command is 
    only available on the "compute" nodes, not the login nodes, of a Slurm cluster.
    """
    cmd = apptainer.inspect
    if build_info['use_slurm']:
        cmd = srun_wrap(
            cmd, 
            build_info["n_tasks"], 
            build_info["srun_args"], 
            build_info["tmp_dir"], 
            True
        )
    return cmd


def update_apptainer_env(
    env_name: str,
    apptainer_config: ApptainerConfig,
    apptainer: sh.Command,
    locs: Dict[str, Path],
    snap_id: SnapId,
    build_config: BuildConfig,
) -> SnapSpec:
    """Create updated snapshot of apptainer environment (image)"""
    # Setup commands we need with build configuration
    apptainer_build = get_apptainer_build(
        apptainer, get_job_build_info(build_config, "apptainer_build")
    )
    apptainer_inspect = get_apptainer_inspect(
        apptainer, get_job_build_info(build_config, "apptainer_inspect")
    )
    # Build the image
    image_path = locs["envs"] / "apptainer" / env_name / f"{snap_id}.sif"
    lock_file = image_path.parent / f"{image_path.stem}.def"
    snap = SnapSpec.from_lock_path(lock_file)
    image_path.parent.mkdir(parents=True, exist_ok=True)
    args = []
    if apptainer_config.inject_nv:
        args.append("--nv")
    if apptainer_config.inject_rocm:
        args.append("--rocm")
    args += [str(image_path), apptainer_config.image_spec]
    try:
        apptainer_build(*args)
    except sh.ErrorReturnCode:
        log.error("Error during apptainer image build for: %s", env_name)
        snap.stash_failed()
        raise
    # Create a more specifc "deffile" as the "lock file"
    try:
        img_data = json.loads(apptainer_inspect(["--json", "--all", str(image_path)]))
        img_attrs = img_data["data"]["attributes"]
        labels = img_attrs["labels"]
        img_vers = labels["org.label-schema.version"]
        vcs_str = ":".join([
            labels.get("org.label-schema.vcs-url", ""),
            labels.get("org.label-schema.vcs-ref", ""),
        ])
        snap = SnapSpec.from_lock_path(lock_file)
        with lock_file.open("w") as f:
            # Currently it doesn't appear possible to build images based on a label or a 
            # hash in apptainer (or even get an image hash?), so we save info about the 
            # VCS commit in a comment and install based on the version
            f.write(f"# VCS Info = {vcs_str}\n")
            for line in img_attrs["deffile"].split("\n"):
                if line.startswith("from: "):
                    # Replace potentially non-specific version with exact version
                    toks = line[6:].split(":")
                    img = toks[0]
                    line = f"from: {img}:{img_vers}\n"
                f.write(line)
        snap.dedupe()
    except Exception:
        log.error("Error while building apptainer lock file: %s", lock_file)
        snap.stash_failed()
        raise
    return snap
    

_APPTAINER_RUN_WRAP_SCRIPT = """\
#!/bin/sh
apptainer run {img} "$@"
"""

_APPTAINER_EXEC_WRAP_SCRIPT = """\
#!/bin/sh
apptainer exec {img} {cmd} "$@"
"""

def update_apptainer_app(
    app_name: str,
    app_config: ApptainerAppConfig,
    apptainer: sh.Command,
    locs: Dict[str, Path],
    snap_id: SnapId,
    build_config: BuildConfig,
) -> Optional[SnapSpec]:
    """Create updated snapshot of apptainer app"""
    env_snap = update_apptainer_env(
        app_name, 
        app_config.apptainer, 
        apptainer, 
        locs, 
        snap_id,
        build_config,
    )
    app_dir = locs["apps"] / "apptainer" / app_name / str(snap_id)
    bin_dir = app_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    if app_config.exported is None:
        app_file = bin_dir / app_name
        app_file.write_text(_APPTAINER_RUN_WRAP_SCRIPT.format(img=env_snap.snap_path))
        app_file.chmod(app_file.stat().st_mode | 0o000550)
    else:
        for exec_name in app_config.exported:
            app_file = bin_dir / exec_name
            app_file.write_text(
                _APPTAINER_EXEC_WRAP_SCRIPT.format(img=env_snap.snap_path, cmd=exec_name)
            )
            app_file.chmod(app_file.stat().st_mode | 0o000550)
    lock_path = (
        locs["apps"] / "apptainer" / app_name / f"{snap_id}{LOCK_SUFFIXES[EnvType.APPTAINER]}"
    )
    lock_path.symlink_to(os.path.relpath(env_snap.lock_file, lock_path.parent))
    # Make app activation scripts
    app_snap = SnapSpec.from_lock_path(lock_path)
    for shell_type in ShellType:
        act_path = app_snap.get_activate_path(shell_type)
        act_path.write_text(make_app_act_script(app_dir, shell_type))
    return app_snap
