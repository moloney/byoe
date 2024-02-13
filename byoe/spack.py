"""Manage / build spack environments"""
import os, sys, logging, shutil, time
from datetime import datetime
from pathlib import Path
from copy import deepcopy
from io import TextIOWrapper
from typing import List, Dict, Optional, Any

import yaml
import sh

sh = sh.bake(_tty_out=False)
git = sh.git

from ._globals import DEFAULT_SLURM_TASKS
from .util import get_activated_envrion, get_env_cmd, srun_wrap, HAS_SLURM, wrap_cmd
from .conf import BuildConfig, SiteConfig, SlurmBuildConfig, SpackConfig


log = logging.getLogger(__name__)


def update_compiler_conf(compiler_conf: Path, binutils_path: Path):
    """Update spack compilers.yaml to prepend binutils_path to PATH"""
    data = yaml.safe_load(compiler_conf.open())
    for comp_info in data["compilers"]:
        comp_env = comp_info["compiler"]["environment"]
        if "prepend_path" not in comp_env:
            comp_env["prepend_path"] = {"PATH": str(binutils_path / "bin")}
    yaml.dump(data, compiler_conf.open("w"))


def get_spack(
    path_locs: Dict[str, Path],
    env: Optional[Path] = None,
    modules: Optional[List[str]] = None,  # TODO: Remove this?
    log_file: Optional[TextIOWrapper] = None,
) -> Dict[str, str]:
    """Get modified environment with basic spack setup"""
    spack_dir = path_locs["spack_dir"]
    tmp_dir = path_locs["tmp_dir"]
    env_data = os.environ.copy()
    # TODO: Better way of handling SSL certificate locations for PBS bootstrapped installs?
    env_data.update(
        {
            "SPACK_ROOT": str(spack_dir),
            "PATH": f"{spack_dir / 'bin'}:{os.environ['PATH']}",
            "SPACK_PYTHON": sys.executable,
            "TMPDIR": str(tmp_dir),
        }
    )
    # Handle alt locations for SSL/TLS certs (for now just redhat)
    alt_cert_dir = Path("/etc/pki/tls/certs")
    if alt_cert_dir.exists():
        env_data["SSL_CERT_DIR"] = str(alt_cert_dir)
    alt_cert_file = Path("/etc/pki/tls/cert.pem")
    if alt_cert_file.exists():
        env_data["SSL_CERT_FILE"] = str(alt_cert_file)
    spack = get_env_cmd(path_locs["spack_dir"] / "bin" / "spack", env_data)
    act_scripts = []
    if env:
        act_scripts.append(spack.env.activate("--sh", dir=str(env)))
    if modules:
        act_scripts.append(spack.load("--first", "--sh", *modules))
    if act_scripts:
        env_data = get_activated_envrion(act_scripts, env_data)
        spack = spack.bake(_env=env_data)
    if log_file:
        spack = spack.bake(_out=log_file, _err=log_file, _tee={"err", "out"})
    return spack


install_script = """\
#!/bin/sh

err_out_dir=$1
spack_cmd=$2
stage_dir=$(${spack_cmd} location -S)
${@:2}
exit_code=$?
if [ $exit_code -ne 0 ] ; then
    echo "Copying staging dirs from failed builds to $err_out_dir"
    mkdir $err_out_dir
    for build_dir in $(ls -d $stage_dir/spack-stage-*) ; do 
        cp -r $build_dir $err_out_dir
    done
fi
exit $exit_code
"""


def par_spack(
    cmd: sh.Command,
    args: Optional[List[str]] = None,
    build_info: Optional[Dict[str, Any]] = None,
):
    """Setup spack command to run in parrallel using multiple CPU cores
    
    Explicitly passed `n_tasks` takes precedence, otherwise if we are already running in 
    a Slurm job use the number of allocated CPUs. Finally if Slurm is available and 
    enabled try to run the command on the Slurm cluster in a new job.
    """
    if args:
        args = args[:]
    if build_info is None:
        build_info = {}
    # Check if we are already in a slurm job
    slurm_cpus = os.environ.get("SLURM_CPUS_ON_NODE")
    if n_tasks:
        args = ["-j", str(n_tasks)] + args
    elif slurm_cpus:
        args = ["-j", slurm_cpus] + args
    elif HAS_SLURM and use_slurm:
        if slurm_opts is None:
            slurm_opts = {}
        if n_tasks is None:
            n_tasks = slurm_opts.get("tasks_per_job", DEFAULT_SLURM_TASKS)
        args = ["-j", str(n_tasks)] + args
    cmd = cmd.bake(*args)
    if HAS_SLURM and use_slurm and not slurm_cpus:
        cmd = srun_wrap(
            cmd,
            n_tasks,
            slurm_opts.get("srun_args", ""),
            slurm_opts.get("tmp_dir"),
        )
    return cmd


def get_spack_install(
    spack: sh.Command,
    base_tmp: Path,
    timestamp: str = None,
    fresh: bool = True,
    yes_to_all: bool = True,
    build_config: Optional[BuildConfig] = None,
) -> sh.Command:
    """Get a preconfigured 'spack install' command"""
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    install_script_path = base_tmp / f"spack_install-{timestamp}.sh"
    install_script_path.touch(0o770)
    install_script_path.write_text(install_script)
    install_cmd = sh.Command(str(install_script_path))
    install_cmd = install_cmd.bake(base_tmp / f"error-stage-dirs-{timestamp}")
    install_args = []
    if fresh:
        install_args.append("--fresh")
    if yes_to_all:
        install_args.append("--yes-to-all")
    return par_spack(
        wrap_cmd(install_cmd, spack.install), 
        install_args, 
        build_config.get_job_build_info("spack_install")
    )


def get_spack_concretize(
    spack: sh.Command,
    fresh: bool = True,
    build_config: Optional[BuildConfig] = None,
) -> sh.Command:
    """Get a preconfigured 'spack concretize' command"""
    if build_config is None:
        build_config = BuildConfig()
    conc_args = []
    if fresh:
        conc_args.append("--fresh")
    return par_spack(
        spack.concretize, conc_args, build_config.get_job_build_info("spack_concretize")
    )


def get_spack_push(
    spack: sh.Command,
    build_config: Optional[BuildConfig] = None,
):
    """Get preconfigured 'spack buildcache push' command"""
    if build_config is None:
        build_config = BuildConfig()
    push_args = ["default"]
    return par_spack(
        spack.buildcache.push, push_args, build_config.get_job_build_info("spack_push")
    )


def conv_view_links(view_dir: Path):
    """Convert symlinks in a view to hardlinks"""
    for dir, sub_dirs, sub_files in os.walk(view_dir):
        for file_path in sub_files:
            file_path = Path(file_path)
            if file_path.is_symlink():
                tgt = file_path.readlink()
                try:
                    rel_path = tgt.relative_to(view_dir)
                except ValueError:
                    # TODO: make the hardlink
                    pass
                else:
                    # make relative symlink
                    pass
            else:
                # TODO: Check if canonical path is outside the view
                pass


def _update_spack_env(
    env_dir: Path,
    snap_path: Path,
    spack: sh.Command,
    spack_install: sh.Command,
    spack_concretize: sh.Command,
    spack_push: sh.Command,
) -> None:
    """Create updated snapshot of a single environment"""
    start = datetime.now()
    log.info("Concretizing spack snapshot: %s", snap_path)
    conc_out = spack_concretize()
    if not conc_out.strip():
        log.info("No updates for spack snapshot: %s", snap_path)
    log.info("Building spack snapshot: %s", snap_path)
    install_err: Optional[Exception] = None
    try:
        spack_install([])
    except Exception as e:
        install_err = e
        log.exception("Error building spack snapshot: %s", snap_path)
        if snap_path.exists():
            shutil.rmtree(snap_path)
    else:
        log.info(
            "Finished spack snapshot: %s (took %s)", snap_path, (datetime.now() - start)
        )
    log.info("Building spack binary packages")
    try:
        spack_push()
    except:
        log.exception("Error while pushing to spack buildcache")
    if install_err is not None:
        raise install_err
    for sh_type in ("sh", "csh", "fish"):
        act_script = spack.env.activate(f"--{sh_type}", dir=str(env_dir))
        with open(f"{snap_path}_activate.{sh_type}", "wt") as out_f:
            out_f.write(act_script)


# TODO: Need to setup missing compilers
def update_spack_env(
    env_name: str,
    env_config: SpackConfig,
    locs: Dict[str, Path],
    update_ts: str,
    build_config: Optional[BuildConfig] = None,
    log_file: Optional[TextIOWrapper] = None,
) -> Path:
    """Update and snapshot a spack environment"""
    if build_config is None:
        build_config = BuildConfig()
    spack = get_spack(locs, log_file=log_file)
    spack_envs_dir = locs["envs_dir"] / "spack"
    spack_envs_dir.mkdir(exist_ok=True)
    # TODO: Package config updates seem to be ignored sometimes, which might be fixed
    #       by putting the env_dir under some unique name in the tmp dir each time
    env_dir = spack_envs_dir / env_name
    env_dir.mkdir(parents=True, exist_ok=True)
    spec_path = env_dir / "spack.yaml"
    snap_name = f"{env_name}-{update_ts}"
    snap_path = spack_envs_dir / snap_name
    log.info("Updating spack snap: %s", snap_path)
    env_info = deepcopy(env_config.etc)
    env_info["specs"] = env_config.specs[:]
    if not env_info["specs"]:
        log.warning("No specs defined for spack env: %s", env_name)
        return
    env_info["view"] = {
        "default": {
            "root": str(snap_path),
            "link": "all",
            "link_type": "symlink",
        }
    }
    with spec_path.open("wt") as spec_f:
        yaml.safe_dump({"spack": env_info}, spec_f)
    # TODO: This is kind of ugly, but needed since "spack env activate --sh" will
    #       behave surprisingly when the environment is already activated (skips setting
    #       some env vars). Could be better to pass the "--env" arg to spack? Or fix
    #       the activate bug in spack.
    spack_env = get_spack(locs, env_dir, log_file=log_file)
    spack_install = get_spack_install(
        spack_env, locs["tmp_dir"], update_ts, build_config,
    )
    spack_concretize = get_spack_concretize(spack_env, build_config)
    spack_push = get_spack_push(spack_env, build_config)
    try:
        _update_spack_env(
            env_dir, snap_path, spack, spack_install, spack_concretize, spack_push
        )
    except:
        log.error("Error building spack environment: %s", env_dir)
        return None
    else:
        conv_view_links(snap_path)
        shutil.copy(
            env_dir / "spack.lock", spack_envs_dir / f"{snap_name}.lock"
        )
    log.info("Updating spack buildcache index")
    try:
        spack.buildcache("update-index", "default")
    except sh.ErrorReturnCode:
        log.exception("Error while updating spack buildcache index")
    return snap_path


# def update_spack_envs(
#     locs: Dict[str, Path],
#     site_conf: SiteConfig,
#     update_ts: str,
#     envs: Optional[List[str]] = None,
#     n_tasks: Optional[int] = None,
#     log_file: Optional[TextIOWrapper] = None,
# ) -> Dict[str, Path]:
#     """Create updated snapshots for the configured spack environments"""
#     if envs is not None:
#         envs = set(envs)
#     created: Dict[str, Path] = {}
#     spack = get_spack(locs, log_file=log_file)
#     spack_env_dir = locs["envs_dir"] / "spack"
#     for env_name, env_info in conf["spack"].get("envs", {}).items():
#         if envs is not None and env_name not in envs:
#             continue
#         env_dir = spack_env_dir / env_name
#         env_dir.mkdir(parents=True, exist_ok=True)
#         spec_path = env_dir / "spack.yaml"
#         snap_name = f"{env_name}-{update_ts}"
#         snap_path = spack_env_dir / snap_name
#         log.info("Updating spack snap: %s", snap_path)
#         env_info = deepcopy(env_info)
#         if "specs" not in env_info:
#             env_info["specs"] = []
#         env_info["specs"] += conf["spack"].get("global_specs", [])
#         if not env_info["specs"]:
#             log.warning("No specs defined for spack env: %s", env_name)
#             continue
#         # TODO: Once spack fixes bug with hardlink views, use those here
#         env_info["view"] = {
#             "default": {
#                 "root": str(snap_path),
#                 "link": "all",
#                 "link_type": "symlink",
#             }
#         }
#         with spec_path.open("wt") as spec_f:
#             yaml.safe_dump({"spack": env_info}, spec_f)
#         # TODO: This is kind of ugly, but needed since "spack env activate --sh" will
#         #       misbehave when the environment is already activated (skips setting some
#         #       env vars). Could be better to pass the "--env" arg to spack? Or fix
#         #       the activate bug in spack.
#         spack_env = get_spack(locs, env_dir, log_file=log_file)
#         use_slurm = conf.get("build_on_slurm", True)
#         slurm_opts = conf["spack"].get("slurm_opts", {})
#         use_slurm = use_slurm and slurm_opts.get("enabled", True)
#         spack_install = get_spack_install(
#             spack_env,
#             locs["tmp_dir"],
#             update_ts,
#             n_tasks=n_tasks,
#             use_slurm=use_slurm,
#             slurm_opts=slurm_opts.get("install", {}),
#         )
#         spack_concretize = get_spack_concretize(
#             spack_env,
#             n_tasks=n_tasks,
#             use_slurm=use_slurm,
#             slurm_opts=slurm_opts.get("concretize", {}),
#         )
#         spack_push = get_spack_push(
#             spack_env,  
#             n_tasks=n_tasks,
#             use_slurm=use_slurm,
#             slurm_opts=slurm_opts.get("push", {}),
#         )
#         try:
#             _update_spack_env(
#                 env_dir, snap_path, spack, spack_install, spack_concretize, spack_push
#             )
#         except:
#             log.error("Error building spack environment: %s", env_dir)
#         else:
#             conv_view_links(snap_path)
#             shutil.copy(
#                 env_dir / "spack.lock", spack_env_dir / f"{snap_name}.lock"
#             )
#             created[env_name] = snap_path
#     log.info("Updating spack buildcache index")
#     try:
#         spack.buildcache("update-index", "default")
#     except sh.ErrorReturnCode:
#         log.exception("Error while updating spack buildcache index")
#     return created


def get_spack_env_cmds(
    spack_env: Path, cmds: List[str], log_file: Optional[TextIOWrapper]
) -> List[sh.Command]:
    """Get sh.Command referencing a command in the given environment"""
    act_path = spack_env.parent / f"{spack_env.name}_activate.sh"
    act_env = get_activated_envrion([act_path.read_text()])
    env_bin = spack_env / "bin"
    return [get_env_cmd(env_bin / cmd, act_env, log_file) for cmd in cmds]
