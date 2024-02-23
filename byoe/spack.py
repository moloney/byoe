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

from .util import get_activated_envrion, get_env_cmd, srun_wrap, HAS_SLURM, wrap_cmd
from .conf import BuildConfig, SpackConfig, get_job_build_info


log = logging.getLogger(__name__)


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
        act_scripts.append(spack.env.activate("--sh", "-d", str(env)))
    if modules:
        act_scripts.append(spack.load("--first", "--sh", *modules))
    if act_scripts:
        env_data = get_activated_envrion(act_scripts, env_data)
        spack = spack.bake(_env=env_data)
    if log_file:
        spack = spack.bake(_out=log_file, _err=log_file, _tee={"err", "out"})
    return spack


install_script = """\
#!/bin/bash

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
    """Setup spack command to run using multiple CPU cores"""
    if args:
        args = args[:]
    if build_info is None:
        build_info = {}
    n_tasks = build_info.get("n_tasks")
    # Check if we are already in a slurm job
    # TODO: Would like to be able to overrides this on commnand line
    slurm_cpus = os.environ.get("SLURM_CPUS_ON_NODE")
    if slurm_cpus:
        args = ["-j", slurm_cpus] + args
    elif n_tasks:
        args = ["-j", str(n_tasks)] + args
    cmd = cmd.bake(*args)
    if build_info["use_slurm"] and not slurm_cpus:
        cmd = srun_wrap(
            cmd,
            n_tasks,
            build_info.get("srun_args", ""),
            build_info.get("tmp_dir"),
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
    build_info = get_job_build_info(build_config, "spack_install")
    if not build_info["tmp_dir"]:
        build_info["tmp_dir"] = base_tmp
    return par_spack(wrap_cmd(install_cmd, spack.install), install_args, build_info)


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
        spack.concretize, 
        conc_args, 
        get_job_build_info(build_config, "spack_concretize"),
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
        spack.buildcache.push, push_args, get_job_build_info(build_config, "spack_push")
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


def get_compilers(spack, env=None):
    """Get compilers spack knows about"""
    if env:
        return [
            x.strip() for x in spack("-e", env, "compiler", "list").split("\n")[2:] 
            if x.strip()
        ]
    else:
        return [x.strip() for x in spack.compiler.list().split("\n")[2:] if x.strip()]


def _update_compiler_conf(compiler_conf: Path, binutils_path: Path):
    """Update spack compiler config to prepend binutils_path to PATH"""
    data = yaml.safe_load(compiler_conf.open())
    for comp_info in data["compilers"]:
        comp_env = comp_info["compiler"]["environment"]
        if "prepend_path" not in comp_env:
            comp_env["prepend_path"] = {"PATH": str(binutils_path / "bin")}
    yaml.dump(data, compiler_conf.open("w"))


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
        act_script = spack.env.activate(f"--{sh_type}", "-d", str(env_dir))
        with open(f"{snap_path}_activate.{sh_type}", "wt") as out_f:
            out_f.write(act_script)


def _prep_spack_build(
    env_dir: Path, 
    spack: sh.Command, 
    spack_config: SpackConfig, 
    build_config: BuildConfig,
    base_tmp: Path,
):
    # Configure the environments build deps, installing any that are missing
    missing_build_deps = []
    compiler = binutils =  None
    if spack_config.compiler is not None:
        compiler = spack_config.compiler
        compilers = get_compilers(spack, env_dir)
        # TODO: Using 'startswith' here is hacky but works for common use of just 
        #       specifying the major version and then matching the full version (e.g. 
        #       spec "gcc@12" and then match "gcc@12.3.0"). Supporting specs with less-
        #       than or greater-than could be useful.
        if not any(c.startswith(compiler) for c in compilers):
            try:
                spack.find(compiler)
            except sh.ErrorReturnCode:
                missing_build_deps.append(compiler)
            else:
                comp_loc = spack.location(first=True, i=compiler)
                spack("-e", env_dir, "compiler", "find", "--scope", env_dir, comp_loc)
    if spack_config.binutils is not None:
        binutils = spack_config.binutils
        if spack_config.compiler is None:
            # TODO: A user could have explicity listed a system compiler here...
            #       Would be nice to just support system compilers here, just awkward 
            #       to implement since we store them in the global spack "site" level
            #       config currently, to avoid overloading the environments spack.yaml
            #       with a bunch of compiler definitions that might not get used in that
            #       environment. If we set the binutils version in the global config
            #       we can't build environments in parallel. Need to see if we can 
            #       override the system config of a compiler with and environment config.
            raise ValueError("Can't set non-system binutils for system compiler")
        try:
            spack.find(binutils)
        except sh.ErrorReturnCode:
            missing_build_deps.append(binutils)
        else:
            binutils_path =  spack.location(first=True, i=binutils)
            _update_compiler_conf(env_dir / "spack.yaml", binutils_path)
    if missing_build_deps:
        log.info("Installing missing build dependencies")
        spack_install = get_spack_install(spack, base_tmp, build_config=build_config)
        for build_dep in missing_build_deps:
            spack_install(build_dep)
        if compiler in missing_build_deps:
            comp_loc = spack.location(first=True, i=compiler)
            spack(
                "-e", env_dir, "compiler", "find", "--scope", f"env:{env_dir}", comp_loc
            )
        if binutils in missing_build_deps or compiler in missing_build_deps:
            binutils_path =  spack.location(first=True, i=spack_config.binutils)
            _update_compiler_conf(env_dir / "spack.yaml", binutils_path)
    # Setup any externals
    if spack_config.externals:
        for external in spack_config.externals:
            spack(
                "-e", env_dir, "external", "find", "--scope", f"env:{env_dir}", external
            )


def update_spack_env(
    env_name: str,
    spack_config: SpackConfig,
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
    if spack_config.etc is not None:
        env_info = deepcopy(spack_config.etc)
    else:
        env_info = {}
    env_info["specs"] = spack_config.specs[:]
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
    log.info("Preparing to build env: %s", env_name)
    try:
        _prep_spack_build(env_dir, spack, spack_config, build_config, locs["tmp_dir"])
    except:
        log.error("Error preparing spack environment: %s", env_dir)
        return None
    # Prepare spack concretize/install/push commands for the environment
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


def get_spack_env_cmds(
    spack_env: Path, cmds: List[str], log_file: Optional[TextIOWrapper]
) -> List[sh.Command]:
    """Get sh.Command referencing a command in the given environment"""
    act_path = spack_env.parent / f"{spack_env.name}_activate.sh"
    act_env = get_activated_envrion([act_path.read_text()])
    env_bin = spack_env / "bin"
    return [get_env_cmd(env_bin / cmd, act_env, log_file) for cmd in cmds]
