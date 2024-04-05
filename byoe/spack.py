"""Manage / build spack environments"""

import json
import os, logging, shutil
from datetime import datetime
from pathlib import Path
from copy import deepcopy
from io import TextIOWrapper
from typing import List, Dict, Optional, Any

import yaml
import sh

sh = sh.bake(_tty_out=False)
git = sh.git

from .globals import SnapId, SnapSpec
from .util import get_activated_envrion, get_env_cmd, srun_wrap, wrap_cmd
from .conf import BuildConfig, SpackBuildChain, SpackConfig, get_job_build_info


log = logging.getLogger(__name__)


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
            str(n_tasks),
            build_info.get("srun_args", ""),
            str(build_info.get("tmp_dir")),
        )
    return cmd


def get_spack_install(
    spack: sh.Command,
    base_tmp: Path,
    run_id: str = None,
    fresh: bool = True,
    yes_to_all: bool = True,
    build_config: Optional[BuildConfig] = None,
) -> sh.Command:
    """Get a preconfigured 'spack install' command"""
    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d%H%M%S")
    install_script_path = base_tmp / f"spack_install-{run_id}.sh"
    install_script_path.touch(0o770)
    install_script_path.write_text(install_script)
    install_cmd = sh.Command(str(install_script_path))
    install_cmd = install_cmd.bake(base_tmp / f"error-stage-dirs-{run_id}")
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
    # We have to manually build spec list, otherwise trying to push a partial 
    # environment will fail
    installed = json.loads(spack.find(json=True))
    specs = [f"{x['name']}@{x['version']}/{x['hash']}" for x in installed]
    push_args = ["default"] + specs
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


def get_compilers(spack):
    """Get compilers spack knows about"""
    return [x.strip() for x in spack.compiler.list().split("\n")[2:] if x.strip()]


def _update_compiler_conf(compiler_conf: Path, binutils_path: Path):
    """Update spack compiler config to prepend binutils_path to PATH"""
    data = yaml.safe_load(compiler_conf.open())
    comp_data = data.get("compilers")
    if comp_data is None:
        comp_data = data["spack"]["compilers"]
    for comp_info in comp_data:
        comp_env = comp_info["compiler"]["environment"]
        if "prepend_path" not in comp_env:
            comp_env["prepend_path"] = {"PATH": str(binutils_path / "bin")}
    yaml.dump(data, compiler_conf.open("w"))


def setup_build_chains(
    spack: sh.Command,
    spack_install: sh.Command,
    spack_comp_find: sh.Command,
    buildchains: List[SpackBuildChain],
    conf_path: Path,
    conf_scope: str,
) -> None:
    """Configure one or more buildchains, installing any missing pieces as needed"""
    compilers = get_compilers(spack)
    missing_build_deps = set()
    for bc in buildchains:
        compiler = binutils = None
        if bc.compiler is not None and bc.compiler not in missing_build_deps:
            compiler = bc.compiler
            # TODO: Using 'startswith' here is hacky but works for common use of just
            #       specifying the major version and then matching the full version (e.g.
            #       spec "gcc@12" and then match "gcc@12.3.0"). Supporting specs with less-
            #       than or greater-than could be useful.
            if not any(c.startswith(compiler) for c in compilers):
                try:
                    spack.find(compiler)
                except sh.ErrorReturnCode:
                    missing_build_deps.add(compiler)
                else:
                    comp_loc = spack.location(first=True, i=compiler).strip()
                    spack_comp_find("--scope", conf_scope, comp_loc)
        if bc.binutils is not None:
            binutils = bc.binutils
            if bc.compiler is None:
                # TODO: A user could have explicity listed a system compiler here...
                #       Would be nice to just support system compilers here, just awkward
                #       to implement since we store them in the global spack "site" level
                #       config currently, to avoid overloading the environments spack.yaml
                #       with a bunch of compiler definitions that might not get used in that
                #       environment. If we set the binutils version in the global config
                #       we can't build environments in parallel. Need to see if we can
                #       override the system config of a compiler with and environment config.
                raise ValueError("Can't set non-system binutils for system compiler")
            # TODO: I guess that enabling the assembler here should be an option, not
            #       sure why it's not the default in spack...
            binutils = f"{binutils} +gas"
            try:
                spack.find(binutils)
            except sh.ErrorReturnCode:
                missing_build_deps.add(binutils)
            else:
                binutils_path = Path(spack.location(first=True, i=binutils).strip())
                _update_compiler_conf(conf_path, binutils_path)
    if missing_build_deps:
        log.info("Installing missing build dependencies: %s", missing_build_deps)
        for build_dep in missing_build_deps:
            spack_install(build_dep)
        for bc in buildchains:
            if bc.compiler in missing_build_deps:
                comp_loc = spack.location(first=True, i=bc.compiler).strip()
                spack_comp_find("--scope", conf_scope, comp_loc)
                binutils_path = Path(spack.location(first=True, i=bc.binutils).strip())
                _update_compiler_conf(conf_path, binutils_path)


def _prep_spack_build(
    env_dir: Path,
    snap_path: Path,
    spack: sh.Command,
    spack_env: sh.Command,
    spack_config: SpackConfig,
    build_config: BuildConfig,
    base_tmp: Path,
):
    """Prepare an environment for building"""
    # Initialize the environment config file
    env_dir.mkdir(parents=True)
    if spack_config.config is not None:
        env_info = deepcopy(spack_config.config)
    else:
        env_info = {}
    env_info["specs"] = spack_config.specs[:]
    if not env_info["specs"]:
        log.warning("No specs defined for spack env: %s", env_dir)
        return
    env_info["view"] = {
        "default": {
            "root": str(snap_path),
            "link": "all",
            "link_type": "symlink",
        }
    }
    spec_path = env_dir / "spack.yaml"
    with spec_path.open("wt") as spec_f:
        yaml.safe_dump({"spack": env_info}, spec_f)
    # Setup any needed buildchains for the env
    if spack_config.build_chains is not None:
        spack_install = get_spack_install(spack, base_tmp, build_config=build_config)
        setup_build_chains(
            spack,
            spack_install,
            spack_env.compiler.find,
            spack_config.build_chains,
            env_dir / "spack.yaml",
            f"env:{env_dir}",
        )
    # Setup any externals for the env
    if spack_config.externals:
        for external in spack_config.externals:
            spack_env.external.find("--scope", f"env:{env_dir}", external)


def _update_spack_env(
    env_dir: Path,
    snap_path: Path,
    spack: sh.Command,
    spack_install: sh.Command,
    spack_concretize: sh.Command,
    spack_push: sh.Command,
) -> None:
    """Create updated snapshot of a single environment and cache any built packages"""
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
        if snap_path.exists():
            shutil.rmtree(snap_path)
        raise install_err
    for sh_type in ("sh", "csh", "fish"):
        act_script = spack.env.activate(f"--{sh_type}", "-d", str(env_dir))
        with open(f"{snap_path}_activate.{sh_type}", "wt") as out_f:
            out_f.write(act_script)


def update_spack_env(
    spack: sh.Command,
    env_name: str,
    spack_config: SpackConfig,
    locs: Dict[str, Path],
    snap_id: SnapId,
    build_config: Optional[BuildConfig] = None,
) -> Optional[SnapSpec]:
    """Update and snapshot a spack environment"""
    if build_config is None:
        build_config = BuildConfig()
    spack_envs_dir = locs["envs"] / "spack"
    spack_envs_dir.mkdir(exist_ok=True)
    env_dir = spack_envs_dir / env_name / f"{snap_id}-env"
    snap_path = spack_envs_dir / env_name / str(snap_id)
    spack_env = spack.bake(e=env_dir)
    log.info("Updating spack snap: %s", snap_path)
    try:
        _prep_spack_build(
            env_dir, snap_path, spack, spack_env, spack_config, build_config, locs["tmp"]
        )
    except:
        log.exception("Error preparing spack environment: %s", env_dir)
        # TODO:
        #if env_dir.exists():
        #    shutil.rmtree(env_dir)
        return None
    # Prepare spack concretize/install/push commands for the environment
    spack_install = get_spack_install(
        spack_env, locs["tmp"], str(snap_id), build_config=build_config
    )
    spack_concretize = get_spack_concretize(spack_env, build_config=build_config)
    spack_push = get_spack_push(spack_env, build_config)
    lock_path = spack_envs_dir / env_name / f"{snap_id}.lock"
    success = True
    try:
        _update_spack_env(
            env_dir, snap_path, spack, spack_install, spack_concretize, spack_push
        )
    except:
        log.error("Error building spack environment: %s", env_dir)
        success = False
    else:
        conv_view_links(snap_path)
        shutil.copy(env_dir / "spack.lock", lock_path)
    log.info("Updating spack buildcache index")
    try:
        spack.buildcache("update-index", "default")
    except sh.ErrorReturnCode:
        log.exception("Error while updating spack buildcache index")
    if not success:
        # TODO:
        #if env_dir.exists():
        #    shutil.rmtree(env_dir)
        return None
    return SnapSpec.from_lock_path(lock_path)


def get_spack_env_cmds(
    spack_env: Path,
    cmds: List[str],
    base_env: Optional[Dict[str, str]] = None,
    log_file: Optional[TextIOWrapper] = None,
) -> List[sh.Command]:
    """Get sh.Command referencing a command in the given environment"""
    act_path = spack_env.parent / f"{spack_env.name}_activate.sh"
    act_env = get_activated_envrion([act_path.read_text()], base_env)
    env_bin = spack_env / "bin"
    return [get_env_cmd(env_bin / cmd, act_env, log_file) for cmd in cmds]


def get_spack_pkg_cmds(
    spec: str,
    cmds: List[str],
    spack: sh.Command,
    spack_install: sh.Command,
    base_env: Optional[Dict[str, str]] = None,
    log_file: Optional[TextIOWrapper] = None,
) -> sh.Command:
    """Get a command from a spack package, installing it if needed"""
    try:
        spack.find(spec)
    except sh.ErrorReturnCode:
        log.info("Installing spack package: %s", spec)
        spack_install([spec])
    act_script = spack.load("--sh", spec)
    act_env = get_activated_envrion([act_script], base_env)
    return [get_env_cmd(cmd, act_env, log_file) for cmd in cmds]
