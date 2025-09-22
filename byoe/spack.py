"""Manage / build spack environments"""

import json
import os, logging, shutil
from datetime import datetime
from pathlib import Path
from copy import deepcopy
from io import TextIOWrapper
import tempfile
from typing import List, Dict, Optional, Any, Tuple

import yaml
import sh # type: ignore

from .globals import ShellType
from .snaps import SnapSpec, SnapId
from .util import get_activated_envrion, get_cmd, srun_wrap, wrap_cmd, unexpand_act_vars
from .conf import BuildConfig, SpackConfig, SpackToolchainConfig, get_job_build_info

sh = sh.bake(_tty_out=False)
git = sh.git


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
    if args is not None:
        args = args[:]
    else:
        args = []
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
    if args:
        cmd = cmd.bake(*args)
    if build_info["use_slurm"] and not slurm_cpus:
        if n_tasks is None:
            n_tasks = 1
        cmd = srun_wrap(cmd, n_tasks, build_info["srun_args"], build_info["tmp_dir"])
    return cmd


def get_spack_install(
    spack: sh.Command,
    base_tmp: Path,
    run_id: Optional[str] = None,
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
    install_args = ["-p", "1"]
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


def get_installed(spack: sh.Command) -> List[str]:
    """Get list of installed packages with version and hash"""
    installed = json.loads(spack.find(json=True))
    return [f"{x['name']}@{x['version']}/{x['hash']}" for x in installed]


def get_concretized_roots(lock_path: Path) -> List[str]:
    roots = sorted(json.loads(lock_path.read_text())["roots"], key=lambda x: x["spec"])
    return [f"{x['spec']}/{x['hash']}" for x in roots]


def get_spack_push(
    spack: sh.Command,
    build_config: Optional[BuildConfig] = None,
):
    """Get preconfigured 'spack buildcache push' command"""
    if build_config is None:
        build_config = BuildConfig()
    push_args = ["default"] + get_installed(spack)
    log.debug("Args being passed to spack_push: %s", push_args)
    return par_spack(
        spack.buildcache.push, push_args, get_job_build_info(build_config, "spack_push")
    )


def get_extern_compilers(spack):
    """Get external compilers spack knows about"""
    lines = [x.strip() for x in spack.compiler.list().split("\n")[2:] if x.strip().startswith("[e]")]
    return [" ".join(line.split()[1:]) for line in lines]


def install_toolchain(
    spack: sh.Command,
    toolchain: SpackToolchainConfig,
    base_tmp: Path,
    build_config: Optional[BuildConfig] = None,
):
    """Ensure that any packages referenced by `toolchain` are installed"""
    pkg_specs = toolchain.get_internal_packages()
    extern_compilers = set(get_extern_compilers(spack))
    missing = []
    for pkg_spec in pkg_specs:
        if pkg_spec in extern_compilers:
            continue
        try:
            spack.find(pkg_spec)
        except sh.ErrorReturnCode:
            missing.append(pkg_spec)
    if not missing:
        return
    log.info("Installing missing toolchain dependencies: %s", missing)
    # Create a temp dir to configure our build
    with tempfile.TemporaryDirectory(dir=base_tmp) as temp_dir:
        conf_path = Path(temp_dir) / "spack.yaml"
        conf_data = toolchain.spack.to_spack_conf()
        log.debug(f"Doing toolchain build with {conf_data=}")
        conf_path.write_text(yaml.safe_dump({"spack": conf_data}))
        spack_install = get_spack_install(spack.bake(["-C", str(conf_path.parent)]), base_tmp, build_config=build_config)
        for pkg_spec in missing:
            spack_install(pkg_spec)


# TODO: This is incorrect approach after Spack v1.0, should use toolchain instead
# def _update_compiler_conf(compiler_conf: Path, binutils_path: Path):
#     """Update spack compiler config to prepend binutils_path to PATH"""
#     data = yaml.safe_load(compiler_conf.open())
#     comp_data = data.get("compilers")
#     if comp_data is None:
#         comp_data = data["spack"]["compilers"]
#     for comp_info in comp_data:
#         comp_env = comp_info["compiler"]["environment"]
#         if "prepend_path" not in comp_env:
#             comp_env["prepend_path"] = {"PATH": str(binutils_path / "bin")}
#     yaml.dump(data, compiler_conf.open("w"))


# def setup_build_chains(
#     spack: sh.Command,
#     spack_install: sh.Command,
#     spack_comp_find: sh.Command,
#     buildchains: List[SpackBuildChain],
#     conf_path: Path,
#     conf_scope: str,
# ) -> None:
#     """Configure one or more buildchains, installing any missing pieces as needed"""
#     compilers = get_compilers(spack)
#     missing_build_deps = set()
#     for bc in buildchains:
#         compiler = binutils = None
#         extra_args = []
#         if bc.target:
#             extra_args.append(f"target={bc.target}")
#         if bc.compiler is not None and bc.compiler not in missing_build_deps:
#             compiler = bc.compiler
#             # TODO: Using 'startswith' here is hacky but works for common use of just
#             #       specifying the major version and then matching the full version (e.g.
#             #       spec "gcc@12" and then match "gcc@12.3.0"). Supporting specs with less-
#             #       than or greater-than could be useful.
#             if not any(c.startswith(compiler) for c in compilers):
#                 try:
#                     spack.find(compiler, *extra_args)
#                 except sh.ErrorReturnCode:
#                     missing_build_deps.add(compiler)
#                 else:
#                     comp_loc = spack.location(["--first", "-i", compiler] + extra_args).strip()
#                     spack_comp_find("--scope", conf_scope, comp_loc)
#         if bc.binutils is not None:
#             binutils = bc.binutils
#             if bc.compiler is None:
#                 # TODO: A user could have explicity listed a system compiler here...
#                 #       Would be nice to just support system compilers here, just awkward
#                 #       to implement since we store them in the global spack "site" level
#                 #       config currently, to avoid overloading the environments spack.yaml
#                 #       with a bunch of compiler definitions that might not get used in that
#                 #       environment. If we set the binutils version in the global config
#                 #       we can't build environments in parallel. Need to see if we can
#                 #       override the system config of a compiler with and environment config.
#                 raise ValueError("Can't set non-system binutils for system compiler")
#             # TODO: I guess that enabling the assembler here should be an option, not
#             #       sure why it's not the default in spack...
#             binutils = f"{binutils} +gas"
#             try:
#                 spack.find(binutils, *extra_args)
#             except sh.ErrorReturnCode:
#                 missing_build_deps.add(binutils)
#             else:
#                 binutils_path = Path(spack.location(["--first", "-i", binutils] + extra_args).strip())
#                 _update_compiler_conf(conf_path, binutils_path)
#     if missing_build_deps:
#         log.info("Installing missing build dependencies: %s", missing_build_deps)
#         for build_dep in missing_build_deps:
#             spack_install(build_dep, *extra_args)
#         for bc in buildchains:
#             if bc.compiler in missing_build_deps:
#                 comp_loc = spack.location(["--first", "-i", bc.compiler] + extra_args).strip()
#                 spack_comp_find("--scope", conf_scope, comp_loc)
#                 binutils_path = Path(spack.location(["--first", "-i", bc.binutils] + extra_args).strip())
#                 _update_compiler_conf(conf_path, binutils_path)


def _prep_spack_build(
    env_dir: Path,
    snap_path: Path,
    spack_env: sh.Command,
    spack_config: SpackConfig,
) -> None:
    """Prepare an environment for building"""
    # Initialize the environment config file
    env_dir.mkdir(parents=True)
    env_info = spack_config.to_spack_conf()
    if not env_info["specs"]:
        log.warning("No specs defined for spack env: %s", env_dir)
        return
    env_info["view"] = {
        "default": {
            "root": str(snap_path),
            "link": "all",
            "link_type": "hardlink",
        }
    }
    spec_path = env_dir / "spack.yaml"
    with spec_path.open("wt") as spec_f:
        yaml.safe_dump({"spack": env_info}, spec_f)
    # Setup any externals for the env
    if spack_config.externals:
        # We need to handle variants for externals ourselves as spack doesn't 
        extern_variants = {}
        for external in spack_config.externals:
            toks = external.split()
            pkg = toks[0]
            if len(toks) > 1:
                extern_variants[pkg] = " ".join(toks[1:])
            log.debug("Checking for external (system) version of pkg: %s", pkg)
            spack_env.external.find("--scope", f"env:{env_dir}", pkg)
        # Add variants info for external package specs
        if extern_variants:
            spec = yaml.safe_load(spec_path.read_text())
            for pkg, variants in extern_variants.items():
                extern_spec = spec["spack"].get("packages", {}).get(pkg)
                if extern_spec is not None:
                    extern_spec = extern_spec.get("externals", (None,))[0]
                if extern_spec is not None:
                    extern_spec["spec"] = f"{extern_spec['spec']} {variants}"
            with spec_path.open("wt") as spec_f:
                yaml.safe_dump(spec, spec_f)


def _update_spack_env(
    env_dir: Path,
    snap_id: SnapId,
    snap_path: Path,
    spack: sh.Command,
    spack_install: sh.Command,
    spack_concretize: sh.Command,
    best_effort: bool = False,
) -> bool:
    """Create updated snapshot of a single environment
    
    If there is an error and `best_effort` is false we will stash the failed build as 
    a hidden dir for potential debugging, otherwise we will keep the partial build.
    """
    start = datetime.now()
    # Concretize package specs for the environment and detect dupes
    log.info("Concretizing spack snapshot: %s", snap_path)
    try:
        spack_concretize()
        lock_path = env_dir.parent / f"{snap_id}.lock"
        shutil.copy(env_dir / "spack.lock", lock_path)
        snap = SnapSpec.from_lock_path(lock_path)
        if snap.dedupe():
            return True
    except Exception:
        log.exception("Error creating lockfile for snapshot: %s")
        snap.stash_failed()
        raise
    log.info("Building spack snapshot: %s", snap_path)
    # TODO: Do we need "--keep-prefix" for best effort install into an env?
    install_args = []
    no_errors = True
    try:
        spack_install(install_args)
    except Exception:
        if best_effort:
            log.warning("Error occured but keeping best effort spack snap: %s", snap_path)
            success = False
        else:
            log.exception("Error building spack snapshot: %s", snap_path)
            snap.stash_failed()
            raise
    try:
        for sh_type in ("sh", "csh", "fish"):
            act_script = spack.env.activate(f"--{sh_type}", "-d", str(env_dir))
            # TODO: We unexpand envvars to make activation script reusable, but this shouldn't
            #       be needed once this is merged: https://github.com/spack/spack/pull/47755
            act_script = unexpand_act_vars(act_script, ShellType(sh_type))
            with open(f"{snap_path}-activate.{sh_type}", "wt") as out_f:
                out_f.write(act_script)
    except Exception:
        log.exception("Error creating activation scrpits for spack snapshot: %s", snap_path)
        snap.stash_failed()
        raise
    log.info(
        "Finished spack snapshot: %s (took %s)", snap_path, (datetime.now() - start)
    )
    return no_errors


def update_spack_env(
    spack: sh.Command,
    env_name: str,
    spack_config: SpackConfig,
    locs: Dict[str, Path],
    snap_id: SnapId,
    build_config: Optional[BuildConfig] = None,
    best_effort: bool = False,
) -> Tuple[SnapSpec, bool]:
    """Build updated snapshot of a spack environment"""
    if build_config is None:
        build_config = BuildConfig()
    spack_envs_dir = locs["envs"] / "spack"
    spack_envs_dir.mkdir(exist_ok=True)
    env_dir = spack_envs_dir / env_name / f"{snap_id}-env"
    snap_path = spack_envs_dir / env_name / str(snap_id)
    lock_path = spack_envs_dir / env_name / f"{snap_id}.lock"
    snap = SnapSpec.from_lock_path(lock_path)
    spack_env = spack.bake(e=env_dir)
    log.info("Updating spack snap: %s", snap_path)
    try:
        _prep_spack_build(
            env_dir,
            snap_path,
            spack_env,
            spack_config,
        )
    except:
        snap.stash_failed()
        raise
    # Prepare spack concretize / install commands for the environment
    spack_install = get_spack_install(
        spack_env, locs["tmp"], str(snap_id), build_config=build_config
    )
    spack_concretize = get_spack_concretize(spack_env, build_config=build_config)
    try:
        no_errors = _update_spack_env(
            env_dir,
            snap_id,
            snap_path,
            spack,
            spack_install,
            spack_concretize,
            best_effort,
        )
    finally:
        log.info("Updating spack buildcache index")
        try:
            spack.buildcache("update-index", "default")
        except sh.ErrorReturnCode:
            log.exception("Error while updating spack buildcache index")
    return (snap, no_errors)


def unset_implicit_pypath(spack_env: Path, act_env: Dict[str, str]) -> None:
    # It seems like spack sets PYTHONPATH  when not needed, which screws up layering of
    # virtual envs. Unset it here with some sanity checks that only the implicit
    # "site-packages" dir is included before we get rid of it
    py_path = act_env.get("PYTHONPATH")
    if py_path:
        py_paths = py_path.split(os.pathsep)
        if len(py_paths) != 1 or not py_paths[0].startswith(str(spack_env / "lib")):
            log.warning("Unsetting PYTHONPATH with unexcpected components: %s", py_path)
        del act_env["PYTHONPATH"]


def get_spack_env_cmds(
    spack_env: Path,
    cmds: List[str],
    base_env: Optional[Dict[str, str]] = None,
    log_file: Optional[TextIOWrapper] = None,
) -> List[sh.Command]:
    """Get sh.Command referencing a command in the given environment"""
    act_path = spack_env.parent / f"{spack_env.name}-activate.sh"
    act_env = get_activated_envrion([act_path.read_text()], base_env)
    unset_implicit_pypath(spack_env, act_env)
    env_bin = spack_env / "bin"
    return [get_cmd(env_bin / cmd, act_env, log_file, log_file) for cmd in cmds]


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
    return [get_cmd(cmd, act_env, log_file, log_file) for cmd in cmds]
