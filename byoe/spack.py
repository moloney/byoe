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
from .util import get_activated_envrion, get_env_cmd, srun_wrap, HAS_SLURM


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


def get_spack_install(
    spack: sh.Command,
    fresh: bool = True,
    n_tasks: Optional[int] = None,
    use_slurm: bool = True,
    slurm_opts: Optional[Dict] = None,
) -> sh.Command:
    """Get a preconfigured 'spack install' command"""
    install_args = []
    if fresh:
        install_args.append("--fresh")
    if HAS_SLURM and use_slurm:
        if slurm_opts is None:
            slurm_opts = {}
        if n_tasks is None:
            n_tasks = slurm_opts.get("tasks_per_job", DEFAULT_SLURM_TASKS)
        install_args.append(f"-j {n_tasks}")
    elif n_tasks:
        install_args += ["-j", n_tasks]
    spack_install = spack.install.bake(*install_args)
    if HAS_SLURM and use_slurm:
        spack_install = srun_wrap(
            spack_install,
            n_tasks,
            slurm_opts.get("srun_args", ""),
            slurm_opts.get("tmp_dir"),
        )
    return spack_install


def get_spack_concretize(
    spack: sh.Command,
    fresh: bool = True,
    n_tasks: Optional[int] = None,
    use_slurm: bool = True,
    slurm_opts: Optional[Dict] = None,
) -> sh.Command:
    """Get a preconfigured 'spack concretize' command"""
    conc_args = []
    if fresh:
        conc_args.append("--fresh")
    if HAS_SLURM and use_slurm:
        if slurm_opts is None:
            slurm_opts = {}
        if n_tasks is None:
            n_tasks = slurm_opts.get("tasks_per_job", DEFAULT_SLURM_TASKS)
        conc_args.append(f"-j {n_tasks}")
    elif n_tasks:
        install_args += ["-j", n_tasks]
    spack_concretize = spack.concretize.bake(*conc_args)
    if HAS_SLURM and use_slurm:
        spack_concretize = srun_wrap(
            spack_concretize,
            n_tasks,
            slurm_opts.get("srun_args", ""),
            slurm_opts.get("tmp_dir"),
        )
    return spack_concretize


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
) -> None:
    """Create updated snapshot of a single environment"""
    start = datetime.now()
    log.info("Concretizing spack snapshot: %s", snap_path)
    conc_out = spack_concretize()
    if not conc_out.strip():
        log.info("No updates for spack snapshot: %s", snap_path)
    log.info("Building spack snapshot: %s", snap_path)
    spack_install([])
    # time.sleep(90)
    for sh_type in ("sh", "csh", "fish"):
        act_script = spack.env.activate(f"--{sh_type}", dir=str(env_dir))
        with open(f"{snap_path}_activate.{sh_type}", "wt") as out_f:
            out_f.write(act_script)
    log.info(
        "Finished spack snapshot: %s (took %s)", snap_path, (datetime.now() - start)
    )


def update_spack_envs(
    update_ts: str,
    conf: Dict[str, Any],
    locs: Dict[str, Path],
    envs: Optional[List[str]] = None,
    n_tasks: Optional[int] = None,
    log_file: Optional[TextIOWrapper] = None,
) -> Dict[str, Path]:
    """Create updated snapshots for the configured spack environments"""
    if envs is not None:
        envs = set(envs)
    created: Dict[str, Path] = {}
    for env_name, env_info in conf["spack"].get("envs", {}).items():
        if envs is not None and env_name not in envs:
            continue
        env_dir = locs["spack_env_dir"] / env_name
        env_dir.mkdir(parents=True, exist_ok=True)
        spec_path = env_dir / "spack.yaml"
        snap_name = f"{env_name}-{update_ts}"
        snap_path = locs["spack_env_dir"] / snap_name
        log.info("Updating spack snap: %s", snap_path)
        env_info = deepcopy(env_info)
        env_info["specs"] += conf["spack"].get("global_specs", [])
        # TODO: Once spack fixes bug with hardlink views, use those here
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
        #       misbehave when the environment is already activated (skips setting some
        #       env vars). Could be better to pass the "--env" arg to spack? Or fix
        #       the activate bug in spack.
        spack = get_spack(locs, log_file=log_file)
        spack_env = get_spack(locs, env_dir, log_file=log_file)
        use_slurm = conf.get("build_on_slurm", True)
        slurm_opts = conf["spack"].get("slurm_opts", {})
        use_slurm = use_slurm and slurm_opts.get("enabled", True)
        spack_install = get_spack_install(
            spack_env,
            n_tasks=n_tasks,
            use_slurm=use_slurm,
            slurm_opts=slurm_opts.get("install", {}),
        )
        spack_concretize = get_spack_concretize(
            spack_env,
            n_tasks=n_tasks,
            use_slurm=use_slurm,
            slurm_opts=slurm_opts.get("concretize", {}),
        )
        _update_spack_env(env_dir, snap_path, spack, spack_install, spack_concretize)
        conv_view_links(snap_path)
        shutil.copy(env_dir / "spack.lock", locs["spack_env_dir"] / f"{snap_name}.lock")
        created[env_name] = snap_path
    return created


def get_latest_spack_snap(env_name: str, spack_env_dir: Path) -> Path:
    lock_files = list(spack_env_dir.glob(f"{env_name}*.lock"))
    lock_files.sort()
    snap_name = lock_files[0].stem
    snap_path = spack_env_dir / snap_name
    if not snap_path.exists() or not snap_path.is_dir():
        raise ValueError("Can't find snapshot for lock file: %s", lock_files[0])
    return snap_path


def get_spack_env_cmds(
    spack_env: Path, cmds: List[str], log_file: Optional[TextIOWrapper]
) -> List[sh.Command]:
    """Get sh.Commnad referencing a command in the given environment"""
    act_path = spack_env.parent / f"{spack_env.name}_activate.sh"
    act_env = get_activated_envrion([act_path.read_text()])
    env_bin = spack_env / "bin"
    return [get_env_cmd(env_bin / cmd, act_env, log_file) for cmd in cmds]
