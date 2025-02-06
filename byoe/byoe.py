"""Bring Your Own Environment

The `ByoeRepo` class provides the user API for interacting with a BYOE repository.
"""

import sys, os, logging, re
from dataclasses import dataclass, asdict
from io import TextIOWrapper
from copy import deepcopy
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import yaml
import sh # type: ignore

from .globals import (
    LOCK_SUFFIXES,
    TS_FORMAT,
    CHANNEL_UPDATE_MONTHS,
    SnapType,
    EnvType,
    UpdateChannel,
    ShellType,
)
from .snaps import SnapId, SnapSpec, InvalidSnapIdError
from .util import (
    get_closest_snap,
    get_ssl_env,
    select_snap,
    get_cmd,
    get_activated_envrion,
)
from .conf import (
    CondaAppConfig, 
    PythonAppConfig, 
    ApptainerAppConfig, 
    SiteConfig, 
    IncludableConfig, 
    get_site_conf
)
from .spack import (
    get_spack_install,
    get_spack_pkg_cmds,
    get_spack_env_cmds,
    update_spack_env,
    get_compilers,
    setup_build_chains,
)
from .python_venv import update_python_env, update_python_app
from .conda import fetch_prebuilt, update_conda_app, update_conda_env
from .apptainer import install_unpriv_apptainer, update_apptainer_app

sh = sh.bake(_tty_out=False)
git = sh.git


log = logging.getLogger(__name__)


class NoCompilerFoundError(Exception):
    pass


PS1_ACT_TEMPLATE = """
if [ -z "${{BYOE_ENV_DISABLE_PROMPT:-}}" ] ; then
    _OLD_BYOE_PS1="${{PS1:-}}"
    PS1="({snap_name}) ${{PS1:-}}"
    export PS1
    BYOE_ENV_PROMPT="(202404) "
    export BYOE_ENV_PROMPT
fi
"""


# TODO: Need to support external directory with env / apps for situations like users
#       importing a "workspace"
class ByoeRepo:
    """A repository is defined by a base directory with a specific structure"""

    def __init__(self, base_dir: Path):
        # Make sure we have access to SSL certs even if using PBS
        os.environ.update(get_ssl_env())
        self._base_dir = base_dir
        # Track various path locations
        internal_dir = base_dir / "._internal"
        pkg_cache = base_dir / "pkg_cache"
        confd = base_dir / "conf.d"
        self._locs = {
            "confd" : confd,
            "envs_confd" : confd / "envs",
            "apps_confd" : confd / "apps",
            "bin" : base_dir / "bin",
            "startup" : base_dir / "user_rc",
            "log" : base_dir / "logs",
            "tmp" : base_dir / "tmp",
            "licenses" : base_dir / "licenses",
            "internal" : base_dir / internal_dir,
            "spack" : internal_dir / "spack",
            "conda" : internal_dir / "conda",
            "pipx" : internal_dir / "pipx",
            "apptainer" : internal_dir / "apptainer",
            "envs" : base_dir / "envs",
            "apps" : base_dir / "apps",
            "pkg_cache" : pkg_cache,
            "spack_cache" : pkg_cache / "spack",
            "python_cache" : pkg_cache / "python",
            "conda_cache" : pkg_cache / "conda",
            "apptainer_cache" : pkg_cache / "apptainer"
        }
        for loc in (
            "confd",
            "envs_confd",
            "apps_confd",
            "bin",
            "startup",
            "log",
            "tmp",
            "licenses",
            "internal",
            "envs",
            "apps",
            "pkg_cache",
            "spack_cache",
            "python_cache",
            "conda_cache",
            "apptainer_cache",
        ):
            self._locs[loc].mkdir(exist_ok=True)
        IncludableConfig.base_dir = base_dir
        site_conf_path = base_dir / "site_conf.yaml"
        if not site_conf_path.exists():
            if sys.stdout.isatty():
                site_conf = SiteConfig.build_interactive()
            else:
                log.warning("No site config found, creating default")
                site_conf = SiteConfig()
            site_conf_path.write_text(yaml.dump(site_conf.to_dict()))
        self._site_conf = get_site_conf(
            site_conf_path, self._locs["envs_confd"], self._locs["apps_confd"]
        )
        self._is_prepped = False
        if not os.access(base_dir / "site_conf.yaml", os.W_OK):
            log.debug("User doesn't have write access to repo")
            return
        
        # Symlink the 'byoe' cli entry point into the bin dir
        self_bin = self._locs["bin"] / "byoe"
        if not self_bin.exists():
            self_bin.symlink_to(os.path.relpath(sys.argv[0], self_bin.parent))
        # Build user startup scripts
        for shell_type in ("sh",):
            startup_lines = [f"export PATH={self._locs['bin']}:$PATH"]
            startup_lines.append(
                f"export PIP_FIND_LINKS={str(self._locs['python_cache'])}' '$PIP_FIND_LINKS"
            )
            startup_lines.append(f"export CONDA_PKGS_DIRS={self._locs['conda_cache']}")
            (self._locs["startup"] / f"byoe_startup.{shell_type}").write_text(
                "\n".join(startup_lines + [""])
            )
        
    @property
    def base_dir(self) -> Path:
        return self._base_dir

    def prep_dir(
        self, pull_spack: bool = True, log_file: Optional[TextIOWrapper] = None
    ) -> None:
        """Perform any baseline setup, particularly installing/updating spack"""
        if self._is_prepped:
            return
        # Create / update spack git repo
        branch = self._site_conf.spack_global.repo_branch
        spack_dir = self._locs["spack"]
        if not (spack_dir / ".git").exists():
            log.info("Cloning spack into: %s", spack_dir)
            kwargs = {}
            if branch:
                kwargs["branch"] = branch
            git.clone(self._site_conf.spack_global.repo_url, spack_dir, **kwargs)
        elif pull_spack:
            # TODO: Should detect / handle changes to branch (and repo)
            log.info("Updating local version of spack repo")
            git("--git-dir", f"{spack_dir / '.git'}", "fetch")
            tgt = "origin"
            if branch:
                tgt = f"origin/{branch}"
            git(
                "--git-dir",
                f"{spack_dir / '.git'}",
                "--work-tree",
                str(spack_dir),
                "reset",
                "--hard",
                tgt,
            )
        # Link byoe "licenses" dir into spack
        spack_lic_dir = spack_dir / "etc" / "spack" / "licenses"
        if not spack_lic_dir.exists():
            spack_lic_dir.symlink_to(
                os.path.relpath(self._locs["licenses"], spack_lic_dir.parent), True
            )
        # Create / update global spack config
        spack_global_config_path = spack_dir / "etc" / "spack"
        (spack_global_config_path / "config.yaml").write_text(
            yaml.dump({"config": {"install_tree": {"padded_length": 128}}})
        )
        # Configure default spack mirror
        mirrors = self._site_conf.spack_global.mirrors
        if mirrors is None:
            mirrors = {}
        if "default" not in mirrors:
            mirrors["default"] = {"url": str(self._locs["spack_cache"]), "autopush": True}
        (spack_global_config_path / "mirrors.yaml").write_text(
            yaml.dump({"mirrors": mirrors})
        )
        # Get basic "spack" command wrapper
        spack = self._get_spack(log_file=log_file)
        # Setup GPG key for signing spack packages
        spack.gpg.init()
        def_gpg = {
            "name": "byoe_build",
            "email": "byoe@noreply.org",
            "comment": "Generated internally by BYOE for signing packages",
        }
        key_mtch = re.search(
            r"^\s+([0-9A-Z]+)\s+uid\s+(\[.+\])?\s*byoe_build\s.+", spack.gpg.list(), flags=re.MULTILINE
        )
        if not key_mtch:
            kwargs = {}
            if "comment" in def_gpg:
                kwargs["comment"] = def_gpg["comment"]
            spack.gpg.create(def_gpg["name"], def_gpg["email"], **kwargs)
            spack.gpg.publish(m="default")
        else:
            pub_hash = key_mtch.group(1)
            if not (self._locs["spack_cache"] / "build_cache" / "_pgp" / f"{pub_hash}.pub").exists():
                spack.gpg.publish(m="default")
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
        self._is_prepped = True

    def _get_spack(
        self,
        env: Optional[Path] = None,
        modules: Optional[List[str]] = None,  # TODO: Remove this?
        log_file: Optional[TextIOWrapper] = None,
    ) -> sh.Command:
        """Get internal spack command"""
        spack_dir = self._locs["spack"]
        tmp_dir = self._locs["tmp"]
        env_data = os.environ.copy()
        env_data.update(
            {
                "SPACK_ROOT": str(spack_dir),
                "PATH": f"{spack_dir / 'bin'}:{os.environ['PATH']}",
                "SPACK_PYTHON": sys.executable,
                "TMPDIR": str(tmp_dir),
                "SPACK_DISABLE_LOCAL_CONFIG": "true",
            }
        )
        # Handle alt locations for SSL/TLS certs in case we are using PBS python
        env_data.update(get_ssl_env())
        spack = get_cmd(self._locs["spack"] / "bin" / "spack", env_data, log_file)
        act_scripts = []
        if modules:
            act_scripts.append(spack.load("--first", "--sh", *modules))
        if env:
            spack = spack.bake(e=env)
        if act_scripts:
            env_data = get_activated_envrion(act_scripts, env_data)
            spack = spack.bake(_env=env_data)
        return spack

    def get_spack(
        self,
        env: Optional[Path] = None,
        modules: Optional[List[str]] = None,
        pull_spack: bool = True,
        log_file: Optional[TextIOWrapper] = None,
    ) -> sh.Command:
        """Get internal 'spack' command"""
        self.prep_dir(pull_spack=pull_spack)
        return self._get_spack(env, modules, log_file)

    def get_python(
        self,
        base_env: Optional[Dict[str, str]] = None,
        log_file: Optional[TextIOWrapper] = None,
    ) -> sh.Command:
        """Get the python command currently running this program"""
        if base_env is None:
            env_data = os.environ.copy()
        else:
            env_data = base_env.copy()
        env_data.update(get_ssl_env())
        return get_cmd(sys.executable, env_data, log_file=log_file)

    def get_pipx(
        self,
        base_env: Optional[Dict[str, str]] = None,
        log_file: Optional[TextIOWrapper] = None,
    ) -> sh.Command:
        """Get the internal 'pipx' command"""
        venv_path = self._locs["pipx"] / "pipx_venv"
        act_path = venv_path / "bin" / "activate"
        python = self.get_python(base_env, log_file)
        if not act_path.exists():
            self._locs["pipx"].mkdir(exist_ok=True)
            python("-m", "venv", str(venv_path))
        venv_env = get_activated_envrion(
            [act_path.read_text()], python._partial_call_args["env"]
        )
        pip = get_cmd("pip", venv_env, log_file)
        if not any(x.startswith("pipx") for x in pip.freeze().split("\n")):
            pip.install("pipx")
        return get_cmd("pipx", venv_env, log_file)

    def get_conda_lock(
        self,
        base_env: Optional[Dict[str, str]] = None,
        log_file: Optional[TextIOWrapper] = None,
    ) -> sh.Command:
        """Get the internal 'conda-lock' command"""
        venv_path = self._locs["conda"] / "conda_lock_venv"
        act_path = venv_path / "bin" / "activate"
        python = self.get_python(base_env, log_file)
        if not act_path.exists():
            python("-m", "venv", str(venv_path))
        # Make sure script for loading micromamba exists
        self.get_micromamba(log_file=log_file)
        venv_env = get_activated_envrion(
            [
                (self._locs["conda"] / "load_micromamba.sh").read_text(),
                act_path.read_text(),
            ],
            python._partial_call_args["env"],
        )
        pip = get_cmd("pip", venv_env, log_file)
        # TODO: Some sort of update mechanism?
        if not any(x.startswith("conda-lock") for x in pip.freeze().split("\n")):
            pip.install("conda-lock")
        return get_cmd("conda-lock", venv_env, log_file)

    def get_micromamba(self, log_file: Optional[TextIOWrapper] = None) -> sh.Command:
        """Get internal 'micromamba' command"""
        config = self._site_conf.conda_global
        mamba_activate_path = self._locs["conda"] / "load_micromamba.sh"
        mamba_activate_path.parent.mkdir(exist_ok=True)
        if config.source == "spack":
            spack = self.get_spack()
            spec = "micromamba"
            if config.build_chain is not None:
                compiler = config.build_chain.compiler
                if compiler:
                    spec = f"{spec} %{compiler}"
            try:
                spack.find(spec)
            except sh.ErrorReturnCode:
                spack_install = get_spack_install(
                    spack, self._locs["tmp"], build_config=self._site_conf.build_opts
                )
                if config.build_chain is not None:
                    setup_build_chains(
                        spack,
                        spack_install,
                        spack.compiler.find,
                        [config.build_chain],
                        self._locs["spack"] / "etc" / "spack" / "compilers.yaml",
                        "site",
                    )
                spack_install([spec])
            mamba_act_text = spack.load("--sh", spec)
        elif config.source.startswith("https://"):
            bin_dir = self._locs["conda"] / "bin"
            if not (bin_dir / "micromamba").exists():
                bin_dir.mkdir(exist_ok=True)
                fetch_prebuilt(bin_dir, config.source)
            mamba_act_text = f"export PATH={bin_dir}:$PATH\n"
        else:
            raise ValueError(f"Invalid 'source' for micromamba: {config.source}")
        mamba_act_text += f"export MAMBA_ROOT_PREFIX={self._locs['conda']}\n"
        mamba_act_text += (
            f"export CONDA_PKGS_DIRS={self._locs['pkg_cache'] / 'conda'}\n"
        )
        mamba_activate_path.write_text(mamba_act_text)
        # TODO: Bake in option to skip rcfiles too
        return get_cmd(
            "micromamba",
            get_activated_envrion([mamba_act_text]),
            log_file,
        ).bake(no_rc=True, yes=True)
    
    def get_apptainer(self, log_file: Optional[TextIOWrapper] = None) -> sh.Command:
        """Get the 'apptainer' command"""
        config = self._site_conf.apptainer_global
        env = os.environ.copy()
        env["APPTAINER_CACHEDIR"] = str(self._locs['apptainer_cache'])
        cmd = None
        if config.source in ("system", None):
            try:
                cmd = get_cmd("apptainer", env, log_file).bake(debug=True)
            except sh.CommandNotFound:
                if config.source == "system":
                    raise
            else:
                return cmd
        if not self._locs["apptainer"].exists():
            self._locs["apptainer"].mkdir()
            install_unpriv_apptainer(config.reloc_install_script, self._locs["apptainer"])
        # TODO: Determine where the binary shows up under the install dir
        raise NotImplementedError

    def _build_env_snap(
        self,
        env_name: str,
        snap_id: SnapId,
        log_file: Optional[TextIOWrapper] = None,
    ) -> Tuple[Dict[EnvType, SnapSpec], bool]:
        """Create new snapshot of an environment"""
        assert self._site_conf.envs is not None and env_name in self._site_conf.envs
        env_conf = self._site_conf.envs[env_name]
        snaps: Dict[EnvType, SnapSpec] = {}
        no_errors = True
        if env_conf.spack:
            spack = self.get_spack(log_file=log_file)
            try:
                snap, no_errors = update_spack_env(
                    spack,
                    env_name,
                    env_conf.spack,
                    self._locs,
                    snap_id,
                    self._site_conf.build_opts,
                    env_conf.best_effort,
                )
            except:
                return (snaps, False)
            snaps[EnvType.SPACK] = snap
            if env_conf.python:
                try:
                    python = get_spack_env_cmds(
                        snap.snap_path, ["python"], log_file=log_file
                    )[0]
                except sh.CommandNotFound:
                    if not no_errors:
                        log.warning(
                            ("Python build failed during best effort build of mixed "
                             "spack / python, skipping venv: %s"), 
                            env_name
                        )
                    else:
                        log.error(
                            ("Can't find python executable for mixed spack / python "
                             "env (missing from spack specs?): %s"), 
                            env_name
                        )
                        snaps[EnvType.SPACK].stash_failed()
                        del snaps[EnvType.SPACK]
                    return (snaps, False)
                try:
                    py_snap, py_no_errors = update_python_env(
                        env_name,
                        env_conf.python,
                        python,
                        self._locs,
                        snap_id,
                        env_conf.best_effort,
                    )
                except:
                    log.exception("Exception while updating python venv")
                    if not env_conf.best_effort:
                        snaps[EnvType.SPACK].stash_failed()
                        del snaps[EnvType.SPACK]
                    return (snaps, False)
                else:
                    snaps[EnvType.PYTHON] = py_snap
                if not py_no_errors:
                    no_errors = False
        elif env_conf.conda:
            snaps[EnvType.CONDA] = update_conda_env(
                env_name,
                env_conf.conda,
                self.get_conda_lock(log_file=log_file),
                self.get_micromamba(log_file=log_file),
                self._locs,
                snap_id,
            )
        return (snaps, no_errors)

    def _build_app_snap(
        self,
        app_name: str,
        snap_id: SnapId,
        log_file: Optional[TextIOWrapper] = None,
    ) -> Optional[SnapSpec]:
        assert self._site_conf.apps is not None and app_name in self._site_conf.apps
        app_conf = self._site_conf.apps[app_name]
        if isinstance(app_conf, PythonAppConfig):
            spack = self.get_spack(log_file=log_file)
            spack_install = get_spack_install(
                spack, self._locs["tmp"], build_config=self._site_conf.build_opts
            )
            # Get the python command for the app
            if app_conf.spack is None:
                spec = app_conf.python_spec
                if spec is None:
                    spec = "python"
                (py_cmd,) = get_spack_pkg_cmds(
                    spec, ["python"], spack, spack_install, log_file=log_file
                )
            else:
                try:
                    spack_env, _ = update_spack_env(
                        spack,
                        app_name,
                        app_conf.spack,
                        self._locs,
                        snap_id,
                        self._site_conf.build_opts,
                    )
                except:
                    return None
                py_cmd = get_spack_env_cmds(
                    spack_env.snap_path, ["python"], log_file=log_file
                )[0]
            # Use pipx to install into an isolated env
            pipx = self.get_pipx(py_cmd._partial_call_args["env"], log_file)
            res = update_python_app(
                app_name, app_conf.python, pipx, py_cmd, self._locs, snap_id
            )
            if res is None:
                # TODO: Stash failed here instead of removing?
                spack_env.remove(keep_lock=False)
            return res
        elif isinstance(app_conf, CondaAppConfig):
            assert isinstance(app_conf, CondaAppConfig)
            return update_conda_app(
                app_name,
                app_conf,
                self.get_conda_lock(log_file=log_file),
                self.get_micromamba(log_file=log_file),
                self._locs,
                snap_id,
            )
        else:
            assert isinstance(app_conf, ApptainerAppConfig)
            return update_apptainer_app(
                app_name, 
                app_conf, 
                self.get_apptainer(log_file=log_file),
                self._locs, 
                snap_id,
                self._site_conf.build_opts
            )

    def update(
        self,
        envs_or_apps: Optional[List[str]] = None,
        pull_spack: bool = True,
        label: Optional[str] = None,
        log_file: Optional[TextIOWrapper] = None,
    ):
        """Perform updates to configured environments and apps"""
        conf = self._site_conf
        if conf.envs is None and conf.apps is None:
            log.warning("Nothing to do, no 'envs' or 'apps' defined")
            return
        envs = [] if conf.envs is None else list(conf.envs.keys())
        apps = [] if conf.apps is None else list(conf.apps.keys())
        snap_id, reserve_path = self._allocate_snap_id(label=label)
        log.info("Using snap id: %s", snap_id)
        success = []
        partial = []
        failed = []
        try:
            self.prep_dir(pull_spack)
            env_snaps = {}
            for env_name in envs:
                if envs_or_apps is not None and env_name not in envs_or_apps:
                    continue
                snaps, no_errors = self._build_env_snap(env_name, snap_id, log_file)
                if len(snaps) == 0:
                    failed.append(env_name)
                else:
                    if not no_errors:
                        partial.append(env_name)
                    else:
                        success.append(env_name)
                    env_snaps[env_name] = snaps
            app_snaps = {}
            for app_name in apps:
                if envs_or_apps is not None and app_name not in envs_or_apps:
                    continue
                app_snaps[app_name] = self._build_app_snap(app_name, snap_id, log_file)
            (self._locs["envs"] / f"{snap_id}-site_conf.yaml").write_text(
                yaml.dump(self._site_conf.to_dict())
            )
        finally:
            reserve_path.unlink()
        log.info("Sucessfully updated envs/apps: %s", ", ".join(success))
        if partial:
            log.warning("Parital failure updating envs/apps: %s", ", ".join(partial))
        if failed:
            log.error("Failure updating envs/apps: %s", ", ".join(failed))

    def get_snaps(
        self, snap_type: SnapType, name: str, exists_only: bool = True
    ) -> List[Tuple[SnapSpec, ...]]:
        """Get list of snapshot tuples"""
        snaps = []
        base_path = self._locs["envs"]
        if snap_type == SnapType.APP:
            base_path = self._locs["apps"]
        for env_type in EnvType:
            name_dir = base_path / env_type.value / name
            if not name_dir.exists():
                continue
            lock_files = list(name_dir.glob(f"*{LOCK_SUFFIXES[env_type]}"))
            snap_specs = [SnapSpec.from_lock_path(f) for f in lock_files]
            if exists_only:
                snap_specs = [s for s in snap_specs if s.snap_path.exists()]
            snaps += snap_specs
        snaps.sort()
        res = []
        curr_group: List[SnapSpec] = []
        for snap in snaps:
            if not curr_group or curr_group[0].snap_id == snap.snap_id:
                curr_group.append(snap)
            else:
                res.append(tuple(curr_group))
                curr_group = [snap]
        if curr_group:
            res.append(tuple(curr_group))
        return res
    
    def get_snap(
        self, 
        channel: Optional[UpdateChannel] = None,
        snap_id: Optional[SnapId] = None,
    ) -> SnapId:
        if channel is None:
            channel = UpdateChannel(os.environ.get("BYOE_CHANNEL", "stable"))
        avail = []
        for pth in self._locs["envs"].glob("*-site_conf.yaml"):
            try:
                avail.append(SnapId.from_prefix(pth.name))
            except InvalidSnapIdError:
                pass
        if not avail:
            raise ValueError(f"No snapshots available")
        if snap_id is None:
            # Labeled snaps are never auto selected, must be explicitly requested
            avail = [x for x in avail if x.label is None]
            snap_id = select_snap(avail, CHANNEL_UPDATE_MONTHS[channel])
            log.info("Using channel %s selects snap: %s", channel, snap_id)
        elif snap_id not in avail:
            raise ValueError(f"No such snap: {snap_id}")
        return snap_id
    
    def get_env_snaps(
        self,
        snap_id: SnapId,
        snap_conf: Dict,
        env_name: str,
    ) -> Tuple[SnapSpec, ...]:
        if env_name not in snap_conf["envs"] and env_name not in snap_conf["apps"]:
            raise ValueError(f"Environment '{env_name}' not found in snap '{snap_id}'")
        env_snaps = get_closest_snap(
            snap_id, self.get_snaps(SnapType.ENV, env_name, exists_only=False)
        )
        if env_snaps is None:
            raise ValueError(f"No snaps available for environment: {env_name}")
        if any(not s.snap_path.exists() for s in env_snaps):
            raise ValueError(f"Env snap was deleted: {env_snaps[0]}")
        return env_snaps
    
    def _make_act_script(
        self,
        snap_id: SnapId,
        env_name: str,
        env_snaps: Tuple[SnapSpec, ...],
        enable: List[str],
        snap_conf: Dict,
        shell_type: ShellType
    ) -> str:
        """Generate the top-level activation script"""
        res = ["# This file was generated by 'byoe'"]
        activated_app_snaps: List[SnapSpec] = []
        # Load apps
        for app_name in enable:
            app_snaps = get_closest_snap(
                snap_id, self.get_snaps(SnapType.APP, app_name, exists_only=False)
            )
            if app_snaps is None:
                log.warning("No snaps available for app: %s", app_name)
                continue
            if any(not s.snap_path.exists() for s in app_snaps):
                log.warning("Skipping deleted snap: %s", app_snaps[0])
                continue
            activated_app_snaps.extend(app_snaps)
            res.append(f"\n# BYOE: Setup for '{app_name}' app")
            for app_snap in app_snaps:
                res.append(app_snap.get_activate_path(shell_type).read_text())
            extra_act = snap_conf["apps"][app_name].get("extra_activation")
            if extra_act:
                res.append(f"# BYOE: Extra activation for '{app_name}' app")
                res.append("\n".join(extra_act))
        # Load the environment
        if env_snaps:
            for snap in env_snaps:
                res.append(
                    f"\n# BYOE: Setup for '{snap.env_type}' layer of '{env_name}' environment"
                )
                if snap.env_type == EnvType.PYTHON:
                    res.append("VIRTUAL_ENV_DISABLE_PROMPT=1")
                res.append(snap.get_activate_path(shell_type).read_text())
                if snap.env_type == EnvType.SPACK:
                    # TODO: this is hacky
                    res.append("unset PYTHONPATH")
            extra_act = snap_conf["envs"][env_name].get("extra_activation")
            if extra_act:
                res.append(f"# BYOE: Extra activation for '{env_name}' env")
                res.append("\n".join(extra_act))
        # Add our own environment modifications
        res.append("\n# BYOE: Custom setup for BYOE itself")
        res.append(f"export BYOE_SNAP_ID={snap_id}")
        apps_path = os.pathsep.join(str(s) for s in activated_app_snaps)
        res.append(f"export BYOE_APPS={apps_path}")
        if env_snaps:
            envs_path = os.pathsep.join(str(s) for s in env_snaps)
            res.append(f"export BYOE_ENVS={envs_path}")
            snap_name = env_snaps[0].snap_name
        else:
            snap_name = str(snap_id)
        res.append(PS1_ACT_TEMPLATE.format(snap_name=snap_name))
        return "\n".join(res)

    def get_activate_script(
        self,
        env_name: Optional[str] = None,
        channel: Optional[UpdateChannel] = None,
        snap_id: Optional[SnapId] = None,
        skip_py_env: bool = False,
        disable: Optional[List[str]] = None,
        enable: Optional[List[str]] = None,
        shell_type: Optional[ShellType] = None,
    ) -> str:
        """Get the script needed to activate an environment / apps"""
        if env_name is None:
            env_name = os.environ.get("BYOE_DEFAULT_ENV_NAME", "main")
        if shell_type is None:
            curr_shell = Path(os.environ.get("SHELL", "/bin/bash"))
            shell_name = curr_shell.stem
            if shell_name == "bash":
                shell_type = ShellType.SH
            elif shell_name == "csh":
                shell_type = ShellType.CSH
            elif shell_name == "fish":
                shell_type = ShellType.FISH
            else:
                raise ValueError(f"Current shell is unsupported: {shell_name}")
        snap_id = self.get_snap(channel, snap_id)
        snap_conf = yaml.safe_load(
            (self._locs["envs"] / f"{snap_id}-site_conf.yaml").read_text()
        )
        env_snaps = self.get_env_snaps(snap_id, snap_conf, env_name)
        if skip_py_env:
            env_snaps = tuple(x for x in env_snaps if not x.env_type == EnvType.PYTHON)
        for s in env_snaps:
            if not s.supports_activation:
                raise ValueError(f"Snaps of type '{s.env_type}' don't support activation")
        snap_apps = snap_conf.get("apps", {})
        default_apps = [k for k, v in snap_apps.items() if v["default"] and k != env_name]
        if enable is None:
            enable = default_apps
        else:
            enable += [x for x in default_apps if x not in enable and x != env_name]
        if disable is not None:
            enable = [x for x in enable if x not in disable]
        return self._make_act_script(snap_id, env_name, env_snaps, enable, snap_conf, shell_type)
    
    def get_deactivate_script(self, shell_type: Optional[ShellType] = None) -> str:
        """Get the script to deactivate the current environment"""
        
    def get_cmd(
        self,
        cmd: str,
        env_name: Optional[str] = None,
        channel: Optional[UpdateChannel] = None,
        snap_id: Optional[SnapId] = None,
        skip_py_env: bool = False,
        disable: Optional[List[str]] = None,
        enable: Optional[List[str]] = None,
    ):
        """Get a sh.Command pointing to a command from inside an env / app"""
        if env_name is None:
            env_name = os.environ.get("BYOE_DEFAULT_ENV_NAME", "main")
        snap_id = self.get_snap(channel, snap_id)
        snap_conf = yaml.safe_load(
            (self._locs["envs"] / f"{snap_id}-site_conf.yaml").read_text()
        )
        env_snaps = self.get_env_snaps(snap_id, snap_conf, env_name)
        if any(not s.supports_activation for s in env_snaps):
            assert len(env_snaps) == 1
            snap = env_snaps[0]
            if enable:
                log.warning("Can't enable apps with env type '{snap.env_type}'")
            log.info(f"Skipping apps as '{snap.env_type}' doesn't support layering")
            enable = []
            return getattr(self.get_apptainer().exec, cmd)
        snap_apps = snap_conf.get("apps", {})
        default_apps = [k for k, v in snap_apps.items() if v["default"] and k != env_name]
        if enable is None:
            enable = default_apps
        else:
            enable += [x for x in default_apps if x not in enable and x != env_name]
        if disable is not None:
            enable = [x for x in enable if x not in disable]
        

    def _allocate_snap_id(
        self, time_stamp: Optional[datetime] = None, label: Optional[str] = None
    ) -> Tuple[SnapId, Path]:
        """Allocate next unique SnapId in sequence"""
        if time_stamp is None:
            time_stamp = datetime.now()
        min_vers = 0
        for snap_conf in self._locs["envs"].glob(
            f"{time_stamp.strftime(TS_FORMAT)}*-site_conf.yaml"
        ):
            try:
                snap_id = SnapId.from_prefix(snap_conf.stem)
            except InvalidSnapIdError:
                log.warning("Skipping potential snap config: %s", snap_conf)
                continue
            if snap_id.label != label:
                continue
            log.debug("Found existing snap: %s", snap_id)
            if min_vers <= snap_id.version:
                min_vers = snap_id.version + 1
        for in_prog in self._locs["envs"].glob(
            f".in-progress-{time_stamp.strftime(TS_FORMAT)}*"
        ):
            try:
                snap_id = SnapId.from_prefix(in_prog.name.split("-")[-1])
            except InvalidSnapIdError:
                continue
            if snap_id.label != label:
                continue
            if min_vers <= snap_id.version:
                min_vers = snap_id.version + 1
        n_tries = 0
        while n_tries < 3:
            snap_id = SnapId(time_stamp, min_vers, label=label)
            log.debug("Trying to allocate snap_id: %s", snap_id)
            # TODO: Use flufl.lock here?
            reserve_path = self._locs["envs"] / f".in-progress-{snap_id}"
            try:
                reserve_fd = os.open(
                    str(reserve_path), os.O_CREAT | os.O_EXCL | os.O_RDWR
                )
            except:
                log.warning("Racing while trying to alloc SnapID...")
                min_vers += 1
                n_tries += 1
            else:
                return (snap_id, reserve_path)
        raise ValueError("Unable to allocate unique SnapId")
        
