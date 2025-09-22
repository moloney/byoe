"""Configuration specification and parsing"""
import os, logging, typing, re, itertools
from pathlib import Path
from enum import Enum
from copy import deepcopy
from dataclasses import asdict, dataclass, field, fields
from urllib.parse import urlparse
from urllib.request import urlopen
from typing import Protocol, ClassVar, Dict, List, Optional, Union, Any, Type

import yaml
import click

from .globals import UpdateChannel, CHANNEL_UPDATE_MONTHS
from .util import HAS_SLURM


log = logging.getLogger(__name__)


DEFAULT_SLURM_TASKS = 16


class ConfigError(Exception):
    pass


class MissingConfigError(ConfigError):
    pass


class InvalidConfigError(ConfigError):
    pass


class IncludeLoopError(InvalidConfigError):
    """Raise when a loop is detected in configuration 'include' statements"""


def _get_conf_content(base_dir: Path, source: str) -> str:
    """Get text from config file that could be local path or URL"""
    url = urlparse(source)
    if url.scheme in ("", "file"):
        url_path = Path(url.path)
        if not url_path.is_absolute():
            url_path = base_dir / "conf.d" / url_path
        return url_path.read_text()
    else:
        return urlopen(source).read()


def _update_conf(base_dict, key, value):
    """Update a config value, merging with existing value if it is a list / dict"""
    if key not in base_dict:
        base_dict[key] = value
        return
    def_val = base_dict.get(key)
    if isinstance(value, list) and isinstance(def_val, list):
        base_dict[key] += value
    elif isinstance(value, dict) and isinstance(def_val, dict):
        for k, v in value.items():
            _update_conf(def_val, k, v)
    else:
        base_dict[key] = value


def _dc_from_conf(cls, conf_data):
    '''Build a dataclass from configuration dict'''
    for attr, hint in typing.get_type_hints(cls).items():
        if attr in conf_data:
            tgt_class = None
            if hasattr(hint, "__origin__"):
                if hint.__origin__ is typing.Union:
                    for sub_hint in typing.get_args(hint):
                        if not sub_hint is type(None):
                            if hasattr(sub_hint, "__origin__"):
                                tgt_class = sub_hint.__origin__
                            else:
                                tgt_class = sub_hint
                            break
                else:
                    tgt_class = hint.__origin__
            else:
                tgt_class = hint
            if hasattr(tgt_class, "from_dict"):
                conf_data[attr] = tgt_class.from_dict(conf_data[attr])
            elif issubclass(tgt_class, Enum):
                conf_data[attr] = tgt_class(conf_data[attr].lower())
            elif conf_data[attr] is not None and not isinstance(
                conf_data[attr], tgt_class
            ):
                conf_data[attr] = tgt_class(conf_data[attr])
    res = cls(**conf_data)
    res._explicitly_set = set(conf_data.keys())
    return res


@dataclass
class Config(Protocol):
    """Base for specifying config as dataclass"""

    def to_dict(self) -> Dict[str, Any]:
        res: Dict[str, Any] = {}
        for field in fields(self):
            attr = field.name
            val = getattr(self, attr)
            if val is None:
                continue
            if isinstance(val, Path):
                res[attr] = str(val)
            elif isinstance(val, Enum):
                res[attr] = val.value
            elif hasattr(val, "to_dict"):
                res[attr] = val.to_dict()
            else:
                res[attr] = val
        return res

    def set_defaults(self, def_config: Dict[str, Any]) -> None:
        for field in fields(self):
            if field.name not in def_config:
                continue
            new_def_val = def_config[field.name]
            if (
                not hasattr(self, "_explicitly_set")
                or field.name not in self._explicitly_set
            ):
                setattr(self, field.name, new_def_val)
            elif new_def_val != field.default:
                prev_val = getattr(self, field.name)
                if isinstance(new_def_val, list):
                    if prev_val is None:
                        prev_val = []
                    setattr(self, field.name, prev_val + new_def_val)
                elif isinstance(new_def_val, dict):
                    if prev_val is None:
                        prev_val = {}
                    res = new_def_val.copy()
                    for k, v in prev_val.items():
                        _update_conf(res, k, v)
                    setattr(self, field.name, res)

    @classmethod
    def get_defaults(cls):
        """Get the default values for the dataclass"""
        return {f.name: f.default for f in fields(cls)}

    @classmethod
    def from_dict(cls, conf_data: Dict[str, Any]):
        return _dc_from_conf(cls, conf_data)


@dataclass
class UserConfig(Config):
    """User specific configuration"""

    base_dir: Path = Path("~/.byoe_repo").expanduser()

    channel: UpdateChannel = UpdateChannel.STABLE

    default_env: str = "main"

    @classmethod
    def build_interactive(cls):
        defaults = cls.get_defaults()
        base_dir = click.prompt(
            "Enter the path to the base directory for the repository",
            default=defaults["base_dir"],
            type=Path,
        ).expanduser()
        update_freq = click.prompt(
            "Choose your default environment update frequency in months",
            type=click.Choice(CHANNEL_UPDATE_MONTHS.values()),
            default=CHANNEL_UPDATE_MONTHS[defaults["channel"]],
        )
        def_channel = None
        for chan, freq in CHANNEL_UPDATE_MONTHS.items():
            if freq == update_freq:
                def_channel = chan
        return cls(base_dir, def_channel)


def get_user_conf(conf_path: Path) -> UserConfig:
    """Load the user configuration"""
    if not conf_path.exists():
        raise MissingConfigError(f"No such file: {conf_path}")
    try:
        in_f = conf_path.open("rt")
    except:
        raise InvalidConfigError(f"Unable to open config file: {conf_path}")
    try:
        user_conf = UserConfig.from_dict(yaml.safe_load(in_f))
    except:
        raise InvalidConfigError(f"Error reading config file: {conf_path}")
    return user_conf


@dataclass
class IncludableConfig(Config):
    """Base class for config that can have include statements

    The `base_dir` must be set on this class before using any subclasses"""

    base_dir: ClassVar[Optional[Path]] = None

    @classmethod
    def filt_include(cls, include_data: Dict[str, Any]) -> Dict[str, Any]:
        """Subclasses can override this method to modify / filter included data"""
        return include_data

    @classmethod
    def merge_values(cls, key, base_val, top_val):
        """Subclasses can override this to modify how values are merged"""
        if isinstance(base_val, (list, tuple)):
            return base_val + top_val
        if isinstance(base_val, dict):
            res = base_val.copy()
            res = {
                k : tv if k not in res else cls.merge_values(k, res[k], tv) 
                for k, tv in top_val.items()
            }
            return res
        else:
            return top_val

    @classmethod
    def resolve_includes(cls, conf_data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve any include elements in config data"""
        seen = set()
        def get_incl(data):
            includes = data.get("include")
            if includes is None:
                return data
            res = {}
            for include in includes:
                if include in seen:
                    raise IncludeLoopError(f"Include loop detected on: {include}")
                seen.add(include)
                res.update(
                    get_incl(yaml.safe_load(_get_conf_content(cls.base_dir, include)))
                )
            res = cls.filt_include(res)
            for key, val in conf_data.items():
                if key == "include":
                    continue
                if key not in res:
                    res[key] = val
                else:
                    res[key] = cls.merge_values(key, res[key], val)
            return res
        return get_incl(conf_data)

    @classmethod
    def from_dict(cls, conf_data: Dict[str, Any]):
        conf_data = cls.resolve_includes(conf_data)
        return _dc_from_conf(cls, conf_data)


_SPACK_MERGE_STR_SET = frozenset(("variants", "require"))


@dataclass
class SpackConfig(IncludableConfig):
    """Spack specific configuration for an environment"""

    externals: Optional[List[str]] = None

    config: Optional[Dict[str, Any]] = None

    specs: Optional[List[str]] = None

    def to_dict(self):
        res = {}
        for field in fields(self):
            val = getattr(self, field.name)
            if val is None:
                continue
            if hasattr(val, "to_dict"):
                res[field.name] = val.to_dict()
            else:
                res[field.name] = val
        return res
    
    @classmethod
    def merge_values(cls, key, base_val, top_val):
        """Specialized `merge_values` concats 'variants' and 'require' strings"""
        if key in _SPACK_MERGE_STR_SET and isinstance(base_val, str):
            return " ".join((base_val, top_val))
        return IncludableConfig.merge_values(key, base_val, top_val)

    @classmethod
    def from_dict(cls, conf_data: Dict[str, Any]):
        conf_data = cls.resolve_includes(conf_data)
        res = cls(**conf_data)
        res._explicitly_set = set(conf_data.keys())
        return res

    def to_spack_conf(self) -> Dict:
        """Create spack config data, suitable for writing to a spack.yaml file"""
        res = deepcopy(self.config) if self.config else {}
        if self.specs:
            res["specs"] = self.specs[:]
        return res


@dataclass
class PythonConfig(IncludableConfig):
    """Python specific configuration for an environment"""

    specs: List[str]

    system_packages: bool = True

    #TODO: Maybe enable by default after this: https://github.com/pypa/pip/pull/11968
    generate_hashes: bool = False


@dataclass
class CondaConfig(IncludableConfig):
    """Conda specific configuration for an environment"""

    channels: List[str] = field(default_factory=list)

    virtual: Dict[str, str] = field(default_factory=dict)

    specs: List[str] = field(default_factory=list)

    @classmethod
    def filt_include(cls, include_data):
        needs_del = [
            k for k in include_data if k not in ("channels", "specs", "dependencies", "virtual")
        ]
        for key in needs_del:
            del include_data[key]
        if "dependencies" in include_data:
            include_data["specs"] = include_data["dependencies"]
            del include_data["dependencies"]
        return include_data


@dataclass
class ApptainerConfig(IncludableConfig):
    """Apptainer specific configuration for an image"""

    image_spec: str

    inject_nv: bool = False

    inject_rocm: bool = False


@dataclass
class EnvConfig(IncludableConfig):
    """Config for an environment"""

    spack: Optional[SpackConfig] = None

    python: Optional[PythonConfig] = None

    conda: Optional[CondaConfig] = None

    extra_activation: Optional[List[str]] = None

    best_effort: bool = False

    def __post_init__(self):
        if self.spack is not None and self.conda is not None:
            raise InvalidConfigError("Can't mix spack / conda in same environment")

    def set_defaults(self, defaults: Dict[str, Dict[str, Any]]) -> None:
        for attr in ("spack", "python", "conda"):
            if getattr(self, attr) and attr in defaults:
                getattr(self, attr).set_defaults(defaults[attr])


@dataclass
class _AppConfig(IncludableConfig):
    """Abstract base class for any application configurations"""
    default: bool = True

    extra_activation: Optional[List[str]] = None


@dataclass
class _CondaAppMixin:
    conda: CondaConfig

    exported: Optional[Dict[str, Dict[str, str]]] = None

    exec_prelude: Optional[Dict[str, List[str]]] = None 


@dataclass
class CondaAppConfig(_AppConfig, _CondaAppMixin):
    """Config for isolated Conda app"""

    def set_defaults(self, defaults: Dict[str, Dict[str, Any]]) -> None:
        if "conda" in defaults:
            self.conda.set_defaults(defaults["conda"])


@dataclass
class _PythonAppMixin:
    python: PythonConfig

    python_spec: Optional[str] = None

    spack: Optional[SpackConfig] = None


@dataclass
class PythonAppConfig(_AppConfig, _PythonAppMixin):
    """Config for isolated Python app"""

    def set_defaults(self, defaults: Dict[str, Dict[str, Any]]) -> None:
        for attr in ("spack", "python"):
            if getattr(self, attr) and attr in defaults:
                getattr(self, attr).set_defaults(defaults[attr])


@dataclass
class _ApptainerAppMixin:
    """Config for an isolated Apptainer app"""
    apptainer: ApptainerConfig
    
    exported: Optional[List[str]] = None


@dataclass
class ApptainerAppConfig(_AppConfig, _ApptainerAppMixin):
    """Config for an isolated Apptainer app"""
   
    def set_defaults(self, defaults: Dict[str, Dict[str, Any]]) -> None:
        if "apptainer" in defaults:
            self.apptainer.set_defaults(defaults["apptainer"])


def get_app_conf(conf_data: Dict) -> Union[CondaAppConfig, PythonAppConfig, ApptainerAppConfig]:
    app_cls: Union[Type[PythonAppConfig], Type[CondaAppConfig], Type[ApptainerAppConfig]]
    if "python" in conf_data:
        app_cls = PythonAppConfig
    elif "conda" in conf_data:
        app_cls = CondaAppConfig
    elif "apptainer" in conf_data:
        app_cls = ApptainerAppConfig
    else:
        raise InvalidConfigError("Configuration isn't valid for an 'app'")
    return app_cls.from_dict(conf_data)


@dataclass
class SlurmBuildConfig(Config):
    """Config for building on Slurm"""

    enabled: bool = True

    tasks_per_job: int = 12

    max_jobs: int = 1

    srun_args: str = ""

    tmp_dir: Optional[Path] = None


def _get_n_cpus(na_default: int = 2) -> int:
    res = os.cpu_count()
    if res is None:
        res = na_default
    return res


@dataclass
class BuildConfig(Config):
    """Config for building environments / apps"""

    # TODO: rename to max_local_tasks
    max_tasks: int = _get_n_cpus() // 2

    tmp_dir: Optional[Path] = None

    slurm_config: Optional[Dict[str, SlurmBuildConfig]] = None

    def to_dict(self):
        res = {}
        for field in fields(self):
            val = getattr(self, field.name)
            if val is None:
                continue
            if field.name == "tmp_dir":
                res[field.name] = str(val)
            if field.name == "slurm_config":
                res[field.name] = {k: v.to_dict() for k, v in val.items()}
            else:
                res[field.name] = val
        return res

    @classmethod
    def from_dict(cls, conf_data: Dict[str, Any]):
        tmp_dir = conf_data.get("tmp_dir")
        if tmp_dir:
            conf_data["tmp_dir"] = Path(tmp_dir)
        slurm_conf = conf_data.get("slurm_config")
        if slurm_conf:
            conf_data["slurm_config"] = {
                name: SlurmBuildConfig.from_dict(sub_conf)
                for name, sub_conf in slurm_conf.items()
            }
        res = cls(**conf_data)
        res._explicitly_set = set(conf_data.keys())
        return res


def get_job_build_info(build_config: Optional[BuildConfig], job_type: str):
    if build_config is None:
        build_config = BuildConfig()
    if HAS_SLURM:
        if build_config.slurm_config is None:
            slurm_conf = {}
        else:
            slurm_conf = build_config.slurm_config
        slurm_info = deepcopy(slurm_conf.get(job_type, SlurmBuildConfig()))
        slurm_info.set_defaults(asdict(slurm_conf.get("default", SlurmBuildConfig())))
        if slurm_info.tasks_per_job is None:
            slurm_info.tasks_per_job = DEFAULT_SLURM_TASKS
        if slurm_info.tmp_dir is None:
            slurm_info.tmp_dir = build_config.tmp_dir
        if slurm_info.enabled:
            return {
                "use_slurm": True,
                "n_tasks": slurm_info.tasks_per_job,
                "tmp_dir": slurm_info.tmp_dir,
                "srun_args": slurm_info.srun_args,
            }
    return {
        "use_slurm": False,
        "n_tasks": build_config.max_tasks,
        "tmp_dir": build_config.tmp_dir,
    }




def _mk_def_tc_conf():
    return SpackConfig(
        config = {
            "packages" : {"gcc": {"require": "+binutils"}, "binutils": {"require": "+gas"}}
        }
    )


@dataclass
class SpackToolchainConfig(Config):
    """Configure spack build toolchain that is itself built with Spack
    
    Attributes
    ----------
    components : The spack configuration data for the toolchain

    spack : Spack config to use when building the toolchain
    """
    components: List[Dict[str, str]]

    spack: SpackConfig = field(default_factory=_mk_def_tc_conf)

    def get_internal_packages(self) -> List[str]:
        """Get list of non-external packages referenced by this toolchain"""
        externals = set()
        if self.spack.externals:
            externals = set(x for x in self.spack.externals)
        pkg_specs = []
        for comp in self.components:
            req_toks = comp.get("spec", "").split("=")
            if len(req_toks) < 2:
                continue
            spec = "=".join(req_toks[1:])
            if spec not in externals and spec not in pkg_specs:
                pkg_specs.append(spec)
        return pkg_specs


@dataclass
class GlobalSpackConfig(Config):
    """Spack config that is handled globally
    
    The `repo_url` and `repo_branch` define the source for Spack itself.
    """

    repo_url: str = "https://github.com/spack/spack.git"

    repo_branch: str = "develop"

    pkg_repo_url: str = "https://github.com/spack/spack-packages.git"

    pkg_repo_branch: str = "develop"

    toolchains: Dict[str, SpackToolchainConfig] = field(default_factory=dict)

    arch: Optional[str] = None

    mirrors: Optional[Dict[str, Dict[str, Any]]] = None

    buildcache_padding: int = 128

    connect_timeout: int = 60

    @classmethod
    def from_dict(cls, conf_data: Dict[str, Any]):
        conf_data["toolchains"] = {
            n: SpackToolchainConfig.from_dict(tc) 
            for n, tc in conf_data["toolchains"].items()
        } 
        res = cls(**conf_data)
        res._explicitly_set = set(conf_data.keys())
        return res

    def get_spack_conf_data(self, conf_type: str) -> Optional[Dict]:
        if conf_type == "toolchains":
            if not self.toolchains:
                return None
            return {name: tc.components for name, tc in self.toolchains.items()}
        elif conf_type == "config":
            return {
                "connect_timeout": self.connect_timeout,
                "install_tree": {"padded_length": self.buildcache_padding},
            }
        elif conf_type == "packages":
            data = None
            if self.arch:
                data = {"all" : {"require": f"target={self.arch}"}}
            return data
    
    def write_global_spack_conf(self, conf_dir: Path):
        for conf_type in ("toolchains", "config", "packages"):
            conf_data = self.get_spack_conf_data(conf_type)
            if conf_data is None:
                continue
            conf_path = conf_dir / f"{conf_type}.yaml"
            conf_path.write_text(yaml.safe_dump({conf_type: conf_data}))



@dataclass
class GlobalCondaConfig(Config):
    """Conda config that is handled globally
    
    The `source` can be:
      * The base URL to download prebuilt binaries from
      * The keyword 'spack' to build with Spack
    """

    source: str = "https://micro.mamba.pm/api/micromamba"



@dataclass
class GlobalApptainerConfig(Config):
    """Apptainer config that is handled globally
    
    The `source` can be:
      * The keyword 'system' to use the system installed version
      * The keyword 'relocatable' to install the relocatable (non-suid) binaries
      * None to prefer system install but fall back to installing relocatable version
    """
    source: Optional[str] = None

    reloc_install_script: str = "https://raw.githubusercontent.com/apptainer/apptainer/main/tools/install-unprivileged.sh"


@dataclass
class WorkSpace(Config):
    """Explicitly define a named workspace"""
    
    env: Optional[str] = None

    exclude_apps: List[str] = field(default_factory=list)

    include_apps: List[str] = field(default_factory=list)

    best_effort: bool = False


VALID_ENV_APP_NAME = "[a-zA-Z0-9_]+"


@dataclass
class SiteConfig(Config):
    """Full configuration for a repository"""

    spack_global: GlobalSpackConfig = field(default_factory=GlobalSpackConfig)

    conda_global: GlobalCondaConfig = field(default_factory=GlobalCondaConfig)

    apptainer_global: GlobalApptainerConfig = field(default_factory=GlobalApptainerConfig)

    build_opts: BuildConfig = field(default_factory=BuildConfig)

    defaults: Optional[Dict[str, Dict[str, Any]]] = None

    apps: Dict[str, Union[CondaAppConfig, PythonAppConfig, ApptainerAppConfig]] = field(default_factory=dict)

    envs: Dict[str, EnvConfig] = field(default_factory=dict)

    workspaces: Dict[str, WorkSpace] = field(default_factory=dict)

    def __post_init__(self):
        app_names = set(self.apps.keys())
        env_names = set(self.envs.keys())
        ws_names = set(self.workspaces.keys())
        for name in app_names | env_names | ws_names:
            if not re.match(VALID_ENV_APP_NAME, name):
                raise InvalidConfigError(f"Invalid env/app/workspace name: {name}")
        collisions = app_names & env_names & ws_names
        if collisions:
            raise InvalidConfigError(
                f"Environments, apps, and workspaces can't share names: {','.join(collisions)}"
            )

    def to_dict(self):
        res = {}
        for field in fields(self):
            val = getattr(self, field.name)
            if val is None:
                continue
            if field.name in ("apps", "envs", "workspaces"):
                res[field.name] = {k: v.to_dict() for k, v in val.items()}
            elif field.name != "defaults":
                res[field.name] = val.to_dict()
            else:
                res[field.name] = val
        return res

    @classmethod
    def from_dict(cls, conf_data: Dict[str, Any]):
        spack_global = conf_data.get("spack_global")
        if spack_global:
            conf_data["spack_global"] = GlobalSpackConfig.from_dict(spack_global)
        conda_global = conf_data.get("conda_global")
        if conda_global:
            conf_data["conda_global"] = GlobalCondaConfig.from_dict(conda_global)
        apptainer_global = conf_data.get("apptainer_global")
        if apptainer_global:
            conf_data["apptainer_global"] = GlobalApptainerConfig.from_dict(apptainer_global)
        build_opts = conf_data.get("build_opts")
        if build_opts:
            conf_data["build_opts"] = BuildConfig.from_dict(build_opts)
        apps = conf_data.get("apps")
        if apps:
            conf_data["apps"] = {n : get_app_conf(d) for n, d in apps.items()}
        envs = conf_data.get("envs")
        if envs:
            conf_data["envs"] = {
                name: EnvConfig.from_dict(env_conf) for name, env_conf in envs.items()
            }
        workspaces = conf_data.get("workspaces")
        if workspaces:
            conf_data["workspaces"] = {
                name: WorkSpace.from_dict(ws_conf) for name, ws_conf in workspaces.items()
            }
        res = cls(**conf_data)
        res._explicitly_set = set(conf_data.keys())
        return res

    @classmethod
    def build_interactive(cls):
        spack_repo = click.prompt(
            "Enter the git repo URL for spack",
            default="https://github.com/spack/spack.git",
            type=str,
        )
        spack_branch = click.prompt(
            "Enter the git branch to use for spack", default="develop", type=str
        )
        pkg_repo = click.prompt(
            "Enter the git repo URL for spack-packages",
            default="https://github.com/spack/spack-packages.git",
            type=str,
        )
        pkg_branch = click.prompt(
            "Enter the git branch to use for spack-packages", default="develop", type=str
        )
        return cls(GlobalSpackConfig(spack_repo, spack_branch, pkg_repo, pkg_branch))


def get_site_conf(
    site_conf_path: Path, env_confd: Path, app_confd: Path
) -> SiteConfig:
    """Get the full site config, allowing envs / apps to be defined in conf.d dir
    
    Also sets defaults on all apps / envs 
    """
    # Load base config
    site_conf = SiteConfig.from_dict(yaml.safe_load(site_conf_path.read_text()))
    existing_names = set(site_conf.envs) & set(site_conf.apps)
    # Add any app / env config from the conf.d directory 
    for conf_path in itertools.chain(env_confd.glob("*.yaml"), app_confd.glob("*.yaml")):
        name = conf_path.stem
        if not re.match(VALID_ENV_APP_NAME, name):
            log.info("Skipping config due to invalid name: %s", name)
            continue
        if name in existing_names:
            raise InvalidConfigError(f"Name collision from conf.d: {name}")
        existing_names.add(name)
        conf_data = yaml.safe_load(conf_path.read_text())
        if conf_path.parent.name == "envs":
             site_conf.envs[name] = EnvConfig.from_dict(conf_data)
        else:
            site_conf.apps[name] = get_app_conf(conf_data)
    if site_conf.defaults is not None:
        if site_conf.envs:
            for env_conf in site_conf.envs.values():
                env_conf.set_defaults(site_conf.defaults)
        if site_conf.apps:
            for app_conf in site_conf.apps.values():
                app_conf.set_defaults(site_conf.defaults)
    return site_conf
