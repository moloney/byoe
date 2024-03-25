import os, logging, typing
from pathlib import Path
from enum import Enum
from copy import deepcopy
from dataclasses import dataclass, field, fields
from urllib.parse import urlparse
from urllib.request import urlopen
from typing import ClassVar, Dict, List, Optional, Union, Any

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


def _get_conf_content(base_dir: Path, source: str) -> str:
    """Get text from config file that could be local path or URL"""
    url = urlparse(source)
    if url.scheme in ("", "file"):
        url_path = Path(url.path)
        if not url_path.absolute:
            url_path = base_dir / url_path
        return url_path.read_text()
    else:
        return urlopen(source).read()


def _update_nested(base_dict, key, value):
    if key not in base_dict:
        base_dict[key] = value
        return
    def_val = base_dict.get(key)
    if isinstance(value, list) and isinstance(def_val, list):
        base_dict[key] += value
    elif isinstance(value, dict) and isinstance(def_val, dict):
        for k, v in value.items():
            _update_nested(def_val, k, v)
    else:
        base_dict[key] = value


@dataclass
class Config:
    """Base for specifying config as dataclass"""

    def to_dict(self) -> Dict[str, Any]:
        res = {}
        for field in fields(self):
            attr = field.name
            val = getattr(self, attr)
            if val is None:
                continue
            if isinstance(val, Path):
                res[attr] = str(val)
            elif isinstance(val, Enum):
                res[attr] = val.value
            elif isinstance(val, Config):
                res[attr] = val.to_dict()
            else:
                res[attr] = val
        return res

    def set_defaults(self, def_config: "Config") -> None:
        for field in fields(def_config):
            new_def_val = getattr(def_config, field.name)
            if field.name not in self._explicitly_set:
                setattr(self, field.name, new_def_val)
            elif new_def_val != field.default:
                prev_val = getattr(self, field.name)
                if isinstance(new_def_val, list):
                    if prev_val is None:
                        prev_val = []
                    setattr(self, prev_val + new_def_val)
                elif isinstance(new_def_val, dict):
                    if prev_val is None:
                        prev_val = {}
                    res = new_def_val.copy()
                    for k, v in prev_val.items():
                        _update_nested(res, k, v)
                    setattr(self, field.name, res)

    @classmethod
    def get_defaults(cls):
        """Get the default values for the dataclass"""
        return {f.name: f.default for f in fields(cls)}

    @classmethod
    def from_dict(cls, conf_data: Dict[str, Any]):
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
                if issubclass(tgt_class, Config):
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
class UserConfig(Config):
    """User specific configuration"""

    base_dir: Path = "~/.byoe_repo"

    channel: UpdateChannel = UpdateChannel.STABLE

    default_env: str = "main"

    default_gpu_env: Optional[str] = None

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
    def filt_include(cls, include_data):
        """Subclasses can override this method to modify / filter included data"""
        return include_data

    # TODO: Need support for recursive includes with loop detection here
    @classmethod
    def from_dict(cls, conf_data: Dict[str, Any]):
        # Handle top level includes
        includes = conf_data.get("include")
        if includes:
            include_data = {}
            for include in includes:
                include_data.update(
                    cls.filt_include(
                        yaml.safe_load(_get_conf_content(cls.base_dir, include))
                    )
                )
            include_data.update(conf_data)
            conf_data = include_data
        for attr, hint in typing.get_type_hints(cls).items():
            if attr in conf_data:
                tgt_class = None
                if hasattr(hint, "__origin__") and hint.__origin__ is typing.Union:
                    for sub_hint in typing.get_args(hint):
                        if not sub_hint is type(None):
                            tgt_class = sub_hint
                            break
                else:
                    tgt_class = hint
                if hasattr(tgt_class, "__origin__"):
                    # We still have type hint not a class, just punt...
                    continue
                if issubclass(tgt_class, IncludableConfig):
                    conf_data[attr] = tgt_class.from_dict(conf_data[attr])
                elif issubclass(tgt_class, Config):
                    conf_data[attr] = tgt_class.from_dict(conf_data[attr])
                elif issubclass(tgt_class, Enum):
                    conf_data[attr] = tgt_class(conf_data[attr].lower())
                elif not isinstance(conf_data[attr], tgt_class):
                    conf_data[attr] = tgt_class(conf_data[attr])
        res = cls(**conf_data)
        res._explicitly_set = set(conf_data.keys())
        return res


@dataclass
class SpackBuildChain(Config):
    """Spack build-chain specification"""

    compiler: Optional[str] = None

    binutils: Optional[str] = None


@dataclass
class SpackConfig(IncludableConfig):
    """Spack specific configuration for an environment"""

    build_chains: Optional[List[SpackBuildChain]] = None

    externals: Optional[List[str]] = None

    config: Optional[Dict[str, Any]] = None

    specs: Optional[List[str]] = None


@dataclass
class PythonConfig(IncludableConfig):
    """Python specific configuration for an environment"""

    specs: List[str]

    system_packages: bool = True


@dataclass
class CondaConfig(IncludableConfig):
    """Conda specific configuration for an environment"""

    channels: List[str]

    specs: List[str]

    @classmethod
    def filt_include(cls, include_data):
        for key in include_data:
            if key not in ("channels", "specs", "dependencies"):
                del include_data["key"]
        if "dependencies" in include_data:
            include_data["specs"] = include_data["dependencies"]
            del include_data["dependencies"]
        return include_data


@dataclass
class EnvConfig(IncludableConfig):
    """Config for an environment"""

    spack: Optional[SpackConfig] = None

    python: Optional[PythonConfig] = None

    conda: Optional[CondaConfig] = None

    def __post_init__(self):
        if self.spack is not None and self.conda is not None:
            raise InvalidConfigError("Can't mix spack / conda in same environment")

    def set_defaults(
        self, defaults: Dict[str, Union[SpackConfig, PythonConfig, CondaConfig]]
    ) -> None:
        for attr in ("spack", "python", "conda"):
            if getattr(self, attr) and attr in defaults:
                getattr(self, attr).set_defaults(defaults[attr])


@dataclass
class CondaAppConfig(IncludableConfig):
    """Config for isolated Conda app"""

    conda: CondaConfig

    exported: Optional[Dict[str, str]] = None

    default: bool = True

    def set_defaults(
        self, defaults: Dict[str, Union[SpackConfig, PythonConfig, CondaConfig]]
    ) -> None:
        if "conda" in defaults:
            self.conda.set_defaults[defaults["conda"]]


@dataclass
class PythonAppConfig(IncludableConfig):
    """Config for isolated Python app"""

    python: PythonConfig

    python_spec: Optional[str] = None

    spack: Optional[SpackConfig] = None

    default: bool = True

    def set_defaults(
        self, defaults: Dict[str, Union[SpackConfig, PythonConfig, CondaConfig]]
    ) -> None:
        for attr in ("spack", "python"):
            if getattr(self, attr) and attr in defaults:
                getattr(self, attr).set_defaults[defaults[attr]]


@dataclass
class SlurmBuildConfig(Config):
    """Config for building on Slurm"""

    enabled: bool = True

    tasks_per_job: int = 12

    max_jobs: int = 1

    srun_args: str = ""

    tmp_dir: Optional[Path] = None


@dataclass
class BuildConfig(Config):
    """Config for building environments / apps"""

    # TODO: rename to max_local_tasks
    max_tasks: int = os.cpu_count() // 2

    tmp_dir: Optional[Path] = None

    slurm_config: Optional[Dict[str, SlurmBuildConfig]] = None

    def to_dict(self):
        res = {}
        for field in fields(self):
            val = getattr(self, field.name)
            if val is None:
                continue
            if field.name == "slurm_config":
                res[field.name] = {k: v.to_dict() for k, v in val.items()}
            else:
                res[field.name] = val.to_dict()
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
        slurm_info.set_defaults(slurm_conf.get("default", SlurmBuildConfig()))
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


@dataclass
class GlobalSpackConfig(Config):
    """Spack config that is handled globally (not per env/app)"""

    repo_url: str = "https://github.com/spack/spack.git"

    repo_branch: str = "develop"

    buildcache_padding: int = 128

    mirrors: Optional[Dict[str, str]] = None


@dataclass
class GlobalCondaConfig(Config):
    """Conda config that is handled globally"""

    source: str = "https://micro.mamba.pm/api/micromamba"

    build_chain: Optional[SpackBuildChain] = None

    def __post_init__(self):
        if self.source != "spack" and self.build_chain is not None:
            raise InvalidConfigError(
                "Specifying 'build_chain' only valid if 'source' is 'spack'"
            )


@dataclass
class SiteConfig(Config):
    """Full configuration for a site"""

    spack_global: GlobalSpackConfig = field(default_factory=GlobalSpackConfig)

    conda_global: GlobalCondaConfig = field(default_factory=GlobalCondaConfig)

    build_opts: BuildConfig = field(default_factory=BuildConfig)

    defaults: Optional[Dict[str, Union[SpackConfig, PythonConfig, CondaConfig]]] = None

    apps: Optional[Dict[str, Union[CondaAppConfig, PythonAppConfig]]] = None

    envs: Optional[Dict[str, EnvConfig]] = None

    def __post_init__(self):
        app_names = set() if self.apps is None else set(self.apps.keys())
        env_names = set() if self.envs is None else set(self.envs.keys())
        collisions = app_names & env_names
        if collisions:
            raise InvalidConfigError(
                f"Environments and apps can't share names: {','.join(collisions)}"
            )

    def to_dict(self):
        res = {}
        for field in fields(self):
            val = getattr(self, field.name)
            if val is None:
                continue
            if field.name in ("defaults", "apps", "envs"):
                res[field.name] = {k: v.to_dict() for k, v in val.items()}
            else:
                res[field.name] = val.to_dict()
        return res

    @classmethod
    def from_dict(cls, conf_data: Dict[str, Any]):
        spack_global = conf_data.get("spack_global")
        if spack_global:
            conf_data["spack_global"] = GlobalSpackConfig.from_dict(spack_global)
        conda_global = conf_data.get("conda_global")
        if conda_global:
            conf_data["conda_global"] = GlobalCondaConfig.from_dict(conda_global)
        build_opts = conf_data.get("build_opts")
        if build_opts:
            conf_data["build_opts"] = BuildConfig.from_dict(build_opts)
        defaults = conf_data.get("defaults")
        if defaults:
            conf_data["defaults"] = {}
            for env_type, env_defaults in defaults.items():
                if env_type == "spack":
                    def_conf = SpackConfig.from_dict(env_defaults)
                elif env_type == "python":
                    def_conf = PythonConfig.from_dict(env_defaults)
                elif env_type == "conda":
                    def_conf = CondaConfig.from_dict(env_defaults)
                else:
                    raise InvalidConfigError(
                        f"Invalid config type under 'defaults': {env_type}"
                    )
                conf_data["defaults"][env_type] = def_conf
        apps = conf_data.get("apps")
        if apps:
            converted = {}
            for name, app_conf in apps.items():
                if "conda" in app_conf:
                    converted[name] = CondaAppConfig.from_dict(app_conf)
                else:
                    converted[name] = PythonAppConfig.from_dict(app_conf)
            conf_data["apps"] = converted
        envs = conf_data.get("envs")
        if envs:
            conf_data["envs"] = {
                name: EnvConfig.from_dict(env_conf) for name, env_conf in envs.items()
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
        return cls(GlobalSpackConfig(spack_repo, spack_branch))
