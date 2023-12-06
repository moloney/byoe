from enum import Enum
from pathlib import Path

import typer


DEFAULT_SLURM_TASKS = 16


DEFAULT_CONF_PATHS = [
    Path("/etc/byoe.yaml"), 
    Path(typer.get_app_dir("byoe")) / "byoe.yaml", 
    Path("./byoe.yaml"),
]


class EnvType(Enum):
    SPACK = "spack"
    PYTHON = "python"
    CONDA = "conda"


LOCK_SUFFIXES = {
    EnvType.SPACK: ".lock",
    EnvType.PYTHON: "-requirements.txt",
    EnvType.CONDA: ".yml",
}


TS_FORMAT = "%Y%m%d%H%M%S"

class UpdateChannel(Enum):
    BLOODY = "bloody"
    FRESH = "fresh"
    STABLE = "stable"
    STALE = "stale"
    OLD = "old"


DEFAULT_UPDATE_MONTHS = {
    UpdateChannel.BLOODY: 1,
    UpdateChannel.FRESH: 3,
    UpdateChannel.STABLE: 6,
    UpdateChannel.STALE: 12,
    UpdateChannel.OLD: 24,
}

class ShellType(Enum):
    SH = "sh"
    CSH = "csh"
    FISH = "fish"

