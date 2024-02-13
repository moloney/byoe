from enum import Enum
from pathlib import Path

import typer


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


CHANNEL_UPDATE_MONTHS = {
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
