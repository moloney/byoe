from enum import Enum


class SnapType(Enum):
    ENV = "env"
    APP = "app"


class EnvType(Enum):
    SPACK = "spack"
    PYTHON = "python"
    CONDA = "conda"
    APPTAINER = "apptainer"


LOCK_SUFFIXES = {
    EnvType.SPACK: ".lock",
    EnvType.PYTHON: "-requirements.txt",
    EnvType.CONDA: "-lock.yml",
    EnvType.APPTAINER: ".def",
}


ENV_SUFFIXES = {EnvType.APPTAINER: ".sif"}


TS_FORMAT = "%Y%m"


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
