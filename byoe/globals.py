import re
from enum import Enum
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import ClassVar, Dict, Any, Optional

import yaml


class SnapType(Enum):
    ENV = "env"
    APP = "app"


class EnvType(Enum):
    SPACK = "spack"
    PYTHON = "python"
    CONDA = "conda"


LOCK_SUFFIXES = {
    EnvType.SPACK: ".lock",
    EnvType.PYTHON: "-requirements.txt",
    EnvType.CONDA: "-lock.yml",
}


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


# TODO: Need some way to track "test" snaps that would have to be explicitly activated
@dataclass(frozen=True)
class SnapId:
    """Uniquely identify a snaphot"""

    time_stamp: datetime

    version: int = 0

    REGEX: str = r"([0-9]+)(\.[0-9]+)?"

    def __repr__(self) -> str:
        if self.version == 0:
            return f"{self.time_stamp.strftime(TS_FORMAT)}"
        return f"{self.time_stamp.strftime(TS_FORMAT)}.{self.version}"

    @classmethod
    def from_str(cls, val: str) -> "SnapId":
        toks = val.split(".")
        ts = datetime.strptime(toks[0], TS_FORMAT)
        vers = 0 if len(toks) == 1 else int(toks[1])
        return cls(ts, vers)

    @classmethod
    def from_prefix(cls, val: str) -> Optional["SnapId"]:
        mtch = re.search(fr"^{cls.REGEX}", val)
        if not mtch:
            return None
        return cls.from_str(mtch.group())

    def __lt__(self, other):
        return (self.time_stamp, self.version) < (other.time_stamp, other.version)


@dataclass(frozen=True)
class SnapSpec:
    """Capture info about a environment / app snapshot"""

    snap_id: SnapId

    env_type: EnvType

    name: str

    snap_dir: Path

    snap_type: SnapType

    @property
    def snap_name(self) -> str:
        return f"{self.name}@{self.snap_id}"

    @property
    def lock_file(self) -> Path:
        return (
            self.snap_dir.parent / f"{self.snap_dir.name}{LOCK_SUFFIXES[self.env_type]}"
        )

    def get_activate_path(self, shell: ShellType = ShellType.SH) -> Path:
        """Get path to the activation script"""
        if self.snap_type == SnapType.APP or self.env_type != EnvType.PYTHON:
            return self.snap_dir.parent / f"{self.snap_dir.name}_activate.{shell.value}"
        elif self.env_type == EnvType.PYTHON:
            if shell == ShellType.SH:
                suffix = ""
            else:
                suffix = f".{shell.value}"
            return self.snap_dir / "bin" / f"activate{suffix}"

    def get_lock_data(self) -> Dict[str, Any]:
        """Get the data from the `lock_file`"""
        txt_data = self.lock_file.read_text()
        if self.env_type in (EnvType.SPACK, EnvType.CONDA):
            return yaml.load(txt_data)
        else:
            return [l for l in txt_data.split("\n") if not l.strip().startswith("#")]
    
    @classmethod
    def from_lock_path(cls, lock_path: Path) -> "SnapSpec":
        """Generate a SnapSpec from the path to its lock file"""
        snap_id = SnapId.from_prefix(lock_path.stem)
        name = lock_path.parent.name
        env_type = EnvType(lock_path.parent.parent.name)
        snap_type = SnapType.ENV
        if lock_path.parent.parent.parent.name == "apps":
            snap_type = SnapType.APP
        return cls(snap_id, env_type, name, lock_path.parent / str(snap_id), snap_type)

    def __lt__(self, other: "SnapSpec"):
        return self.snap_name < other.snap_name
