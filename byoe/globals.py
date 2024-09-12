import os
import re, shutil, logging
from enum import Enum
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import ClassVar, Dict, Any, List, Optional

import yaml


log = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class SnapId:
    """Uniquely identify a snaphot"""

    time_stamp: datetime

    version: int = 0

    label: Optional[str] = None

    REGEX: str = r"^([0-9]+)(?:\.([A-Za-z]+)?([0-9]+))?$"

    def __repr__(self) -> str:
        if self.version == 0 and self.label is None:
            return f"{self.time_stamp.strftime(TS_FORMAT)}"
        elif self.label is None:
            return f"{self.time_stamp.strftime(TS_FORMAT)}.{self.version}"
        else:
            return f"{self.time_stamp.strftime(TS_FORMAT)}.{self.label}{self.version}"

    @classmethod
    def from_str(cls, val: str) -> "SnapId":
        ts, label, vers = re.match(cls.REGEX, val).groups()
        ts = datetime.strptime(ts, TS_FORMAT)
        vers = 0 if vers is None else int(vers)
        return cls(ts, vers, label)

    @classmethod
    def from_prefix(cls, val: str) -> Optional["SnapId"]:
        mtch = re.search(rf"^{cls.REGEX[:-1]}", val)
        if not mtch:
            return None
        return cls.from_str(mtch.group())

    def __lt__(self, other):
        self_lbl = self.label if self.label is not None else ''
        other_lbl = other.label if other.label is not None else ''
        return (self.time_stamp, self_lbl, self.version) < (other.time_stamp, other_lbl, other.version)


@dataclass(frozen=True)
class SnapSpec:
    """Capture info about a environment / app snapshot"""

    snap_id: SnapId

    env_type: EnvType

    name: str

    snap_dir: Path

    snap_type: SnapType

    def __lt__(self, other: "SnapSpec"):
        return (self.name, self.snap_id) < (other.name, other.snap_id)

    def __str__(self) -> str:
        return f"{self.env_type.name}/{self.snap_name}"

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
            return self.snap_dir.parent / f"{self.snap_dir.name}-activate.{shell.value}"
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

    def get_paths(self) -> List[Path]:
        """Get list of paths associated with the snap"""
        assoc_files = [self.snap_dir]
        for sh_type in ShellType:
            assoc_files.append(self.get_activate_path(sh_type))
        assoc_files.append(self.lock_file)
        if self.snap_type == SnapType.ENV:
            if self.env_type == EnvType.SPACK:
                assoc_files.append(self.snap_dir.parent / f"._{self.snap_id}")
                assoc_files.append(self.snap_dir.parent / f"{self.snap_id}-env")
            elif self.env_type == EnvType.PYTHON:
                assoc_files.append(self.snap_dir.parent / f"{self.snap_id}-main-req.in")
                assoc_files.append(self.snap_dir.parent / f"{self.snap_id}-sys-req.txt")
            elif self.env_type == EnvType.CONDA:
                assoc_files.append(self.snap_dir.parent / f"{self.snap_id}-in.yml")
        assoc_files = [x for x in assoc_files if x.exists()]
        return assoc_files

    def remove(self, keep_lock: bool = True) -> None:
        """Remove a snap"""
        assoc_files = self.get_paths()
        log.info("Removing files associated with snap %s: %s", self, assoc_files)
        for fp in assoc_files:
            if keep_lock and fp == self.lock_file:
                continue
            if not fp.is_symlink() and fp.is_dir():
                shutil.rmtree(fp)
            else:
                fp.unlink()
        by_hash = self.snap_dir.parent.parent / ".by_hash"
        if by_hash.exists():
            for link_path in by_hash.iterdir():
                if link_path.is_symlink() and link_path.resolve() in assoc_files:
                    link_path.unlink()
                    break

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

    @classmethod
    def make_symlinked(
        cls, source: "SnapSpec", name: str, new_id: SnapId
    ) -> "SnapSpec":
        """Create symlinked snap with `new_id` pointing to `source`"""
        new_lock = None
        for src_path in source.get_paths():
            new_path = (
                src_path.parent.parent
                / name
                / src_path.name.replace(str(source.snap_id), str(new_id))
            )
            new_path.symlink_to(
                os.path.relpath(src_path, new_path.parent), src_path.is_dir()
            )
            if src_path == source.lock_file:
                new_lock = new_path
        return cls.from_lock_path(new_lock)
