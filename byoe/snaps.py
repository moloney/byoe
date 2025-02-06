"""Snapshot tracking"""
import logging, re, json, os, shutil
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from hashlib import blake2b
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


import yaml

from .globals import ENV_SUFFIXES, LOCK_SUFFIXES, TS_FORMAT, EnvType, ShellType, SnapType


log = logging.getLogger(__name__)


VALID_SNAP_ID_REGEX = r"^([0-9]+)(?:\.([A-Za-z]+)?([0-9]+))?$"


class InvalidSnapIdError(Exception):
    """Raised when trying to parse an invalid snap ID"""
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return self.msg


@dataclass(frozen=True)
class SnapId:
    """Uniquely identify a snaphot"""

    time_stamp: datetime

    version: int = 0

    label: Optional[str] = None

    def __repr__(self) -> str:
        if self.version == 0 and self.label is None:
            return f"{self.time_stamp.strftime(TS_FORMAT)}"
        elif self.label is None:
            return f"{self.time_stamp.strftime(TS_FORMAT)}.{self.version}"
        else:
            return f"{self.time_stamp.strftime(TS_FORMAT)}.{self.label}{self.version}"

    @classmethod
    def from_str(cls, val: str) -> "SnapId":
        mtch = re.match(VALID_SNAP_ID_REGEX, val)
        if mtch is None:
            raise InvalidSnapIdError(f"Invalid format: {val}")
        ts, label, vers = mtch.groups()
        try:
            ts = datetime.strptime(ts, TS_FORMAT)
        except:
            raise InvalidSnapIdError(f"Invalid date string: {ts}")
        vers = 0 if vers is None else int(vers)
        return cls(ts, vers, label)

    @classmethod
    def from_prefix(cls, val: str) -> "SnapId":
        mtch = re.search(rf"^{VALID_SNAP_ID_REGEX[:-1]}", val)
        if not mtch:
            raise InvalidSnapIdError(f"Invalid prefix format: {val}")
        return cls.from_str(mtch.group())

    def __lt__(self, other):
        self_lbl = self.label if self.label is not None else ''
        other_lbl = other.label if other.label is not None else ''
        return (self.time_stamp, self_lbl, self.version) < (other.time_stamp, other_lbl, other.version)


class ActivationUnsupportedError(Exception):
    """Activation attempted on an environment type that doesn't support it"""


class SnapReferenceError(Exception):
    """Raised when trying to remove a snap with existing valid references"""


@dataclass(frozen=True)
class SnapSpec:
    """Capture info about an environment / app snapshot"""

    snap_id: SnapId

    env_type: EnvType

    name: str

    snap_path: Path

    snap_type: SnapType

    def __postinit__(self):
        if self.snap_type == SnapType.ENV:
            env_suffix = ENV_SUFFIXES.get(self.env_type)
            if env_suffix and not self.snap_path.endswith(env_suffix):
                raise ValueError(f"The 'snap_path' doesn't have the required suffix: {env_suffix}")

    def __lt__(self, other: "SnapSpec"):
        return (self.name, self.snap_id) < (other.name, other.snap_id)

    def __str__(self) -> str:
        return f"{self.env_type.name}/{self.snap_name}"

    @property
    def snap_name(self) -> str:
        return f"{self.name}@{self.snap_id}"

    @property
    def lock_suffix(self) -> str:
        return LOCK_SUFFIXES[self.env_type]

    @property
    def lock_file(self) -> Path:
        return self.snap_path.parent / f"{self.snap_id}{self.lock_suffix}"

    @cached_property
    def lock_hash(self) -> str:
        data = self.get_lock_data()
        if self.env_type is EnvType.SPACK:
            assert isinstance(data, dict)
            roots = sorted(data["roots"], key=lambda x: x["spec"])
            data = [f"{x['spec']}/{x['hash']}" for x in roots]
        elif self.env_type is EnvType.PYTHON:
            assert isinstance(data, list)
            data = sorted([l for l in data if not l.startswith("#")])
        elif self.env_type is EnvType.CONDA:
            assert isinstance(data, dict)
            pkgs = sorted(data["package"], key=lambda x: x["name"])
            data = [f"{x['name']}/{x['hash']['sha256'] if 'sha256' in x['hash'] else x['hash']['md5']}" for x in pkgs]
        return blake2b("\n".join(data).encode('utf-8'), digest_size=20).hexdigest()

    @property
    def lock_hash_dir(self) -> Path:
        return self.snap_path.parent.parent / ".by_hash"

    @property
    def lock_hash_link(self) -> Path:
        return self.lock_hash_dir / self.lock_hash

    @property
    def is_canon(self) -> bool:
        return self.lock_hash_link.exists() and self.lock_hash_link.resolve() == self.lock_file

    @property
    def supports_activation(self) -> bool:
        return not (self.snap_type == SnapType.ENV and self.env_type == EnvType.APPTAINER)

    def get_activate_path(self, shell: ShellType = ShellType.SH) -> Path:
        """Get path to the activation script"""
        if not self.supports_activation:
            raise ActivationUnsupportedError()
        if self.snap_type == SnapType.APP or self.env_type != EnvType.PYTHON:
            return self.snap_path.parent / f"{self.snap_path.name}-activate.{shell.value}"
        elif self.env_type == EnvType.PYTHON:
            if shell == ShellType.SH:
                suffix = ""
            else:
                suffix = f".{shell.value}"
            return self.snap_path / "bin" / f"activate{suffix}"

    def get_lock_data(self) -> Union[Dict[str, Any], List[str]]:
        """Get the data from the `lock_file`"""
        txt_data = self.lock_file.read_text()
        if self.env_type is EnvType.SPACK:
            return json.loads(txt_data)
        if self.env_type is EnvType.CONDA:
            return yaml.safe_load(txt_data)
        else:
            return [l.strip() for l in txt_data.split("\n") if l.strip()]

    def get_paths(self) -> List[Path]:
        """Get list of paths associated with the snap"""
        assoc_files = [self.snap_path]
        if self.supports_activation:
            for sh_type in ShellType:
                assoc_files.append(self.get_activate_path(sh_type))
        assoc_files.append(self.lock_file)
        if self.snap_type == SnapType.ENV:
            if self.env_type == EnvType.SPACK:
                assoc_files.append(self.snap_path.parent / f"._{self.snap_id}")
                assoc_files.append(self.snap_path.parent / f"{self.snap_id}-env")
            elif self.env_type == EnvType.PYTHON:
                assoc_files.append(self.snap_path.parent / f"{self.snap_id}-main-req.in")
                assoc_files.append(self.snap_path.parent / f"{self.snap_id}-sys-req.txt")
            elif self.env_type == EnvType.CONDA:
                assoc_files.append(self.snap_path.parent / f"{self.snap_id}-in.yml")
                assoc_files.append(self.snap_path.parent / f"{self.snap_id}-virtual.yml")
        assoc_files = [x for x in assoc_files if x.exists()]
        return assoc_files

    def get_references(self) -> List["SnapSpec"]:
        """Get list of snaps to point to this one through symlinks"""
        if not self.is_canon:
            return []
        res = []
        for lock_path in self.snap_path.parent.parent.glob(f"*/*{self.lock_suffix}"):
            if not lock_path.is_symlink() or lock_path.resolve() != self.lock_file:
                continue
            try:
                SnapId.from_prefix(lock_path.name)
            except InvalidSnapIdError:
                pass
            else:
                res.append(SnapSpec.from_lock_path(lock_path))
        return res

    def remove(self, keep_lock: bool = True) -> None:
        """Remove a snap"""
        # Refuse to delete snap with references
        refs = self.get_references()
        if refs:
            raise SnapReferenceError("Can't delete snap with references")
        assoc_files = self.get_paths()
        log.info("Removing files associated with snap %s: %s", self, assoc_files)
        for fp in assoc_files:
            if keep_lock and fp == self.lock_file:
                continue
            if not fp.is_symlink() and fp.is_dir():
                shutil.rmtree(fp)
            else:
                fp.unlink()

    def dedupe(self) -> bool:
        """If a pre-existing SnapSpec with same env_type and hash exists symlink to it

        Otherwise register a link to this snap based on the hash
        """
        hash_link = self.lock_hash_link
        if hash_link.exists():
            prev = SnapSpec.from_lock_path(hash_link.resolve())
            if self == prev:
                return False
            if not prev.snap_path.exists():
                hash_link.unlink()
            else:
                log.info("Duplicate snap found, symlinking to it")
                self.remove(keep_lock=False)
                SnapSpec.make_symlinked(prev, self.name, self.snap_id)
                return True
        else:
            hash_link.parent.mkdir(exist_ok=True)
        hash_link.symlink_to(os.path.relpath(self.lock_file, hash_link.parent))
        return False

    def stash_failed(self):
        """Stash any failed build artifacts in a hidden dir for debugging"""
        try:
            hash_link = self.lock_hash_link
        except:
            pass
        else:
            if hash_link.exists() and hash_link.resolve() == self.lock_file:
                hash_link.unlink()
        assoc_files = self.get_paths()
        if not assoc_files:
            return
        env_dir = self.snap_path.parent
        stash_dir = env_dir / f".failed_{self.snap_id}"
        stash_dir.mkdir()
        log.info("Stashing files associated with failed update to: %s", stash_dir)
        stashed = set()
        for fp in assoc_files:
            if fp.is_symlink():
                continue
            stash_path = stash_dir / os.path.relpath(fp, env_dir)
            shutil.move(fp, stash_path)
            stashed.add(stash_path)
        for fp in assoc_files:
            if not fp.is_symlink():
                continue
            stash_path = stash_dir / os.path.relpath(fp, env_dir)
            tgt = fp.resolve()
            if tgt in stashed or any(p in stashed for p in tgt.parents):
                tgt_stash = stash_dir / os.path.relpath(tgt, env_dir)
                stash_path.symlink_to(tgt_stash)
            else:
                stash_path.symlink_to(tgt)
            fp.unlink()

    @classmethod
    def from_lock_path(cls, lock_path: Path) -> "SnapSpec":
        """Generate a SnapSpec from the path to its lock file"""
        snap_id = SnapId.from_prefix(lock_path.stem)
        name = lock_path.parent.name
        env_type = EnvType(lock_path.parent.parent.name)
        snap_type = SnapType.ENV
        if lock_path.parent.parent.parent.name == "apps":
            snap_type = SnapType.APP
        if snap_type == SnapType.ENV:
            env_path = lock_path.parent / (str(snap_id) + ENV_SUFFIXES.get(env_type, ""))
        else:
            env_path = lock_path.parent / str(snap_id)
        return cls(snap_id, env_type, name, env_path, snap_type)

    @classmethod
    def make_symlinked(
        cls, source: "SnapSpec", name: str, new_id: SnapId
    ) -> "SnapSpec":
        """Create symlinked snap with `new_id` pointing to `source`"""
        new_lock = None
        path_map = {}
        for src_path in source.get_paths():
            new_path = (
                src_path.parent.parent
                / name
                / src_path.name.replace(str(source.snap_id), str(new_id))
            )
            path_map[src_path] = new_path
            if src_path == source.lock_file:
                new_lock = new_path
        if new_lock is None:
            raise ValueError("The 'source' is missing its lockfile")
        for src_path, new_path in path_map.items():
            new_path.symlink_to(
                os.path.relpath(src_path, new_path.parent), src_path.is_dir()
            )
        return cls.from_lock_path(new_lock)

