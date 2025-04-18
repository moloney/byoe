import os, shlex, json, logging
from datetime import datetime, timedelta
from pathlib import Path
from io import TextIOWrapper
from difflib import unified_diff
from typing import List, Dict, Optional, Tuple, Union

import sh # type: ignore

from .globals import ShellType
from .snaps import SnapId, SnapSpec


sh = sh.bake(_tty_out=False)
bash = sh.bash
which = sh.which
try:
    srun = sh.srun
    HAS_SLURM = True
except sh.CommandNotFound:
    HAS_SLURM = False


log = logging.getLogger(__name__)


def select_snap(
    snap_ids: List[SnapId], period: int, now: Optional[datetime] = None
) -> SnapId:
    """Select snapshot date base on the `period` (and `now`)"""
    if not snap_ids:
        raise ValueError("Can't select from empty list of `snap_ids`")
    if now is None:
        now = datetime.now()
    if period > 12:
        if period % 12 != 0:
            raise ValueError(
                "Update periods over 12 months must be evenly divisible by 12"
            )
        period_yrs = period // 12
        tgt = datetime(now.year - (now.year % period_yrs), 1, 1)
    else:
        if 12 % period != 0:
            raise ValueError("Update periods under 12 months must evenly divide 12")
        tgt = datetime(now.year, now.month - ((now.month - 1) % period), 1)
    by_delta: Dict[timedelta, List[SnapId]] = {}
    min_delta = None
    log.debug("Target date is %s Selecting from snap_ids: %s", tgt, snap_ids)
    for snap_id in snap_ids:
        delta = abs(snap_id.time_stamp - tgt)
        if delta not in by_delta:
            by_delta[delta] = []
        by_delta[delta].append(snap_id)
        if min_delta is None or delta < min_delta:
            min_delta = delta
    assert min_delta is not None
    return sorted(by_delta[min_delta])[-1]


def get_closest_snap(
    tgt: SnapId, avail: List[Tuple[SnapSpec, ...]]
) -> Optional[Tuple[SnapSpec, ...]]:
    """Select the SnapSpec to use given the `tgt` SnapId"""
    for idx, snaps in enumerate(avail):
        if snaps[0].snap_id == tgt:
            return snaps
        elif snaps[0].snap_id > tgt:
            if idx != 0:
                return avail[idx - 1]
            return snaps
    if avail:
        return avail[-1]
    return None


def get_activated_envrion(
    activation_scripts: List[str], base_env: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """Get the environment after running one or more scripts in Bash

    DON'T RUN ON UNTRUSTED INPUT!
    """
    if base_env is None:
        base_env = os.environ.copy()
    activation_scripts = [str(x) for x in activation_scripts]
    bash_cmd = "\n".join(
        activation_scripts
        + ['python -c "import json, os ; print(json.dumps(dict(os.environ)))"']
    )
    env_json_str = bash(_in=bash_cmd, _env=base_env)
    return json.loads(env_json_str)


def get_ssl_env():
    """Get environment variables to handle alternative TLS/SSL cert locations

    For now we just handle Redhat.
    """
    env_data = {}
    alt_cert_dir = Path("/etc/pki/tls/certs")
    if alt_cert_dir.exists():
        env_data["SSL_CERT_DIR"] = str(alt_cert_dir)
    alt_cert_file = Path("/etc/pki/tls/cert.pem")
    if alt_cert_file.exists():
        env_data["SSL_CERT_FILE"] = str(alt_cert_file)
    return env_data


def get_sh_special(cmd: sh.Command):
    """Get special kwargs used to create thie command"""
    return {
        f"_{kw}": val if not hasattr(val, "copy") else val.copy()
        for kw, val in cmd._partial_call_args.items()
    }


def get_cmd(
    cmd: Union[str, Path, sh.Command],
    env: Optional[Dict[str, str]] = None,
    log_file: Optional[TextIOWrapper] = None,
    ref_cmd: Optional[sh.Command] = None,
):
    """Helper to create / modify sh.Command instances with environment mods and logging
    
    The environment / logging setting priorities are `env` / `log_file` args > `ref_cmd`
    attributes > `cmd` attributes (if it is a `sh.Command`). 
    """
    special = {}
    if isinstance(cmd, sh.Command):
        special.update(get_sh_special(cmd))
    if ref_cmd is not None:
        special.update(get_sh_special(ref_cmd))
    if env:
        if "_env" not in special:
            special["_env"] =  {}
        special["_env"].update(env)
    if log_file:
        special["_out"] = log_file
        special["_err"] = log_file
        special["_tee"] = {"err", "out"}
    if isinstance(cmd, sh.Command):
        cmd = cmd.bake(**special)
    else:
        cmd = Path(cmd)
        if cmd.is_absolute():
            cmd_path = str(cmd)
        else:
            try:
                cmd_path = which(str(cmd), _env=env).strip("\n")
            except sh.ErrorReturnCode:
                raise sh.CommandNotFound(cmd)
        cmd = getattr(sh, cmd_path).bake(**special)
    return cmd


def wrap_cmd(
    wrapper_cmd: sh.Command,
    inner_cmd: sh.Command,
    inject_env: Optional[Dict[str, str]] = None,
    make_inner_relative: bool = False,
) -> sh.Command:
    """Call ``wrapper_cmd`` with ``inner_cmd`` as the final arguments
    
    If `make_inner_relative` is true we use the relative command name instead of the 
    absolute path, which is needed when the wrapper could change the path we want to 
    use.
    """
    inner_path = Path(inner_cmd._path)
    if make_inner_relative:
        inner_str = inner_path.name
    else:
        inner_str = str(inner_path)
    args = [inner_str] + inner_cmd._partial_baked_args
    sh_kwargs = get_sh_special(inner_cmd)
    if inject_env:
        if "_env" not in sh_kwargs:
            sh_kwargs["_env"] = os.environ.copy()
        sh_kwargs["_env"].update(inject_env)
    return wrapper_cmd.bake(args, **sh_kwargs)


def srun_wrap(
    cmd: sh.Command,
    n_cpus: int = 1,
    base_args: str = "",
    tmp_dir: Optional[Union[str, Path]] = None,
    make_inner_relative: bool = False,
) -> sh.Command:
    """Wrap existing sh.Command to run on slurm with 'srun'"""
    srun_args = shlex.split(base_args) + ["--cpus-per-task=%s" % n_cpus]
    inject_env = None if tmp_dir is None else {"TMPDIR": str(tmp_dir)}
    return wrap_cmd(srun.bake(srun_args), cmd, inject_env, make_inner_relative)


def make_app_act_script(snap_dir: Path, shell_type: ShellType) -> str:
    """Create an activation script for isolate app snapshot"""
    if shell_type == ShellType.SH:
        return "\n".join(
            [
                f"export PATH={snap_dir / 'bin'}:$PATH",
                f"export MANPATH={snap_dir / 'man'}:$MANPATH",
            ]
        )
    elif shell_type == ShellType.CSH:
        return "\n".join(
            [
                f"setenv PATH {snap_dir / 'bin'}:$PATH",
                f"setenv MANPATH {snap_dir / 'man'}:$MANPATH",
            ]
        )
    elif shell_type == ShellType.FISH:
        return "\n".join(
            [
                f"set -gx PATH {snap_dir / 'bin'}:$PATH",
                f"set -gx MANPATH {snap_dir / 'man'}:$MANPATH",
            ]
        )
    else:
        raise NotImplementedError


def parse_env_set(line: str, shell_type: ShellType) -> Optional[Tuple[str, str]]:
    toks=line.split()
    if len(toks) < 2:
        return None
    if shell_type is ShellType.SH:
        if toks[0] != "export":
            return None
        name = toks[1].split("=")[0]
        val = "=".join(line.split("=")[1:])
    elif shell_type is ShellType.CSH:
        if toks[0] != "setenv":
            return None
        name = toks[1]
        val = " ".join(toks[2:])
    elif shell_type is ShellType.FISH:
        if toks[0] != "set" and toks[1] != "-gx":
            return None
        name = toks[2]
        val = " ".join(toks[3:])
    else:
        raise NotImplementedError
    return name, val


def format_env_set(name: str, val:str, shell_type: ShellType) -> str:
    if shell_type is ShellType.SH:
        return f"export {name}={val}"
    elif shell_type is ShellType.CSH:
        return f"setenv {name} {val}"
    elif shell_type is ShellType.FISH:
        return f"set -gx {name} {val}"
    else:
        raise NotImplementedError


def unexpand_act_vars(act_script: str, shell_type: ShellType) -> str:
    orig_lines = act_script.split("\n")
    patched_lines = []
    for line in orig_lines:
        res = parse_env_set(line, shell_type)
        if res is None:
            patched_lines.append(line)
            continue
        name, new_val = res
        old_val = os.environ.get(name)
        if old_val is not None:
            new_val = new_val.replace(old_val, f"${{{name}}}")
            patched_lines.append(format_env_set(name, new_val, shell_type))
        else:
            patched_lines.append(line)
    return "\n".join(patched_lines)


def diff_env(pre_env, post_env):
    res = {}
    res["new"] = {k: v for k, v in post_env.items() if k not in pre_env}
    res["del"] = {k: v for k, v in pre_env.items() if k not in post_env}
    res["pre"] = {}
    for k, pre_v in pre_env.items():
        post_v = post_env.get(k)
        if post_v is None:
            continue
        pre_toks = pre_v.split(os.pathsep)
        post_toks = post_v.split(os.pathsep)
        curr_seq = []
        before = ""
        post_idx = 0
        res["pre"][k] = {}
        for pre_tok in pre_toks:
            post_tok = post_toks[post_idx]
            if pre_tok != post_tok:
                curr_seq.append(pre_tok)
                continue
            else:
                #if curr_seq:
                # TODO: A path could appear mutiple times in the sequence, although it
                #       is semantically equivalent to delete/ignore those
                res["pre"][k][post_tok] = curr_seq
    return res

def restore_env(post_env, env_diff):
    for k in env_diff["new"]:
        del post_env[k]
    for k, old_v in env_diff["del"].items():
        post_env[k] = old_v
