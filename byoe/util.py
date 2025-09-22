import os, shlex, json, re, logging
from pathlib import Path
from io import TextIOWrapper
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Union, Callable

import sh # type: ignore

from .globals import ShellType


sh = sh.bake(_tty_out=False)
bash = sh.bash
which = sh.which
try:
    srun = sh.srun
    HAS_SLURM = True
except sh.CommandNotFound:
    HAS_SLURM = False


log = logging.getLogger(__name__)


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
    """Get special kwargs used to create this command"""
    return {
        f"_{kw}": val if not hasattr(val, "copy") else val.copy()
        for kw, val in cmd._partial_call_args.items()
    }


@dataclass
class StreamHandler:
    """Handle output stream from an `sh.Command` instance"""
    log_file: Optional[TextIOWrapper] = None

    callbacks: List[Callable[[str], None]] = field(default_factory=list)

    tee: bool = True

    def __call__(self, data: str) -> None:
        if self.log_file is not None:
            self.log_file.write(data)
        for cb in self.callbacks:
            cb(data)

    @classmethod
    def merge(
        cls, lower: Optional["StreamHandler"], upper: Optional["StreamHandler"]
    ) -> "StreamHandler":
        if lower is None:
            return upper
        if upper is None:
            return lower
        return cls(
            upper.log_file if upper.log_file is not None else lower.log_file,
            upper.callbacks + lower.callbacks,
            upper.tee or lower.tee,
        )


@dataclass
class RegexCallback:
    """Callback to find lines matching some expression"""
    pattern: str

    matches: List[re.Match] = field(default_factory=list)

    max_matches: int = 0

    def __call__(self, data: str) -> None:
        if self.max_matches and len(self.matches) == self.max_matches:
            return
        mtch = re.match(self.pattern, data)
        if mtch is not None:
            self.matches.append(mtch)


def get_cmd(
    cmd: Union[str, Path, sh.Command],
    env: Optional[Dict[str, str]] = None,
    out_handler: Optional[Union[StreamHandler, TextIOWrapper]] = None,
    err_handler: Optional[Union[StreamHandler, TextIOWrapper]] = None,
    err_to_out: Optional[bool] = None,
    ref_cmd: Optional[sh.Command] = None,
) -> sh.Command:
    """Helper to create / modify sh.Command instances

    The "special" kwargs for the `sh.Command` being produced (e.g. `_env`, `_out`, 
    `_err`, etc.) are determined from the `cmd`, then the `ref_cmd`, and finally the 
    `env` / `out_handler` / `err_handler` / `err_to_out` arguments. 
    """
    base_special = ref_special = None
    handlers = {}
    if isinstance(cmd, sh.Command):
        base_special = get_sh_special(cmd)
    if ref_cmd is not None:
        ref_special = get_sh_special(ref_cmd)
    if base_special is not None:
        special = base_special
        handlers["_out"] = base_special.get("_out")
        handlers["_err"] = base_special.get("_err")
        if ref_special is not None:
            special.update(ref_special)
            if "_out" in ref_special and handlers["_out"] is not None:
                special["_out"] = StreamHandler.merge(handlers["_out"], ref_special["_out"])
            if "_err" in ref_special and handlers["_err"] is not None:
                special["_err"] = StreamHandler.merge(handlers["_err"], ref_special["_err"])
    elif ref_special is not None:
        special = ref_special
    else:
        special = {}
    if env is not None:
        if "_env" not in special:
            special["_env"] =  {}
        special["_env"].update(env)
    if out_handler is not None:
        if not isinstance(out_handler, StreamHandler):
            out_handler = StreamHandler(out_handler)
        if "_out" in special:
            special["_out"] = StreamHandler.merge(special["_out"], out_handler)
        else:
            special["_out"] = out_handler
    if err_handler is not None:
        if not isinstance(err_handler, StreamHandler):
            err_handler = StreamHandler(err_handler)
        if "_err" in special:
            special["_err"] = StreamHandler.merge(special["_err"], err_handler)
        else:
            special["_err"] = err_handler
    if err_to_out is not None:
        if err_to_out:
            if "_err" in special:
                raise ValueError("Can't set both err_to_out and pass err_handler")
            special["_err_to_out"] = True
        elif "_err_to_out" in special:
            del special["_err_to_out"]
    elif "_err" in special:
        if "_err_to_out" in special:
            del special["_err_to_out"]
    tee = set()
    if "_out" in special and special["_out"].tee:
        tee.add("out")
    if "_err" in special and special["_err"].tee:
        tee.add("err")
    if tee:
        special["_tee"] = tee
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
