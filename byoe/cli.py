import sys, logging
from tempfile import NamedTemporaryFile
from pathlib import Path
from datetime import datetime
from typing import Optional, List

import typer
from rich.console import Console
import sh

from ._globals import DEFAULT_CONF_PATHS, EnvType, UpdateChannel, ShellType
from .util import get_locations
from .spack import get_spack
from .byoe import (
    get_config,
    NoCompilerFoundError,
    prep_base_dir,
    update_all,
    get_activate_script,
)


log = logging.getLogger("byoe")


error_console = Console(stderr=True, style="bold red")


cli = typer.Typer()


conf_data = {}


@cli.callback()
def main(
    verbose: bool = False,
    debug: bool = False,
    config: Optional[Path] = None,
):
    """
    Manage environments
    """
    root_logger = logging.getLogger("")
    root_logger.setLevel(logging.DEBUG)
    sh_logger = logging.getLogger("sh")
    sh_logger.setLevel(logging.CRITICAL)
    stream_formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(stream_formatter)
    if debug:
        stream_handler.setLevel(logging.DEBUG)
    elif verbose:
        stream_handler.setLevel(logging.INFO)
    else:
        stream_handler.setLevel(logging.WARN)
    root_logger.addHandler(stream_handler)
    conf_paths = DEFAULT_CONF_PATHS
    if config:
        conf_paths.append(config)
    conf_data.update(get_config(conf_paths))


@cli.command()
def init_dir(
    n_tasks: Optional[int] = None,
    log_path: Optional[Path] = None,
):
    """Prepare the configured base directory

    This could take a while to run the first time, particularly if we need to build
    any compilers.

    Calling this is optional, mostly useful if you want to prepopulate some sub
    directories (e.g. licenses) before calling 'update_envs'.
    """
    if not conf_data:
        error_console.print("Unable to find config")
        return 1
    update_ts = datetime.now().strftime("%Y%m%d%H%M%S")
    if log_path is None:
        log_path = conf_data["base_dir"] / "logs" / f"prep_base_{update_ts}.log"
    log.info("Logging prep_base_dir run to file: %s" % log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("w")
    file_handler = logging.StreamHandler(log_file)
    file_handler.setLevel(logging.INFO)
    root_logger = logging.getLogger("")
    root_logger.addHandler(file_handler)
    try:
        prep_base_dir(conf_data, n_tasks, log_file)
    except NoCompilerFoundError:
        error_console.print("No system compiler found, install one and rerun.")
        return 1


@cli.command()
def update_envs(
    n_tasks: Optional[int] = None,
    log_path: Optional[Path] = None,
    pull_spack: bool = True,
):
    """Update configured environments"""
    if not conf_data:
        error_console.print("Unable to find config")
        return 1
    update_ts = datetime.now().strftime("%Y%m%d%H%M%S")
    if log_path is None:
        log_path = conf_data["base_dir"] / "logs" / f"update_envs_{update_ts}.log"
    log.info("Logging update_envs run to file: %s" % log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("w")
    file_handler = logging.StreamHandler(log_file)
    file_handler.setLevel(logging.INFO)
    root_logger = logging.getLogger("")
    root_logger.addHandler(file_handler)
    try:
        update_all(update_ts, conf_data, pull_spack, n_tasks, log_file)
    except NoCompilerFoundError:
        error_console.print("No system compiler found, install one and rerun.")
        return 1


@cli.command(
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "help_option_names": ["--byoe-help"],
    }
)
def spack(ctx: typer.Context):
    """Forward commands to internal `spack` command (use `--byoe-help` for details)

    Useful for looking up package info and testing installs of individual packages.

    Administrators must be careful when running commands that mutate state, and
    generally avoid commands that update configuration.
    """
    locs = get_locations(conf_data["base_dir"])
    spack_cmd = get_spack(locs).bake(_in=sys.stdin, _out=sys.stdout, _err=sys.stderr)
    try:
        spack_cmd(ctx.args)
    except sh.ErrorReturnCode:
        pass


@cli.command()
def activate(
    name: Optional[str] = None,
    channel: Optional[UpdateChannel] = None,
    time_stamp: Optional[str] = None,
    skip_layer: Optional[List[EnvType]] = None,
    shell_type: Optional[ShellType] = None,
    tmp: bool = False,
):
    """Produce activation script which can be used with 'source `byoe activate --tmp`'

    To avoid the use of a temp file each time you can do 'source <(byoe activate)' in 
    BASH, or 'source (byoe activate | psub)' in FISH. Unfortunately TCSH lacks such 
    functionality.
    """
    # TODO: Raise more specific errors in get_activate_script and convert to error messages here
    if EnvType.SPACK in skip_layer:
        if EnvType.PYTHON not in skip_layer:
            error_console.print(
                "If skipping 'spack' layer, 'python' must also be skipped"
            )
    act_script = get_activate_script(
        conf_data["base_dir"], name, channel, time_stamp, skip_layer, shell_type
    )
    if tmp:
        tmp_f = NamedTemporaryFile(delete=False)
        tmp_f.write(act_script.encode())
        print(tmp_f.name)
    else:
        print(act_script)
