import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import typer
from rich.console import Console

from .byoe import (
    DEFAULT_CONF_PATHS,
    UpdateChannel,
    get_config,
    NoCompilerFoundError,
    prep_base_dir,
    update_all,
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
):
    """Update environments"""
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
        update_all(update_ts, conf_data, n_tasks, log_file)
    except NoCompilerFoundError:
        error_console.print("No system compiler found, install one and rerun.")
        return 1


@cli.command()
def activate(
    name: Optional[str] = None,
    channel: Optional[UpdateChannel] = None,
):
    """Activate an environment"""
