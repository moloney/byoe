import os, sys, logging
from tempfile import NamedTemporaryFile
from pathlib import Path
from datetime import datetime
from typing import Annotated, Optional, List

import yaml
import typer
from rich.console import Console
import sh # type: ignore

from .globals import EnvType, UpdateChannel, ShellType
from .snaps import SnapId, InvalidSnapIdError
from .util import get_activated_envrion, get_cmd
from .conf import MissingConfigError, UserConfig, get_user_conf
from .byoe import NoCompilerFoundError, ByoeRepo


log = logging.getLogger("byoe")

success_console = Console(style="green")

error_console = Console(stderr=True, style="bold red")


cli = typer.Typer()


conf_data = {}


@cli.callback(no_args_is_help=True)
def main(
    verbose: bool = False,
    debug: bool = False,
    base_dir: Optional[Path] = None,
):
    """
    Manage environments
    """
    root_logger = logging.getLogger("")
    root_logger.setLevel(logging.DEBUG)
    sh_logger = logging.getLogger("sh")
    if debug:
        sh_logger.setLevel(logging.INFO)
        sh_logger.addFilter(lambda rec: rec.msg.endswith("process started"))
    else:
        sh_logger.setLevel(logging.WARNING)
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
    conf_path = Path(
        os.environ.get("BYOE_USER_CONF", Path(typer.get_app_dir("byoe")) / "conf.yaml")
    )
    try:
        conf_data["user"] = get_user_conf(conf_path)
    except MissingConfigError:
        pass
    if base_dir is not None:
        if "user" in conf_data:
            conf_data["user"].base_dir = base_dir
        else:
            conf_data["user"] = UserConfig(base_dir)
    if "user" not in conf_data:
        if sys.stdout.isatty():
            conf_data["user"] = UserConfig.build_interactive()
            conf_path.parent.mkdir(parents=True, exist_ok=True)
            conf_path.write_text(yaml.dump(conf_data["user"].to_dict()))
        else:
            error_console.print("No user config at: {conf_path}")


def _get_success_intro_msg(repo):
    return f"""
[bold green]Success:[/bold green] Add config files under {repo._locs["confd"]}
or modify  {repo._base_dir}/site_conf.yaml and place any needed license 
files in {repo._locs["licenses"]} before running the [bold magenta]update[/bold magenta] command
"""


@cli.command(rich_help_panel="Admin Commands")
def prep_repo(
    pull_spack: bool = True,
    pull_spack_packages: bool = True,
    log_path: Optional[Path] = None,
):
    """Prepare the configured base directory, including fetching and updating spack repo

    Gemerally this is just called once to setup a new repo, but it can be used to update
    spack and spack-packages without modifying any BYOE envs / apps.
    """
    if not conf_data:
        error_console.print("Unable to find config")
        return 1
    update_ts = datetime.now().strftime("%Y%m%d%H%M%S")
    if log_path is None:
        log_path = conf_data["user"].base_dir / "logs" / f"init_dir_{update_ts}.log"
    log.info("Logging prep_base_dir run to file: %s" % log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("w")
    file_handler = logging.StreamHandler(log_file)
    file_handler.setLevel(logging.INFO)
    root_logger = logging.getLogger("")
    root_logger.addHandler(file_handler)
    repo = ByoeRepo(conf_data["user"].base_dir)
    try:
        repo.prep_dir(pull_spack, pull_spack_packages, log_file)
    except NoCompilerFoundError:
        error_console.print("No system compiler found, install one and rerun.")
        return 1
    success_console.print(_get_success_intro_msg(repo))


@cli.command(rich_help_panel="Admin Commands")
def update(
    env_or_app: Annotated[Optional[List[str]], typer.Argument()] = None,
    pull_spack: bool = True,
    pull_spack_packages: bool = True,
    label: Optional[str] = None,
    log_path: Optional[Path] = None,
):
    """Update all configured environments and apps, or just those specified as args"""
    if not conf_data:
        error_console.print("Unable to find config")
        return 1
    update_ts = datetime.now().strftime("%Y%m%d%H%M%S")
    if log_path is None:
        log_path = conf_data["user"].base_dir / "logs" / f"update_envs_{update_ts}.log"
    log.info("Logging update_envs run to file: %s" % log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("w")
    file_handler = logging.StreamHandler(log_file)
    file_handler.setLevel(logging.INFO)
    root_logger = logging.getLogger("")
    root_logger.addHandler(file_handler)
    if not env_or_app:
        env_or_app = None
    repo = ByoeRepo(conf_data["user"].base_dir)
    try:
        repo.update(
            env_or_app,
            pull_spack=pull_spack,
            pull_spack_packages=pull_spack_packages,
            label=label,
            log_file=log_file,
        )
    except NoCompilerFoundError:
        error_console.print("No system compiler found, install one and rerun.")
        return 1


def _wrp_cmd(cmd: sh.Command, ctx: typer.Context):
    cmd = cmd.bake(_in=sys.stdin, _out=sys.stdout, _err=sys.stderr)
    try:
        cmd(ctx.args)
    except sh.ErrorReturnCode as e:
        raise typer.Exit(code=e.exit_code)


@cli.command(
    rich_help_panel="Admin Commands",
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "help_option_names": ["--byoe-help"],
    },
)
def spack(ctx: typer.Context):
    """Forward commands to internal `spack` command (use `--byoe-help` for details)

    Useful for looking up package info and testing installs of individual packages.

    Administrators must be careful when running commands that mutate state, and
    generally avoid commands that update configuration.
    """
    repo = ByoeRepo(conf_data["user"].base_dir)
    if repo._locs["spack"].exists():
        spack_cmd = repo._get_spack()
    else:
        spack_cmd = repo.get_spack()
    _wrp_cmd(spack_cmd, ctx)
    

@cli.command(
    rich_help_panel="Admin Commands",
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "help_option_names": ["--byoe-help"],
    },
)
def micromamba(ctx: typer.Context):
    """Forward commands to internal `micromamba` command (use `--byoe-help` for details)

    Useful for looking up package info and testing installs of individual packages.
    """
    repo = ByoeRepo(conf_data["user"].base_dir)
    mm_cmd = repo.get_micromamba()
    _wrp_cmd(mm_cmd, ctx)
    

@cli.command(
    "conda-lock",
    rich_help_panel="Admin Commands",
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "help_option_names": ["--byoe-help"],
    },
)
def conda_lock(ctx: typer.Context):
    """Forward commands to internal `conda-lock` command (use `--byoe-help` for details)
    """
    repo = ByoeRepo(conf_data["user"].base_dir)
    cl_cmd = repo.get_conda_lock()
    _wrp_cmd(cl_cmd, ctx)


@cli.command(
    rich_help_panel="Admin Commands",
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "help_option_names": ["--byoe-help"],
    },
)
def apptainer(ctx: typer.Context):
    """Forward commands to internal `apptainer` command (use `--byoe-help` for details)
    """
    repo = ByoeRepo(conf_data["user"].base_dir)
    apptainer_cmd = repo.get_apptainer()
    _wrp_cmd(apptainer_cmd, ctx)


def _snap_id_cb(value: str) -> SnapId:
    try:
        return SnapId.from_str(value)
    except Exception as e:
        raise typer.BadParameter(str(e))


@cli.command(
    rich_help_panel="User Commands",
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "help_option_names": ["--byoe-help"],
    },
)
def run(
    ctx: typer.Context,
    byoe_name: Optional[str] = None,
    byoe_channel: Optional[UpdateChannel] = None,
    byoe_snap_id: Optional[SnapId] = typer.Option(default=None, parser=_snap_id_cb),
    byoe_skip_py_env: bool = False
):
    """Run the given command inside a byoe environment (use `--byoe-help` for details)"""
    repo = ByoeRepo(conf_data["user"].base_dir)
    act_script = repo.get_activate_script(
        byoe_name,
        byoe_channel,
        byoe_snap_id,
        byoe_skip_py_env,
        shell_type=ShellType.SH,
    )
    # TODO: If we're launching this in a shell we should just source the activation
    #       script there before running the users code. Also since we are running a
    #       shell anyway, might as well run the user command in there too?
    act_env = get_activated_envrion([act_script])
    cmd = get_cmd(ctx.args[0], act_env)
    try:
        cmd(ctx.args[1:], _in=sys.stdin, _out=sys.stdout, _err=sys.stderr)
    except sh.ErrorReturnCode as e:
        raise typer.Exit(code=e.exit_code)


@cli.command(rich_help_panel="User Commands")
def activate(
    env_name: Optional[str] = None,
    channel: Optional[UpdateChannel] = None,
    snap_id: Optional[SnapId] = typer.Option(default=None, parser=_snap_id_cb),
    skip_py_env: bool = False,
    disable: Optional[List[str]] = None,
    enable: Optional[List[str]] = None,
    shell_type: Optional[ShellType] = None,
    tmp: bool = False,
):
    """Print activation script for an environment plus any number of apps.

    Most users (i.e. BASH users) can do 'source <(byoe activate)' to change their
    current shell environment. Users with FISH as their shell can do
    'source (byoe activate | psub)', while any shell (e.g. TCSH) can do
    'source $(activate --tmp)'.
    """
    repo = ByoeRepo(conf_data["user"].base_dir)
    act_script = repo.get_activate_script(
        env_name, channel, snap_id, skip_py_env, disable, enable, shell_type
    )
    if tmp:
        tmp_f = NamedTemporaryFile(delete=False)
        tmp_f.write(act_script.encode())
        tmp_f.write(f"\nrm {tmp_f.name}\n".encode())
        print(tmp_f.name)
    else:
        print(act_script)


@cli.command(rich_help_panel="User Commands")
def status(short: bool = False):
    """Print info about any currently activated environment or apps"""
    snap_id = os.environ.get("BYOE_SNAP_ID")
    envs = os.environ.get("BYOE_ENVS")
    apps = os.environ.get("BYOE_APPS")
    if not snap_id:
        return
    if short:
        if envs:
            print(envs.split(os.pathsep)[0].split("/")[-1])
        else:
            print(f"_apps_only_@{snap_id}")
    else:
        print(f"Snap ID: {snap_id}")
        if envs:
            print("Environment layers:")
            for env_layer in envs.split(os.pathsep):
                print(f"\t{env_layer}")
        if apps:
            print("Apps:")
            for app in apps.split(os.pathsep):
                print(f"\t{app}")

# TODO: Add ability to query / update / remove software stored in ._internal dir

#@cli.command(rich_help_panel="User Commands")
#def export():
#    """Export current environment as yaml description"""
    