from pathlib import Path

import typer


DEFAULT_SLURM_TASKS = 16

DEFAULT_CONF_PATHS = [
    Path("/etc/byoe.yaml"), 
    Path(typer.get_app_dir("byoe")) / "byoe.yaml", 
    Path("./byoe.yaml"),
]