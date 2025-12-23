
"""
Read Configs

SRP: Read configs for operation and process.
"""

from pathlib import Path

import yaml


# ==================================================
# Helper
# ==================================================

def _main_path(project_name: str) -> Path:
    project_root = Path.cwd()

    while project_root.parent != project_root:
        if (
            project_root.name == project_name or (project_root / 'configs').is_dir()
        ):
            return project_root
        project_root = project_root.parent

    raise RuntimeError(f"Could not find project root '{project_name}' directory")


def _load_yaml(path: str | Path) -> dict[str, any]:
    with open(path, 'r') as f:
        return yaml.safe_load(f)
    

# ==================================================
# Load Configs
# ==================================================

def load_config(project_name: str) -> dict[str, any]: 
    config_path = _main_path(project_name) / 'configs' / 'base.yaml'
    return _load_yaml(config_path)

