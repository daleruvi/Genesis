from pathlib import Path

from genesis.config.settings import PROJECT_ROOT, SRC_DIR


def project_root() -> Path:
    return PROJECT_ROOT


def src_dir() -> Path:
    return SRC_DIR
