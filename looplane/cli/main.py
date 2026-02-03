import importlib
import sys
from pathlib import Path
from typing import Optional, Tuple

import click

from looplane.cli.monitor import run_monitor
from looplane.cli.worker import run_worker
from looplane.result.base import ResultBackend
from looplane.result.inmemory import InMemoryResultBackend
from looplane.result.lmdb import LMDBResultBackend
from looplane.storage.base import StorageBackend
from looplane.storage.inmemory import InMemoryStorage
from looplane.storage.lmdb_storage import LMDBStorage


def _load_app_module(module_path: str) -> None:
    """Load an application module to register tasks.

    Args:
        module_path: Either a Python module path (e.g., 'myapp.tasks')
                     or a file path (e.g., 'myapp/tasks.py').
    """
    # Add current directory to path if not present
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    if module_path.endswith(".py"):
        # File path - convert to module path
        module_path = module_path.replace("/", ".").replace("\\", ".").rstrip(".py")
        if module_path.startswith("."):
            module_path = module_path[1:]

    importlib.import_module(module_path)


def _create_backends(
    storage_type: str,
    db_path: str,
) -> Tuple[StorageBackend, ResultBackend]:
    """Create storage and result backends based on type.

    Args:
        storage_type: Either 'memory' or 'lmdb'.
        db_path: Path for LMDB storage.

    Returns:
        Tuple of (StorageBackend, ResultBackend).
    """
    storage_backend: StorageBackend
    result_backend: ResultBackend

    if storage_type == "lmdb":
        storage_backend = LMDBStorage(path=db_path)
        result_backend = LMDBResultBackend(path=db_path)
    else:
        storage_backend = InMemoryStorage()
        result_backend = InMemoryResultBackend()

    return storage_backend, result_backend


@click.group()
def cli() -> None:
    """Looplane - Async Task Queue for Python"""
    pass


@cli.command()
@click.option(
    "-A", "--app",
    help="Application module to load (e.g., 'myapp.tasks' or 'tasks.py')"
)
@click.option(
    "-c", "--concurrency",
    default=10,
    type=int,
    help="Number of concurrent tasks to process"
)
@click.option(
    "--storage",
    type=click.Choice(["memory", "lmdb"]),
    default="lmdb",
    help="Storage backend to use"
)
@click.option(
    "--db-path",
    default="/tmp/looplane",
    help="Database path (for lmdb storage)"
)
def worker(
    app: Optional[str],
    concurrency: int,
    storage: str,
    db_path: str,
) -> None:
    """Start a Looplane worker to process tasks.

    Examples:

        # Start worker with LMDB storage (default)
        looplane worker -A myapp.tasks

        # Start with custom concurrency
        looplane worker -A myapp.tasks -c 20

        # Start with in-memory storage (for testing)
        looplane worker -A myapp.tasks --storage memory
    """
    # Load application module if provided
    if app:
        try:
            _load_app_module(app)
        except ImportError as e:
            raise click.ClickException(f"Failed to load app module '{app}': {e}") from None

    # Create backends
    storage_backend, result_backend = _create_backends(storage, db_path)

    # Run worker
    run_worker(
        storage=storage_backend,
        result_backend=result_backend,
        concurrency=concurrency,
        app_module=app,
    )


@cli.command()
@click.option(
    "--storage",
    type=click.Choice(["memory", "lmdb"]),
    default="memory",
    help="Storage backend to use"
)
@click.option(
    "--db-path",
    default="/tmp/looplane",
    help="Database path (for lmdb storage)"
)
def monitor(storage: str, db_path: str) -> None:
    """Start the Looplane monitor UI.

    Shows real-time information about pending tasks and completed results.
    """
    storage_backend, result_backend = _create_backends(storage, db_path)
    run_monitor(storage_backend, result_backend)


if __name__ == "__main__":
    cli()
