"""Worker runner for the Looplane CLI."""

import asyncio
import os
import platform
import signal
import sys
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from looplane import __version__
from looplane.broker import StorageConsumer
from looplane.orchestrator import Orchestrator
from looplane.result.base import ResultBackend
from looplane.storage.base import StorageBackend
from looplane.task import task_registry


class WorkerRunner:
    """Runs the Looplane worker with startup banner and graceful shutdown."""

    def __init__(
        self,
        storage: StorageBackend,
        result_backend: ResultBackend,
        concurrency: int = 10,
        app_module: Optional[str] = None,
    ):
        self.storage = storage
        self.result_backend = result_backend
        self.concurrency = concurrency
        self.app_module = app_module
        self.console = Console()
        self._shutdown_event = asyncio.Event()
        self._orchestrator: Optional[Orchestrator] = None

    def _get_storage_info(self) -> str:
        """Get storage backend information."""
        storage_type = type(self.storage).__name__
        if hasattr(self.storage, "env") and hasattr(self.storage.env, "path"):
            path = self.storage.env.path()
            if isinstance(path, bytes):
                path = path.decode()
            return f"{storage_type} (path: {path})"
        return f"{storage_type} (transient: {self.storage.is_transient})"

    def _get_result_backend_info(self) -> str:
        """Get result backend information."""
        backend_type = type(self.result_backend).__name__
        if hasattr(self.result_backend, "env") and hasattr(self.result_backend.env, "path"):
            path = self.result_backend.env.path()
            if isinstance(path, bytes):
                path = path.decode()
            return f"{backend_type} (path: {path})"
        return backend_type

    def _print_banner(self) -> None:
        """Print the startup banner with system and configuration info."""
        # Header
        header = """
[bold blue]╦  ╔═╗╔═╗╔═╗╦  ╔═╗╔╗╔╔═╗[/bold blue]
[bold blue]║  ║ ║║ ║╠═╝║  ╠═╣║║║║╣ [/bold blue]
[bold blue]╩═╝╚═╝╚═╝╩  ╩═╝╩ ╩╝╚╝╚═╝[/bold blue]

[dim]Async Task Queue for Python[/dim]
        """
        self.console.print(Panel(header.strip(), border_style="blue"))

        # System Information Table
        sys_table = Table(title="System Information", border_style="dim", show_header=False)
        sys_table.add_column("Key", style="cyan")
        sys_table.add_column("Value", style="white")

        sys_table.add_row("Looplane Version", __version__)
        sys_table.add_row("Python Version", f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
        sys_table.add_row("Platform", platform.platform())
        sys_table.add_row("Process ID", str(os.getpid()))
        sys_table.add_row("Started At", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        self.console.print(sys_table)
        self.console.print()

        # Configuration Table
        config_table = Table(title="Configuration", border_style="dim", show_header=False)
        config_table.add_column("Key", style="cyan")
        config_table.add_column("Value", style="white")

        config_table.add_row("Concurrency", str(self.concurrency))
        config_table.add_row("Storage Backend", self._get_storage_info())
        config_table.add_row("Result Backend", self._get_result_backend_info())
        if self.app_module:
            config_table.add_row("App Module", self.app_module)

        self.console.print(config_table)
        self.console.print()

        # Registered Tasks Table
        tasks = task_registry.all()
        tasks_table = Table(title=f"Registered Tasks ({len(tasks)})", border_style="dim")
        tasks_table.add_column("#", style="dim", width=4)
        tasks_table.add_column("Task Name", style="green")
        tasks_table.add_column("Module", style="dim")

        if tasks:
            for i, (name, func) in enumerate(tasks.items(), 1):
                module = getattr(func, "__module__", "unknown")
                tasks_table.add_row(str(i), name, module)
        else:
            tasks_table.add_row("-", "[yellow]No tasks registered[/yellow]", "-")

        self.console.print(tasks_table)
        self.console.print()

        # Ready message
        self.console.print(
            "[bold green]Worker ready.[/bold green] "
            "Press [bold]Ctrl+C[/bold] to stop.\n"
        )

    def _print_shutdown(self) -> None:
        """Print shutdown message."""
        self.console.print("\n[bold yellow]Shutting down worker...[/bold yellow]")

    def _print_stopped(self) -> None:
        """Print stopped message."""
        self.console.print("[bold green]Worker stopped.[/bold green]")

    async def _run(self) -> None:
        """Run the worker loop."""
        consumer = StorageConsumer(storage=self.storage)

        self._orchestrator = Orchestrator(
            consumer=consumer,
            result_backend=self.result_backend,
            max_concurrent_tasks=self.concurrency,
        )

        await self._orchestrator.start()

        # Wait for shutdown signal
        await self._shutdown_event.wait()

        # Graceful shutdown
        self._print_shutdown()
        await self._orchestrator.stop(wait=True)
        await consumer.close()

    def _handle_signal(self, signum: int, frame: object) -> None:
        """Handle shutdown signals."""
        self._shutdown_event.set()

    def run(self) -> None:
        """Start the worker (blocking)."""
        # Print startup banner
        self._print_banner()

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        # Run the event loop
        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            pass

        self._print_stopped()


def run_worker(
    storage: StorageBackend,
    result_backend: ResultBackend,
    concurrency: int = 10,
    app_module: Optional[str] = None,
) -> None:
    """Start the Looplane worker.

    Args:
        storage: The storage backend for task queue.
        result_backend: The result backend for storing task results.
        concurrency: Number of concurrent tasks to process.
        app_module: Optional module path that was loaded (for display).
    """
    runner = WorkerRunner(
        storage=storage,
        result_backend=result_backend,
        concurrency=concurrency,
        app_module=app_module,
    )
    runner.run()


__all__ = ["run_worker", "WorkerRunner"]
