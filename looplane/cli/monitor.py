import asyncio
import sys
import termios
import tty
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from looplane.result.base import AbstractResultBackend
from looplane.storage.base import StorageBackend


class Monitor:
    """Real-time terminal UI for monitoring task execution.

    Displays a Rich-based live table showing task results including
    PID, task ID, function name, success status, timestamps, and duration.

    Supports pagination with keyboard navigation (j/k or arrows).
    """

    def __init__(
        self,
        storage: StorageBackend,
        result_backend: AbstractResultBackend,
        refresh_interval: float = 1.0,
        page_size: int = 15,
    ):
        self.storage = storage
        self.result_backend = result_backend
        self.refresh_interval = refresh_interval
        self.page_size = page_size
        self.current_page = 0
        self.total_pages = 1
        self.total_items = 0
        self.console = Console()
        self._running = True

    def _format_datetime(self, dt: Optional[datetime]) -> str:
        """Format datetime for display."""
        if dt is None:
            return "[dim]-[/dim]"
        return dt.strftime("%H:%M:%S")

    def _build_unified_table(self, pending_tasks: list, task_results: list) -> Table:
        """Build a unified table showing both pending tasks and completed results."""
        table = Table(title="Tasks", border_style="blue", show_footer=False)
        table.add_column("ID", style="dim", width=8, no_wrap=True)
        table.add_column("Name", min_width=12, no_wrap=True)
        table.add_column("Status", width=9, no_wrap=True)
        table.add_column("Att", width=5, no_wrap=True)
        table.add_column("Fetched", width=10, no_wrap=True)
        table.add_column("Started", width=10, no_wrap=True)
        table.add_column("Finished", width=10, no_wrap=True)
        table.add_column("Duration", width=9, no_wrap=True)
        table.add_column("Error", max_width=25, no_wrap=True, overflow="ellipsis")

        # Combine all items with a sort key (newest first)
        items = []

        # Add pending tasks with their created_at for sorting
        for task in pending_tasks:
            items.append(("pending", task.created_at, task))

        # Add completed results with their finished_at for sorting
        for result in task_results:
            items.append(("result", result.finished_at, result))

        # Sort by timestamp descending (newest first)
        items.sort(key=lambda x: x[1], reverse=True)

        # Calculate pagination
        self.total_items = len(items)
        self.total_pages = max(1, (self.total_items + self.page_size - 1) // self.page_size)

        # Ensure current_page is valid
        if self.current_page >= self.total_pages:
            self.current_page = self.total_pages - 1
        if self.current_page < 0:
            self.current_page = 0

        # Get current page items
        start_idx = self.current_page * self.page_size
        end_idx = start_idx + self.page_size
        page_items = items[start_idx:end_idx]

        # Add rows for current page
        for item_type, _, item in page_items:
            if item_type == "pending":
                task = item
                status = str(task.status)
                if status == "PENDING":
                    status_str = "[yellow]PENDING[/yellow]"
                elif status == "RUNNING":
                    status_str = "[blue]RUNNING[/blue]"
                else:
                    status_str = f"[dim]{status}[/dim]"

                # Format attempt with color for retries
                attempt_str = f"{task.attempt}/{task.max_retries}"
                if task.attempt > 1:
                    attempt_str = f"[yellow]{attempt_str}[/yellow]"

                table.add_row(
                    task.id[:8],
                    task.func_name or "-",
                    status_str,
                    attempt_str,
                    self._format_datetime(task.created_at),
                    "[dim]-[/dim]",
                    "[dim]-[/dim]",
                    "[dim]-[/dim]",
                    "[dim]-[/dim]",
                )
            else:
                result = item
                duration = f"{result.duration:.2f}s" if result.duration else "-"

                # Format status based on success
                if result.success:
                    status_str = "[green]SUCCESS[/green]"
                else:
                    status_str = "[red]FAILED[/red]"

                # Format attempt with color for retries
                attempt_str = f"{result.attempt}/{result.max_retries}"
                if result.is_retry:
                    attempt_str = f"[yellow]{attempt_str}[/yellow]"

                # Truncate error message
                error = "-"
                if result.error:
                    error = f"[red]{result.error[:25]}{'...' if len(result.error) > 25 else ''}[/red]"

                table.add_row(
                    result.task_id[:8],
                    result.func_name,
                    status_str,
                    attempt_str,
                    self._format_datetime(result.fetched_from_storage_at),
                    self._format_datetime(result.started_at),
                    self._format_datetime(result.finished_at),
                    duration,
                    error,
                )

        if not page_items:
            table.add_row(
                "[dim]-[/dim]",
                "[dim]No tasks[/dim]",
                "[dim]-[/dim]",
                "[dim]-[/dim]",
                "[dim]-[/dim]",
                "[dim]-[/dim]",
                "[dim]-[/dim]",
                "[dim]-[/dim]",
                "[dim]-[/dim]",
            )

        return table

    def _build_footer(self) -> Text:
        """Build pagination footer."""
        text = Text()
        text.append("Page ", style="dim")
        text.append(f"{self.current_page + 1}/{self.total_pages}", style="bold cyan")
        text.append(f" ({self.total_items} items)", style="dim")
        text.append("  |  ", style="dim")
        text.append("←/k", style="bold yellow")
        text.append(" prev  ", style="dim")
        text.append("→/j", style="bold yellow")
        text.append(" next  ", style="dim")
        text.append("q", style="bold red")
        text.append(" quit", style="dim")
        return text

    async def _render_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3),
        )

        layout["header"].update(
            Panel("[bold green]Looplane Task Monitor[/bold green]", border_style="blue")
        )

        # Retrieve pending tasks and completed results
        pending_tasks = await self.storage.fetch_batch(1000)
        task_results = await self.result_backend.results()

        # Build unified table
        layout["main"].update(self._build_unified_table(pending_tasks, task_results))

        # Build footer with pagination info
        layout["footer"].update(
            Panel(self._build_footer(), border_style="dim")
        )

        return layout

    def _get_key(self) -> Optional[str]:
        """Non-blocking key read."""
        import select

        # Only try to read if stdin is a TTY
        if not sys.stdin.isatty():
            return None

        # Check if input is available
        if select.select([sys.stdin], [], [], 0)[0]:
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                ch = sys.stdin.read(1)
                # Handle escape sequences (arrow keys)
                if ch == '\x1b':
                    if select.select([sys.stdin], [], [], 0.1)[0]:
                        ch2 = sys.stdin.read(1)
                        if ch2 == '[':
                            if select.select([sys.stdin], [], [], 0.1)[0]:
                                ch3 = sys.stdin.read(1)
                                if ch3 == 'D':  # Left arrow
                                    return 'left'
                                elif ch3 == 'C':  # Right arrow
                                    return 'right'
                                elif ch3 == 'A':  # Up arrow
                                    return 'up'
                                elif ch3 == 'B':  # Down arrow
                                    return 'down'
                    return 'esc'
                return ch
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return None

    def _handle_input(self) -> bool:
        """Handle keyboard input. Returns False if should quit."""
        key = self._get_key()
        if key is None:
            return True

        if key in ('q', 'Q', '\x03'):  # q or Ctrl+C
            return False
        elif key in ('j', 'right', 'down', ' '):  # Next page
            if self.current_page < self.total_pages - 1:
                self.current_page += 1
        elif key in ('k', 'left', 'up'):  # Previous page
            if self.current_page > 0:
                self.current_page -= 1
        elif key == 'g':  # First page
            self.current_page = 0
        elif key == 'G':  # Last page
            self.current_page = self.total_pages - 1

        return True

    async def run(self):
        # Save terminal settings if available
        old_settings = None
        is_tty = sys.stdin.isatty()

        if is_tty:
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)

        try:
            with Live(auto_refresh=False, console=self.console) as live:
                while self._running:
                    # Handle input
                    if not self._handle_input():
                        break

                    layout = await self._render_layout()
                    live.update(layout, refresh=True)
                    await asyncio.sleep(self.refresh_interval)
        finally:
            # Restore terminal settings if they were saved
            if old_settings is not None:
                fd = sys.stdin.fileno()
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


def run_monitor(storage: StorageBackend, result_backend: AbstractResultBackend):
    """Starts the task monitor using the provided storage and result backend."""
    monitor = Monitor(storage=storage, result_backend=result_backend)
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        pass


__all__ = [
    "run_monitor",
]
