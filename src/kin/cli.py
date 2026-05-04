import typer
import httpx
import time
import sys
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.markdown import Markdown

app = typer.Typer(add_completion=False)
console = Console()
API_URL = "http://localhost:8000/v1"


def get_status_color(status: str) -> str:
    mapping = {
        "COMPLETED": "green",
        "RUNNING": "yellow",
        "FAILED": "red",
        "PENDING": "white",
    }
    return mapping.get(status, "white")


@app.command()
def submit(prompt: str = typer.Argument(..., help="Workflow input prompt")):
    """Submit a task and transition to tracking."""
    try:
        response = httpx.post(
            f"{API_URL}/workflows", json={"prompt": prompt}, timeout=30.0
        )
        response.raise_for_status()
        workflow_id = response.json()["workflow_id"]
        console.print(
            f"[bold blue]INFO[/bold blue] | Workflow Created: [cyan]{workflow_id}[/cyan]"
        )
        poll(workflow_id)
    except Exception as e:
        console.print(f"[red]ERROR[/red] | Could not submit: {str(e)}")
        sys.exit(1)


@app.command()
def poll(workflow_id: str):
    """Monitor execution and render final output."""
    with Live(console=console, refresh_per_second=2) as live:
        while True:
            try:
                response = httpx.get(f"{API_URL}/workflows/{workflow_id}")
                data = response.json()

                overall_status = data.get("status", "RUNNING")
                results = data.get("results", {})

                table = Table(
                    title=f"RUNNING WORKFLOW: {workflow_id}",
                    title_style="bold blue",
                    show_header=True,
                    header_style="bold cyan",
                    border_style="dim",
                )
                table.add_column("NODE_ID", width=20)
                table.add_column("AGENT", width=15)
                table.add_column("STATUS", width=12)

                for node_id, node_data in results.items():
                    # Check if the node is actually done based on our API response
                    status_str = node_data.get("status", "RUNNING")
                    table.add_row(
                        node_id,
                        node_data.get("agent_type", "N/A"),
                        f"[{get_status_color(status_str)}]{status_str}[/]",
                    )

                live.update(table)

                if overall_status in ["COMPLETED", "FAILED"]:
                    live.stop()  # Stop the live table to print the final report
                    console.print(
                        f"\n[bold green]✔[/bold green] [bold]EXECUTION_FINISHED[/bold] | Status: {overall_status}\n"
                    )

                    # Find the writer node dynamically
                    writer_node = next(
                        (
                            v
                            for v in results.values()
                            if v.get("agent_type") == "writer"
                        ),
                        None,
                    ) or next(iter(results.values()), {})
                    raw_content = writer_node.get("data", {})
                    markdown_body = (
                        raw_content.get("markdown")
                        if isinstance(raw_content, dict)
                        else None
                    )

                    if markdown_body:
                        console.print(
                            Panel(
                                Markdown(markdown_body),
                                title="[bold green]FINAL GENERATED REPORT[/bold green]",
                                border_style="green",
                                padding=(1, 2),
                            )
                        )
                        
                        total_prompt = 0
                        total_comp = 0
                        total_tokens = 0
                        for node_data in results.values():
                            out_data = node_data.get("data") or {}
                            if isinstance(out_data, dict):
                                usage = out_data.get("usage") or {}
                                total_prompt += usage.get("prompt_tokens", 0)
                                total_comp += usage.get("completion_tokens", 0)
                                total_tokens += usage.get("total_tokens", 0)
                        
                        if total_tokens > 0:
                            console.print(f"\n[dim]Token Usage | Prompt: {total_prompt} | Completion: {total_comp} | Total: {total_tokens}[/dim]\n")
                            
                    else:
                        console.print(
                            "[yellow]WARNING[/yellow] | No markdown content found in writer output."
                        )
                    break

                time.sleep(2)
            except Exception as e:
                live.update(f"[yellow]RECONNECTING... ({str(e)})[/yellow]")
                time.sleep(2)


def main():
    app()


if __name__ == "__main__":
    app()
