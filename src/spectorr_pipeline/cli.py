# src/spectorr_pipeline/cli.py
import os
from pathlib import Path

import typer

from spectorr_pipeline import etl, mockgen

app = typer.Typer(help="Spectorr pipeline CLI")


@app.command("mockgen")
def mockgen_cmd(
    n: int = typer.Option(200, "--n", help="Number of synthetic rows"),
    raw_dir: str = typer.Option(
        os.path.expanduser(
            os.getenv("SPECTORR_DATA", "~/Documents/Projects/spectorr/spectorr-data") + "/raw"
        ),
        "--raw-dir",
        help="Raw data directory",
    ),
):
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    mockgen.generate_raw(Path(raw_dir), n=n)
    typer.echo(f"Mock data written to {raw_dir}")


@app.command("run")
def run_cmd(
    raw_dir: str = os.path.expanduser(
        os.getenv("SPECTORR_DATA", "~/Projects/spectorr-data") + "/raw"
    ),
):
    raw = Path(raw_dir)
    sources = [str(p) for p in raw.glob("*.csv")]
    df = etl.extract(sources)
    df = etl.transform(df)
    out = etl.load(df)
    typer.echo(f"Wrote: {out}")


def main():
    app()
