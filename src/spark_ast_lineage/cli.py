import argparse
import logging
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table
from rich.progress import Progress
from spark_ast_lineage.analyzer.pyspark_tables_extractor import PysparkTablesExtractor

console = Console()

logging.basicConfig(level=logging.INFO)


def print_table_details(detailed_info: list[dict]):
    """
    Helper function to print detailed information for each extracted table.

    Args:
        detailed_info (list): A list of dictionaries containing detailed information
                               about each table extracted, including variables and
                               code fragments.
    """
    for entry in detailed_info:
        console.print("\n" + "-" * 70)

        console.print(f"[bold green]Table:[/bold green] {entry['table']}")
        console.print(f"[cyan]Extractor:[/cyan] {entry['extracted_by']}")
        console.print(f"[magenta]Line Number:[/magenta] {entry['line_number']}")
        console.print(f"[yellow]Operator Applied:[/yellow] {entry['operator_applied']}")
        console.print(
            f"[blue]Processed variables:[/blue] {entry['processed_variables']}"
        )

        code = entry["code_fragment"]
        if code:
            syntax = Syntax(code, "python", theme="monokai", line_numbers=True)
            console.print("[red]Code Fragment:[/red] ", syntax)
        else:
            console.print("[orange]Code Fragment:[/orange] No code available")

        console.print("-" * 70)


def main():
    """
    Main function that parses the command-line arguments, reads the provided Python
    file, and extracts the table names using pyspark_ast_lineage module. It also handles the verbose output based on
    user input, and prints the extracted tables or detailed verbose information.
    """
    parser = argparse.ArgumentParser(
        description="Extract table names from PySpark code"
    )
    parser.add_argument(
        "file", type=str, help="The Python file to analyze (.py format)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output with extraction details",
    )

    args = parser.parse_args()

    try:
        with open(args.file, "r") as file:
            code = file.read()
    except FileNotFoundError:
        console.print(f"[red]Error: The file '{args.file}' was not found.[/red]")
        return

    if not args.file.endswith(".py"):
        console.print(f"[red]Error: The file '{args.file}' must be a .py file.[/red]")
        return

    with Progress() as progress:
        task = progress.add_task(
            f"[cyan]Extracting tables from [yellow]{args.file}[/yellow][/cyan]",
            total=100,
        )

        if args.verbose:
            tables, detailed_info = PysparkTablesExtractor.extract_tables_from_code(
                code, verbose=True
            )
        else:
            tables = PysparkTablesExtractor.extract_tables_from_code(
                code, verbose=False
            )

        progress.update(task, advance=100)

    if args.verbose:
        print_table_details(detailed_info)
    else:
        console.print("\n[bold cyan]Extracted Tables:[/bold cyan]")
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Table Name", style="dim")

        for table_name in tables:
            table.add_row(table_name)

        console.print(table)


if __name__ == "__main__":
    main()
