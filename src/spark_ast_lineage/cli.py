import argparse
import logging
from rich.console import Console
from spark_ast_lineage.analyzer.pyspark_tables_extractor import PysparkTablesExtractor

# Initialize the rich console
console = Console()

# Set up logging for verbosity
logging.basicConfig(level=logging.INFO)


def print_table_details(detailed_info):
    """Helper function to print the detailed table information"""
    for entry in detailed_info:
        # Add a line separator between entries
        console.print("\n" + "=" * 50)

        # Print each entry in a structured format
        console.print(f"[bold green]Table:[/bold green] {entry['table']}")
        console.print(f"[cyan]Extractor:[/cyan] {entry['extracted_by']}")
        console.print(f"[magenta]Line Number:[/magenta] {entry['line_number']}")
        console.print(f"[yellow]Operator Applied:[/yellow] {entry['operator_applied']}")
        console.print(
            f"[blue]Processed variables:[/blue] {entry['processed_variables']}"
        )
        console.print(f"[orange]Code Fragment:[/orange] {entry['code_fragment']}")

        console.print("=" * 50)


def main():
    parser = argparse.ArgumentParser(
        description="Extract table names from PySpark code"
    )
    parser.add_argument("file", type=str, help="The Python file to analyze")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output with extraction details",
    )

    args = parser.parse_args()

    # Read the content of the file
    try:
        with open(args.file, "r") as file:
            code = file.read()
    except FileNotFoundError:
        console.print(f"[red]Error: The file '{args.file}' was not found.[/red]")
        return

    # Extract tables from code with optional verbose mode
    if args.verbose:
        tables, detailed_info = PysparkTablesExtractor.extract_tables_from_code(
            code, verbose=True
        )

        # Print verbose output header
        console.print(f"\n[bold yellow]{'Verbose Output':^50}[/bold yellow]")
        console.print("=" * 50)

        # Print detailed extraction info
        print_table_details(detailed_info)
    else:
        tables = PysparkTablesExtractor.extract_tables_from_code(code)

        # Print extracted tables
        console.print("\n[bold cyan]Extracted Tables:[/bold cyan]")
        console.print("=" * 30)

        for table in tables:
            console.print(f"[yellow]- {table}[/yellow]")


if __name__ == "__main__":
    main()
