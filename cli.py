import click
from commands.create import create_snowflake_objects
from commands.dbt import run_dbt_command
from commands.check import check_connectivity

@click.group()
def cli():
    """CLI tool for managing Snowflake objects and DBT transformations."""
    pass

@cli.command()
@click.option('--dry-run', is_flag=True, help='Preview SQL commands without executing them.')
def apply(dry_run):
    """Apply Snowflake objects creation as per YAML configuration."""
    create_snowflake_objects(dry_run)

@cli.command()
def dbt_run():
    """Run DBT transformations as specified in the YAML configuration."""
    run_dbt_command()

@cli.command()
def check():
    """Check connectivity to Snowflake."""
    check_connectivity()

if __name__ == '__main__':
    cli()
