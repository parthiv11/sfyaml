import click
from commands.create import create_snowflake_objects
from commands.dbt import run_dbt_command
from commands.check import check_connectivity
from commands.validate import validate as validate_config
from commands.rollback import rollback as rollback_config

@click.group()
def cli():
    """Snowflake & DBT CLI Tool

    Commands:
      apply           Create Snowflake objects as per YAML configuration.
      validate        Validate your configuration files.
      rollback        Drop objects defined in the configuration (use with caution).
      dbt_run         Run DBT transformations.
      check           Test connectivity to Snowflake.
    """
    pass

@cli.command()
@click.option('--dry-run', is_flag=True, help='Preview creation without executing any queries.')
def apply(dry_run):
    """Create Snowflake objects as per YAML configuration."""
    click.secho("Starting object creation...", fg="blue", bold=True)
    create_snowflake_objects(dry_run)

@cli.command()
def validate():
    """Validate configuration files for required fields and proper formatting."""
    click.secho("Validating configuration files...", fg="blue", bold=True)
    validate_config()

@cli.command()
@click.option('--dry-run', is_flag=True, help='Show DROP statements without executing them.')
@click.option('--confirm', is_flag=True, help='Execute DROP statements to rollback objects.')
def rollback(dry_run, confirm):
    """
    Rollback changes by dropping all objects defined in the configuration.
    
    WARNING: This command will drop objects from your Snowflake environment.
    Use --dry-run to preview and --confirm to execute.
    """
    click.secho("Starting rollback...", fg="blue", bold=True)
    rollback_config(dry_run, confirm)

@cli.command()
def dbt_run():
    """Run DBT transformations as specified in the configuration."""
    click.secho("Starting DBT transformation...", fg="blue", bold=True)
    run_dbt_command()

@cli.command()
def check():
    """Test connectivity to Snowflake."""
    click.secho("Checking Snowflake connectivity...", fg="blue", bold=True)
    check_connectivity()

if __name__ == '__main__':
    cli()
