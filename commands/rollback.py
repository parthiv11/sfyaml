import os
import yaml
import glob
import click
from utils import substitute_env_vars
from snowflake_connector import create_snowflake_connection

def read_yaml(file_path):
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    return substitute_env_vars(data)

def load_yaml_configs(path_pattern):
    if os.path.isdir(path_pattern):
        files = glob.glob(os.path.join(path_pattern, '*.yaml'))
    elif '*' in path_pattern or '?' in path_pattern:
        files = glob.glob(path_pattern)
    else:
        files = [path_pattern]
    configs = []
    for file in files:
        try:
            configs.append(read_yaml(file))
        except Exception as e:
            click.secho(f"ERR: Failed to load YAML file {file}: {e}", fg="red")
    return configs

def get_object_definitions(master_config, obj_type):
    definitions = []
    if obj_type not in master_config:
        return definitions
    for entry in master_config[obj_type]:
        if "file" in entry:
            pattern = os.path.join("config", entry["file"])
            configs = load_yaml_configs(pattern)
            for config in configs:
                if obj_type in config:
                    definitions.extend(config[obj_type])
        elif "folder" in entry:
            folder = os.path.join("config", entry["folder"])
            pattern = os.path.join(folder, "*.yaml")
            configs = load_yaml_configs(pattern)
            for config in configs:
                if obj_type in config:
                    definitions.extend(config[obj_type])
        elif "pattern" in entry:
            pattern = os.path.join("config", entry["pattern"])
            configs = load_yaml_configs(pattern)
            for config in configs:
                if obj_type in config:
                    definitions.extend(config[obj_type])
        elif obj_type in entry:
            definitions.extend(entry[obj_type])
        else:
            click.secho(f"ERR: Unrecognized {obj_type} configuration entry: {entry}", fg="red")
    return definitions

def drop_statement(obj_type, name):
    statements = {
        "table": f"DROP TABLE IF EXISTS {name}",
        "view": f"DROP VIEW IF EXISTS {name}",
        "task": f"DROP TASK IF EXISTS {name}",
        "snowpipe": f"DROP PIPE IF EXISTS {name}"
    }
    return statements.get(obj_type.lower(), None)

def drop_objects(cursor, definitions, obj_type, dry_run=False):
    for obj in definitions:
        name = obj.get("name")
        if not name:
            click.secho(f"ERR: {obj_type} definition missing 'name'.", fg="red")
            continue
        stmt = drop_statement(obj_type, name)
        if not stmt:
            click.secho(f"ERR: Unsupported object type: {obj_type}", fg="red")
            continue
        if dry_run:
            click.secho(f"[DRY RUN] {obj_type.upper()} '{name}' would be dropped with: {stmt}", fg="green", bold=True)
        else:
            try:
                cursor.execute(stmt)
                click.secho(f"OK: {obj_type.upper()} '{name}' dropped.", fg="green")
            except Exception as e:
                click.secho(f"ERR: Failed to drop {obj_type.upper()} '{name}': {e}", fg="red")

@click.command()
@click.option('--dry-run', is_flag=True, help='Preview DROP statements without executing them.')
@click.option('--confirm', is_flag=True, help='Confirm dropping the objects.')
def rollback(dry_run, confirm):
    """
    Drop all objects defined in the configuration.

    WARNING: This will permanently remove objects from your Snowflake environment.
    Use --dry-run to preview and --confirm to execute.
    """
    master_path = os.path.join("config", "master_sf_objects.yaml")
    try:
        with open(master_path, 'r') as f:
            master_config = yaml.safe_load(f)
        master_config = substitute_env_vars(master_config)
    except Exception as e:
        click.secho(f"ERR: Failed to load master configuration: {e}", fg="red")
        return

    object_types = ["tables", "views", "tasks", "snowpipes"]
    all_definitions = {}
    for obj_type in object_types:
        definitions = get_object_definitions(master_config, obj_type)
        all_definitions[obj_type] = definitions
        click.secho(f"Found {len(definitions)} {obj_type}.", fg="blue")

    if not confirm:
        click.secho("This command will drop all objects defined in the configuration. Use --confirm to proceed.", fg="yellow", bold=True)
        return

    conn = create_snowflake_connection(master_config)
    cursor = conn.cursor()

    try:
        for obj_type in object_types:
            drop_objects(cursor, all_definitions[obj_type], obj_type[:-1], dry_run)
    except Exception as e:
        click.secho(f"ERR: Rollback encountered an error: {e}", fg="red")
    finally:
        cursor.close()
        conn.close()
        click.secho("Rollback complete.", fg="green", bold=True)

if __name__ == '__main__':
    rollback()
