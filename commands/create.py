import os
import yaml
import glob
import click
from snowflake_connector import create_snowflake_connection
from object_creator import create_objects, execute_query
from utils import substitute_env_vars

def read_yaml(file_path):
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    return substitute_env_vars(data)

def load_yaml_configs(path_pattern):
    """
    Accepts a file, directory, or glob pattern and returns a list of YAML configurations.
    """
    if os.path.isdir(path_pattern):
        files = glob.glob(os.path.join(path_pattern, '*.yaml'))
    elif '*' in path_pattern or '?' in path_pattern:
        files = glob.glob(path_pattern)
    else:
        files = [path_pattern]
    
    configs = []
    for file in files:
        configs.append(read_yaml(file))
    return configs

def get_object_definitions(master_config, obj_type):
    """
    Processes one section (e.g., tables, views, tasks, snowpipes) from the master YAML.
    Each entry can specify a file, folder, pattern or inline definitions.
    Returns a list of object definitions (each must contain 'name' and 'query').
    """
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
                else:
                    click.secho(f"Warning: No '{obj_type}' key found in file {pattern}.", fg="yellow")
        elif "folder" in entry:
            folder = os.path.join("config", entry["folder"])
            pattern = os.path.join(folder, "*.yaml")
            configs = load_yaml_configs(pattern)
            for config in configs:
                if obj_type in config:
                    definitions.extend(config[obj_type])
                else:
                    click.secho(f"Warning: No '{obj_type}' key found in files in folder {folder}.", fg="yellow")
        elif "pattern" in entry:
            pattern = os.path.join("config", entry["pattern"])
            configs = load_yaml_configs(pattern)
            for config in configs:
                if obj_type in config:
                    definitions.extend(config[obj_type])
                else:
                    click.secho(f"Warning: No '{obj_type}' key found in file matching pattern {pattern}.", fg="yellow")
        elif obj_type in entry:
            # Inline definitions provided directly in master YAML.
            definitions.extend(entry[obj_type])
        else:
            click.secho(f"Error: Unrecognized {obj_type} configuration: {entry}", fg="red")
    return definitions

def create_snowflake_objects(dry_run=False):
    master_path = os.path.join('config', 'master_sf_objects.yaml')
    master_config = read_yaml(master_path)
    
    # Create Snowflake connection using credentials from master config or env variables.
    conn = create_snowflake_connection(master_config)
    
    # Disable autocommit so we can rollback on error.
    conn.autocommit = False
    cursor = conn.cursor()
    
    try:
        for obj_type in ['tables', 'views', 'tasks', 'snowpipes']:
            definitions = get_object_definitions(master_config, obj_type)
            if definitions:
                create_objects(cursor, definitions, obj_type[:-1], dry_run)
            else:
                click.secho(f"No {obj_type} definitions found.", fg="cyan")
        # Commit if not a dry run.
        if not dry_run:
            conn.commit()
            click.secho("All objects created successfully. Transaction committed.", fg="green", bold=True)
        else:
            click.secho("[DRY RUN] No changes were made.", fg="green", bold=True)
    except Exception as e:
        click.secho(f"ERR: Error during object creation: {e}", fg="red", bold=True)
        conn.rollback()
        click.secho("Automatic rollback executed due to error.", fg="red", bold=True)
    finally:
        cursor.close()
        conn.close()
