import os
import yaml
import glob
import click
from utils import substitute_env_vars

REQUIRED_SNOWFLAKE_KEYS = ["user", "password", "account", "warehouse", "database", "schema"]

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

def validate_master_config(master_config):
    valid = True
    if "snowflake" not in master_config:
        click.secho("ERR: 'snowflake' key is missing in master configuration.", fg="red")
        valid = False
    else:
        creds = master_config["snowflake"]
        for key in REQUIRED_SNOWFLAKE_KEYS:
            if key not in creds:
                click.secho(f"ERR: Snowflake credential '{key}' is missing.", fg="red")
                valid = False
    return valid

def validate_object_definitions(master_config, obj_type):
    valid = True
    if obj_type not in master_config:
        return valid
    for entry in master_config[obj_type]:
        # Inline definitions
        if obj_type in entry:
            for obj in entry[obj_type]:
                if "name" not in obj:
                    click.secho(f"ERR: {obj_type} definition missing 'name': {obj}", fg="red")
                    valid = False
                if "query" not in obj:
                    click.secho(f"ERR: {obj_type} definition for '{obj.get('name', 'unknown')}' missing 'query'.", fg="red")
                    valid = False
        # File/folder/pattern reference
        elif "file" in entry or "folder" in entry or "pattern" in entry:
            if "file" in entry:
                pattern = os.path.join("config", entry["file"])
            elif "folder" in entry:
                pattern = os.path.join("config", entry["folder"], "*.yaml")
            elif "pattern" in entry:
                pattern = os.path.join("config", entry["pattern"])
            configs = load_yaml_configs(pattern)
            if not configs:
                click.secho(f"ERR: No YAML files loaded for {obj_type} using pattern '{pattern}'.", fg="red")
                valid = False
            else:
                for cfg in configs:
                    if obj_type not in cfg:
                        click.secho(f"ERR: YAML file {pattern} missing key '{obj_type}'.", fg="red")
                        valid = False
                    else:
                        for obj in cfg[obj_type]:
                            if "name" not in obj:
                                click.secho(f"ERR: {obj_type} definition missing 'name': {obj}", fg="red")
                                valid = False
                            if "query" not in obj:
                                click.secho(f"ERR: {obj_type} definition for '{obj.get('name', 'unknown')}' missing 'query'.", fg="red")
                                valid = False
        else:
            click.secho(f"ERR: Unrecognized {obj_type} configuration entry: {entry}", fg="red")
            valid = False
    return valid

@click.command()
def validate():
    """Validate master configuration and object definitions."""
    master_path = os.path.join("config", "master_sf_objects.yaml")
    try:
        with open(master_path, 'r') as f:
            master_config = yaml.safe_load(f)
        master_config = substitute_env_vars(master_config)
    except Exception as e:
        click.secho(f"ERR: Failed to load master configuration: {e}", fg="red")
        return

    valid = True
    click.secho("Validating master configuration...", fg="blue", bold=True)
    if not validate_master_config(master_config):
        valid = False

    for obj_type in ["tables", "views", "tasks", "snowpipes"]:
        click.secho(f"Validating {obj_type} definitions...", fg="blue")
        if not validate_object_definitions(master_config, obj_type):
            valid = False

    if valid:
        click.secho("Validation passed. All configurations are valid.", fg="green", bold=True)
    else:
        click.secho("Validation failed. Please review the errors above.", fg="red", bold=True)

if __name__ == '__main__':
    validate()
