import os
import yaml
import glob
from snowflake_connector import create_snowflake_connection
from object_creator import create_objects, execute_query

def read_yaml(file_path):
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

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

def create_snowflake_objects(dry_run=False):
    master_path = os.path.join('config', 'master_sf_objects.yaml')
    master_config = read_yaml(master_path)

    # Create Snowflake connection using credentials from master config (if provided) or env vars.
    conn = create_snowflake_connection(master_config)
    cursor = conn.cursor()

    try:
        # Process Snowflake objects for each type
        for obj_type in ['tables', 'views', 'tasks', 'snowpipes']:
            if obj_type in master_config:
                for entry in master_config[obj_type]:
                    file_pattern = entry.get('file')
                    if not file_pattern:
                        print(f"Error: Missing 'file' key in {obj_type} configuration.")
                        continue
                    # Prepend config directory if needed
                    full_pattern = os.path.join('config', file_pattern)
                    configs = load_yaml_configs(full_pattern)
                    for config in configs:
                        if obj_type not in config:
                            print(f"Warning: No '{obj_type}' key found in file matching {full_pattern}. Skipping.")
                            continue
                        create_objects(cursor, config.get(obj_type, []), obj_type[:-1], dry_run)

        # Process Airbyte connections (create required schemas)
        if 'airbyte_connections' in master_config:
            for connection in master_config['airbyte_connections']:
                if 'name' not in connection:
                    print("Error: Airbyte connection configuration missing 'name'. Skipping.")
                    continue
                raw_schema = connection.get('raw_schema')
                info_schema = connection.get('info_schema')
                if not raw_schema or not info_schema:
                    print(f"Error: Airbyte connection {connection.get('name')} missing required schema configuration (raw_schema/info_schema). Skipping.")
                    continue
                print(f"Processing Airbyte connection: {connection.get('name')}")
                for schema in [raw_schema, info_schema]:
                    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema}"
                    execute_query(cursor, create_schema_query, dry_run)
    except Exception as e:
        print(f"Error during object creation: {e}")
        cursor.connection.rollback()
    finally:
        cursor.close()
        conn.close()
