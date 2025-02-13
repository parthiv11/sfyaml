import os
import yaml
from snowflake_connector import create_snowflake_connection
from utils import substitute_env_vars

def read_yaml(file_path):
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    return substitute_env_vars(data)

def check_connectivity():
    master_path = os.path.join('config', 'master_sf_objects.yaml')
    try:
        master_config = read_yaml(master_path)
        conn = create_snowflake_connection(master_config)
        cursor = conn.cursor()
        cursor.execute('SELECT CURRENT_TIMESTAMP()')
        print("Successfully connected to Snowflake!")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
