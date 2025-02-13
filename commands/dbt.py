import os
import yaml
import subprocess
from utils import substitute_env_vars

def read_yaml(file_path):
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    return substitute_env_vars(data)

def run_dbt_command():
    master_path = os.path.join('config', 'master_sf_objects.yaml')
    master_config = read_yaml(master_path)
    
    # Process DBT configurations if present (this example will run for each DBT block found)
    if "airbyte_connections" in master_config:
        for connection in master_config["airbyte_connections"]:
            print(f"Processing DBT for connection: {connection.get('name')}")
            dbt_config = connection.get("dbt")
            if dbt_config:
                git_url = dbt_config.get("git_url")
                branch = dbt_config.get("branch", "main")
                if not git_url:
                    print(f"Error: DBT configuration for {connection.get('name')} is missing 'git_url'. Skipping...")
                    continue
                
                temp_dir = '/tmp/dbt_repo'
                # Remove any previous clone
                subprocess.run(['rm', '-rf', temp_dir])
                
                print(f"Cloning DBT repository from {git_url} (branch: {branch}) to {temp_dir}")
                result = subprocess.run(['git', 'clone', '-b', branch, git_url, temp_dir])
                if result.returncode != 0:
                    print("Error cloning DBT repository.")
                    continue
                
                print("Running 'dbt run' in the cloned repository...")
                result = subprocess.run(['dbt', 'run'], cwd=temp_dir)
                if result.returncode != 0:
                    print("DBT run failed.")
                else:
                    print("DBT run completed successfully.")
            else:
                print(f"No DBT configuration found for connection {connection.get('name')}.")
    else:
        print("No DBT configuration found in master file.")
