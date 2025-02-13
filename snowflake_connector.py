import snowflake.connector
import os

def create_snowflake_connection(config=None):
    # If provided in the master config, use those credentials; otherwise, fallback to env variables.
    if config and "snowflake" in config:
        creds = config["snowflake"]
        required_keys = ["user", "password", "account", "warehouse", "database", "schema"]
        missing = [k for k in required_keys if k not in creds]
        if missing:
            raise Exception(f"Missing required Snowflake credentials in master file: {missing}")
        user = creds["user"]
        password = creds["password"]
        account = creds["account"]
        warehouse = creds["warehouse"]
        database = creds["database"]
        schema = creds["schema"]
    else:
        user = os.getenv("SF_USER")
        password = os.getenv("SF_PASSWORD")
        account = os.getenv("SF_ACCOUNT")
        warehouse = os.getenv("SF_WAREHOUSE")
        database = os.getenv("SF_DATABASE")
        schema = os.getenv("SF_SCHEMA")
        if not all([user, password, account, warehouse, database, schema]):
            raise Exception("Missing required Snowflake credentials. Provide them in the master YAML file or as environment variables.")
    
    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
