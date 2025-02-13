import re
import snowflake.connector
import click

def execute_query(cursor, query, dry_run=False):
    if dry_run:
        click.secho(f"[DRY RUN] Execute: {query}", fg="green")
        return
    try:
        cursor.execute(query)
        click.secho("OK: Query executed.", fg="blue")
    except snowflake.connector.errors.ProgrammingError as e:
        click.secho(f"ERR: Query failed: {e}", fg="red")
        cursor.connection.rollback()
        raise

def check_object_exists(cursor, obj_type, object_name):
    """Check if an object exists in Snowflake."""
    query_map = {
        "table": f"SHOW TABLES LIKE '{object_name.upper()}'",
        "view": f"SHOW VIEWS LIKE '{object_name.upper()}'",
        "task": f"SHOW TASKS LIKE '{object_name.upper()}'",
        "snowpipe": f"SHOW PIPES LIKE '{object_name.upper()}'"
    }
    query = query_map.get(obj_type)
    if query:
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return len(results) > 0
        except Exception as e:
            click.secho(f"[{obj_type.upper()}] ERR: Checking '{object_name}' failed: {e}", fg="red")
            return False
    return False

def check_stage_exists(cursor, stage_name):
    """Check if a stage exists in Snowflake."""
    query = f"SHOW STAGES LIKE '{stage_name.upper()}'"
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return len(results) > 0
    except Exception as e:
        click.secho(f"[STAGE] ERR: Failed to check stage '{stage_name}': {e}", fg="red")
        return False

def extract_stage_from_query(query):
    """
    Extracts a stage name from a query by searching for the pattern 'FROM @<stage_name>'.
    Returns the stage name if found, otherwise None.
    """
    match = re.search(r"FROM\s+@(\S+)", query, re.IGNORECASE)
    if match:
        stage_name = match.group(1).rstrip(";")
        return stage_name
    return None

def create_objects(cursor, objects, obj_type, dry_run=False):
    """
    Process object definitions:
    - Prefix log messages with [OBJTYPE].
    - For snowpipes, check if the associated stage exists:
        * If 'stage' is not provided, extract it from the query.
    - If an object exists, log a yellow warning.
    - If not, log in green that it will be/has been created.
    - In dry-run mode, prefix messages with [DRY RUN].
    - Log any errors in red.
    """
    for obj in objects:
        if "name" not in obj:
            click.secho(f"[{obj_type.upper()}] ERR: Missing 'name'.", fg="red")
            continue
        if "query" not in obj:
            click.secho(f"[{obj_type.upper()}] ERR: '{obj.get('name')}' missing 'query'.", fg="red")
            continue
        
        name = obj["name"]
        query = obj["query"]
        
        # For snowpipe objects, ensure stage is defined or extract from query.
        if obj_type == "snowpipe":
            stage = obj.get("stage")
            if not stage:
                stage = extract_stage_from_query(query)
                if stage:
                    click.secho(f"[SNOWPIPE] INFO: Extracted stage '{stage}' from query.", fg="blue")
                else:
                    click.secho(f"[SNOWPIPE] WARN: No stage specified or found in query for '{name}'. Skipping.", fg="yellow")
                    continue
            if check_stage_exists(cursor, stage):
                click.secho(f"[SNOWPIPE] OK: Stage '{stage}' exists.", fg="green")
            else:
                click.secho(f"[SNOWPIPE] WARN: Stage '{stage}' does not exist. Skipping '{name}'.", fg="yellow")
                continue

        # Check if the object already exists.
        try:
            exists = check_object_exists(cursor, obj_type, name)
        except Exception as e:
            click.secho(f"[{obj_type.upper()}] ERR: Existence check for '{name}' failed: {e}", fg="red")
            continue
        
        if exists:
            click.secho(f"[{obj_type.upper()}] WARN: '{name}' already exists. Skipping.", fg="yellow")
        else:
            if dry_run:
                click.secho(
                    f"[DRY RUN] [{obj_type.upper()}] '{name}' will be created.\nDDL: {query}",
                    fg="green",
                    bold=True
                )
            else:
                try:
                    execute_query(cursor, query, dry_run)
                    click.secho(f"[{obj_type.upper()}] OK: '{name}' created.", fg="green")
                except Exception as e:
                    click.secho(f"[{obj_type.upper()}] ERR: Failed to create '{name}': {e}", fg="red")
