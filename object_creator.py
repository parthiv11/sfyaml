import snowflake.connector

def execute_query(cursor, query, dry_run=False):
    if dry_run:
        print(f"[Dry Run] Would execute: {query}")
        return
    try:
        cursor.execute(query)
        print(f"Executed: {query}")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error executing query: {e}")
        cursor.connection.rollback()
        raise

def create_objects(cursor, objects, obj_type, dry_run=False):
    """
    Executes DDL statements for the given objects.
    Each object must contain a 'name' and a 'query' field.
    """
    for obj in objects:
        if "name" not in obj:
            print(f"Error: {obj_type} definition missing 'name': {obj}")
            continue
        if "query" not in obj:
            print(f"Error: {obj_type} definition for '{obj.get('name')}' missing 'query'.")
            continue
        query = obj["query"]
        execute_query(cursor, query, dry_run)
        print(f"{obj_type.capitalize()} '{obj['name']}' created.")
