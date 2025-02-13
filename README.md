
# Snowflake & DBT CLI Tool

A command-line tool to manage your Snowflake objects and DBT transformations using YAML configurations. This tool supports creating, validating, and rolling back objects in Snowflake with robust logging, dry-run simulation, environment variable substitution, and automatic transaction rollback on error.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Create (apply)](#create-apply)
  - [Validate Configuration](#validate-configuration)
  - [Rollback Objects](#rollback-objects)
  - [Run DBT Transformations](#run-dbt-transformations)
  - [Check Connectivity](#check-connectivity)
- [Automatic Rollback & Transaction Control](#automatic-rollback--transaction-control)
- [Logging & Dry-Run Mode](#logging--dry-run-mode)
- [Advanced Topics](#advanced-topics)
- [License](#license)

---

## Overview

This CLI tool automates the management of Snowflake objects—tables, views, tasks, and snowpipes—using modular YAML configuration files. It supports:

- **Declarative configuration:** Define objects inline or reference external YAML files via file, folder, or glob patterns.
- **Environment variable substitution:** Use the `${env:VAR_NAME}` syntax in YAML to inject environment-specific values.
- **Transactional execution:** All object creations run in a single transaction with automatic rollback on error.
- **Dry-run mode:** Simulate object creation and rollback to preview changes without modifying your environment.
- **Intuitive logging:** Color-coded and concise log messages indicate each operation’s status.
- **Validation & Rollback Commands:** Validate your YAML configuration and rollback objects if needed.

---

## Features

- **Modular YAML Configuration:** Organize Snowflake objects in separate YAML files or define them inline.
- **Flexible Credential Management:** Specify Snowflake credentials in the master configuration or via environment variables.
- **Environment Variable Substitution:** Dynamically substitute values in your YAML using the `${env:VAR_NAME}` syntax.
- **Automatic Transaction Management:** Automatic rollback of all changes if any error occurs during object creation.
- **Dry Run Mode:** Preview DDL statements and object creation without executing changes.
- **Robust Logging:** Color-coded log messages (green, blue, yellow, red) for clear, real-time feedback.
- **Validation Command:** Validate your YAML configuration for required fields and proper formatting.
- **Rollback Command:** Drop all objects defined in your configuration (with dry-run preview) to quickly revert changes.
- **Additional Commands:** Run DBT transformations and check connectivity to Snowflake.

---

## Project Structure

```
project/
├── cli.py                    # CLI entry point.
├── commands/                 
│   ├── __init__.py           
│   ├── create.py             # Command to create objects in Snowflake.
│   ├── dbt.py                # Command to run DBT transformations.
│   ├── check.py              # Command to check Snowflake connectivity.
│   ├── validate.py           # Command to validate YAML configuration.
│   └── rollback.py           # Command to drop objects (rollback).
├── config/                   
│   ├── master_sf_objects.yaml  # Master configuration (credentials & object definitions).
│   ├── tables/              # Table definitions.
│   ├── views/               # View definitions.
│   ├── tasks/               # Task definitions.
│   └── snowpipes/           # Snowpipe definitions.
├── snowflake_connector.py    # Module for Snowflake connection.
├── object_creator.py         # Module for executing SQL commands with transaction and logging.
├── utils.py                  # Utility functions (env var substitution, etc.).
├── requirements.txt          # Project dependencies.
└── README.md                 # This documentation.
```

---

## Prerequisites

- Python 3.7+
- A valid Snowflake account with appropriate privileges.
- Snowflake credentials (either in the master YAML file or as environment variables).
- [DBT](https://docs.getdbt.com/) and Git (if using DBT transformations).
- [Click](https://click.palletsprojects.com/) for CLI support (installed via `requirements.txt`).

---

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://your_repo_url.git
   cd project
   ```

2. **Create and activate a virtual environment:**

   ```bash
   python -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

---

## Configuration

Edit the master configuration file at `config/master_sf_objects.yaml` to include:

- **Snowflake Credentials:**

  ```yaml
  snowflake:
    user: "your_username"
    password: "your_password"
    account: "your_account"
    warehouse: "your_warehouse"
    database: "your_database"
    schema: "your_default_schema"
  ```

- **Object Definitions:**

  Define objects for tables, views, tasks, and snowpipes. You can either provide inline definitions:

  ```yaml
  tables:
    - tables:
        - name: "inline_employee"
          query: |
            CREATE TABLE PUBLIC.inline_employee (
              id VARCHAR,
              name VARCHAR
            );
  ```

  Or reference external YAML files, folders, or glob patterns:

  ```yaml
  tables:
    - file: "tables/*.yaml"
  views:
    - file: "views/*.yaml"
  tasks:
    - file: "tasks/*.yaml"
  snowpipes:
    - file: "snowpipes/*.yaml"
  ```

- **(Optional) DBT Configuration:**

  DBT configurations can be included for future use. They are currently used in the `dbt_run` command.

  ```yaml
  airbyte_connections:
    - name: "employee_connection"
      raw_schema: "raw"
      info_schema: "info"
      dbt:
        git_url: "https://github.com/yourorg/dbt-employee-models.git"
        branch: "main"
        models:
          - name: "employee_transform"
            raw_table: "employee"
            info_table: "employee_info"
            transformation: |
              SELECT 
                CAST(id AS INT) AS id, 
                UPPER(name) AS name, 
                CURRENT_TIMESTAMP() AS transformed_at
              FROM raw.employee
  ```

- **Environment Variable Substitution:**

  You can use `${env:VAR_NAME}` in your YAML to inject values from environment variables.

---

## Usage

### Create (apply)

Create objects in Snowflake based on your YAML configuration. The command runs in a single transaction and automatically rolls back on error.

- **Dry Run (Preview only):**

  ```bash
  python cli.py apply --dry-run
  ```

- **Apply Changes:**

  ```bash
  python cli.py apply
  ```

### Validate Configuration

Check that your master configuration and all referenced YAML files include the required fields and are properly formatted.

```bash
python cli.py validate
```

### Rollback Objects

Drop all objects defined in your configuration. Use `--dry-run` to preview DROP statements, and `--confirm` to execute them.

- **Preview DROP Statements:**

  ```bash
  python cli.py rollback --dry-run
  ```

- **Execute Rollback (Use with Caution):**

  ```bash
  python cli.py rollback --confirm
  ```

### Run DBT Transformations

Execute DBT transformations as specified in your configuration (if applicable).

```bash
python cli.py dbt_run
```

### Check Connectivity

Test connectivity to your Snowflake environment.

```bash
python cli.py check
```

---

## Automatic Rollback & Transaction Control

- **Transaction Management:**  
  All object creation operations are executed within a single transaction.  
  - **On Success:** The transaction is committed.  
  - **On Error:** The transaction is automatically rolled back to ensure consistency.

- **Dry-Run Mode:**  
  When using the `--dry-run` flag, queries are only simulated, and no changes are committed.

---

## Logging & Dry-Run Mode

- **Color-Coded Logs:**  
  - **Green:** Success messages and dry-run simulation details.
  - **Blue:** Informational messages (e.g., connectivity, query execution).
  - **Yellow:** Warnings (e.g., object already exists, missing stage).
  - **Red:** Error messages (e.g., query failures, validation errors).

- **Dry-Run Prefix:**  
  All log messages during a dry-run are clearly prefixed with `[DRY RUN]` so that you can differentiate simulated actions from real ones.

---

## Advanced Topics

- **Enhanced Logging:**  
  Consider integrating Python's `logging` module for multi-target logging (console, file, etc.) if needed.

- **Schema Validation:**  
  Tools like Cerberus or jsonschema can be used for additional configuration validation.

- **Unit Testing:**  
  Implement tests with pytest to ensure that changes to the configuration or code do not break expected behaviors.

- **Retry Logic:**  
  Use libraries such as tenacity to add retry mechanisms for transient errors.

---

## License

This project is licensed under the MIT License.

