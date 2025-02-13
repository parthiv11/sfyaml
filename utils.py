import re
import os

def substitute_env_vars(value):
    """
    Recursively replace ${env:VAR_NAME} in strings with the corresponding environment variable.
    """
    if isinstance(value, str):
        pattern = re.compile(r"\${env:([^}]+)}")
        def replacer(match):
            env_var = match.group(1)
            return os.environ.get(env_var, match.group(0))
        return pattern.sub(replacer, value)
    elif isinstance(value, list):
        return [substitute_env_vars(item) for item in value]
    elif isinstance(value, dict):
        return {k: substitute_env_vars(v) for k, v in value.items()}
    else:
        return value
