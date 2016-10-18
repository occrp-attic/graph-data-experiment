import os
import yaml


DATA_PAGE = 10000


def resolve_includes(file_path, data):
    """Handle include statements in the configuration file."""
    if isinstance(data, (list, tuple, set)):
        data = [resolve_includes(file_path, i) for i in data]
    elif isinstance(data, dict):
        include_paths = data.pop('include', [])
        if not isinstance(include_paths, (list, tuple, set)):
            include_paths = [include_paths]
        for include_path in include_paths:
            dir_prefix = os.path.dirname(file_path)
            include_path = os.path.relpath(include_path, dir_prefix)
            data.update(load_config_file(include_path))
        for key, value in data.items():
            data[key] = resolve_includes(file_path, value)
    return data


def load_config_file(file_path):
    """Load a YAML (or JSON) model configuration file."""
    # How to prevent configurations from becoming too big:
    # Option 0: YAML master file
    # Option 1: Directory of YAML
    # Option 2: YAML walker
    # Option 3: YAML index list
    file_path = os.path.abspath(file_path)
    with open(file_path, 'r') as fh:
        data = yaml.load(fh)
    return resolve_includes(file_path, data)
