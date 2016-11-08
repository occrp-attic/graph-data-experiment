import os
import re
import six
import yaml
from icu import Transliterator


DATA_PAGE = 10000
WS_PATTERN = re.compile('\s+')
tr = Transliterator.createInstance('Any-Latin')


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
            include_path = os.path.join(dir_prefix, include_path)
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
        data = yaml.load(fh) or {}
    return resolve_includes(file_path, data)


def chunk_iter(iter, chunk_size):
    """Turn an iterator into a set of smaller lists."""
    chunk = []
    for item in iter:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if len(chunk):
        yield chunk


def is_list(obj):
    return isinstance(obj, (list, tuple, set))


def unique_list(lst):
    """Make a list unique, retaining order of initial appearance."""
    uniq = []
    for item in lst:
        if item not in uniq:
            uniq.append(item)
    return uniq


def clean_text(text):
    """Apply some very simple cleaning operations to a piece of text."""
    if text is None:
        return None
    text = six.text_type(text)
    text = WS_PATTERN.sub(' ', text).strip()
    if not len(text):
        return None
    return text


def latinize_text(text):
    """Transliterate a piece of text into the latin alphabet."""
    return tr.transliterate(text)


def ensure_list(obj):
    """Make the returned object a list, otherwise wrap as single item."""
    if obj is None:
        return []
    if not is_list(obj):
        return [obj]
    return obj


def dict_list(data, *keys):
    """Get an entry as a list from a dict. Provide a fallback key."""
    for key in keys:
        if key in data:
            return ensure_list(data[key])
    return []


def remove_nulls(data):
    """Remove None from an object, recursively."""
    if isinstance(data, dict):
        for k, v in data.items():
            if v is None:
                data.pop(k)
            data[k] = remove_nulls(v)
    elif is_list(data):
        data = [remove_nulls(d) for d in data if d is not None]
    return data
