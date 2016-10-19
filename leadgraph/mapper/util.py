

def ensure_list(obj):
    """Make the returned object a list, otherwise wrap as single item."""
    if obj is None:
        return []
    if not isinstance(obj, (list, tuple, set)):
        return [obj]
    return obj


def dict_list(data, *keys):
    """Get an entry as a list from a dict. Provide a fallback key."""
    for key in keys:
        if key in data:
            return ensure_list(data[key])
    return []
