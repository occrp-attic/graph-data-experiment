

def ensure_list(obj):
    if obj is None:
        return []
    if not isinstance(obj, (list, tuple, set)):
        return [obj]
    return obj


def dict_list(data, *keys):
    for key in keys:
        if key in data:
            return ensure_list(data[key])
    return []
