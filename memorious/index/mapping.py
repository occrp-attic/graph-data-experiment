
ENTITY_MAPPING = {
    "_all": {
        "enabled": True
    },
    "dynamic_templates": [
        {
            "fields": {
                "match": "properties.*",
                "mapping": {
                    "type": "string",
                    "index": "analyzed"
                }
            }
        }
    ],
    "date_detection": False,
    "properties": {
        "name": {"type": "string", "index": "analyzed"},
        "schema": {"type": "string", "index": "not_analyzed"},
        "schemata": {"type": "string", "index": "not_analyzed"},
        "dataset": {"type": "string", "index": "not_analyzed"},
        "groups": {"type": "string", "index": "not_analyzed"},
        "text": {"type": "string", "index": "analyzed"},
        "fingerprints": {"type": "string", "index": "not_analyzed"},
        "countries": {"type": "string", "index": "not_analyzed"},
        "dates": {"type": "string", "index": "not_analyzed"},
        "emails": {"type": "string", "index": "not_analyzed"},
        "phones": {"type": "string", "index": "not_analyzed"},
        "addresses": {"type": "string", "index": "not_analyzed"},
        "properties": {"type": "nested"},
    }
}

LINK_MAPPING = {
    "_all": {
        "enabled": True
    },
    "dynamic_templates": [
        {
            "fields": {
                "match": "properties.*",
                "mapping": {
                    "type": "string",
                    "index": "not_analyzed"
                }
            }
        },
        {
            "fields": {
                "match": "remote.properties.*",
                "mapping": {
                    "type": "string",
                    "index": "not_analyzed"
                }
            }
        }
    ],
    "date_detection": False,
    "properties": {
        "schema": {"type": "string", "index": "not_analyzed"},
        "schemata": {"type": "string", "index": "not_analyzed"},
        "dataset": {"type": "string", "index": "not_analyzed"},
        "groups": {"type": "string", "index": "not_analyzed"},
        "text": {"type": "string", "index": "analyzed"},
        "properties": {"type": "nested"},
        "origin": {
            "type": "object",
            "properties": {
                "id": {"type": "string", "index": "not_analyzed"},
                "fingerprints": {"type": "string", "index": "not_analyzed"}
            }
        },
        "remote": {
            "type": "object",
            "include_in_all": True,
            "properties": ENTITY_MAPPING.get('properties')
        }
    }
}


CROSSREF_MAPPING = {
    "_all": {
        "enabled": False
    },
    "properties": {
        "fingerprint": {"type": "string", "index": "analyzed"},
        "datasets": {"type": "string", "index": "not_analyzed"}
    }
}
