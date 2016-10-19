
ENTITY_MAPPING = {
    "_all": {
        "enabled": True
    },
    "dynamic_templates": [
        {
            "fields": {
                "match": "record.*",
                "mapping": {
                    "type": "string",
                    "index": "analyzed"
                }
            }
        },
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
        "text": {"type": "string", "index": "analyzed"},
        "fingerprints": {"type": "string", "index": "not_analyzed"},
        "countries": {"type": "string", "index": "not_analyzed"},
        "record": {"type": "nested"},
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
                "match": "record.*",
                "mapping": {
                    "type": "string",
                    "index": "not_analyzed"
                }
            }
        },
        {
            "fields": {
                "match": "properties.*",
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
        "text": {"type": "string", "index": "analyzed"},
        "fingerprints": {"type": "string", "index": "not_analyzed"},
        "entities": {"type": "string", "index": "not_analyzed"},
        "record": {"type": "nested"},
        "properties": {"type": "nested"},
        "source": {
            "type": "object",
            "properties": {
                "id": {"type": "string", "index": "not_analyzed"},
                "fingerprints": {"type": "string", "index": "not_analyzed"},
                "name": {"type": "string", "index": "not_analyzed"}
            }
        },
        "target": {
            "type": "object",
            "properties": {
                "id": {"type": "string", "index": "not_analyzed"},
                "fingerprints": {"type": "string", "index": "not_analyzed"},
                "name": {"type": "string", "index": "not_analyzed"}
            }
        }
    }
}
