from sqlalchemy.types import *


def get_schema(table_name):
    schemas = {
        "control": {
            "id": Integer,
            "name": String(64),
            "salary": DECIMAL(15, 4),
            "notes": JSON
        }
    }

    return schemas[table_name]
