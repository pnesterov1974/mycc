from sqlalchemy import inspect, engine

from db_connection import DBConnection as dbc


DB_CONNECTION_NAME = 'DWH'
DB_SCHEMAS = [
    '1', '2'
]

def for_each_schema_in_db(e: engine):
    insp = inspect(e)
    for s in insp.get_schema_names():
        yield s

def for_each_table_in_schema(e: engine, schema_name: str):
    insp = inspect(e)
    for t in insp.get_table_names(schema_name):
        yield t

def work():
    e = dbc.get_sqla_engine('kladr')
    for s in DB_SCHEMAS:
        for t in for_each_table_in_schema(e, s):
            dwh_table_name = t   # fullname with schema
            src_table_name = ''  # TODO

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
