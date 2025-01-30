from sqlalchemy import inspect, text
from db_connection_ms import DBConnection as dbc
import pandas as pd

class DataService:

    def __init__(self, conn_name: str, sql: str):
        self._conn_name = conn_name
        self._dbengine = dbc.get_sqla_engine(conn_name)

    def __iter__(self):
        with self._dbengine.connect() as connection:
            rows = connection.execute(text(self.base_select_sql))
            for row in rows:
                yield row._mapping

    def get_df(self):
        return pd.DataFrame(self)
    
    def inspect_db(self):
        e = dbc.get_sqla_engine(self._conn_name)
        insp = inspect(e)
        for s in insp.get_schema_names():
            print(f"schema: {s}")
            for t in insp.get_table_names(schema=s):
                print(f"\tschema: {s} table: {t}")
                for c in insp.get_columns(table_name=t, schema=s):
                    print(f"\t\tcolumn: {c}")
            

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
