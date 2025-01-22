from sqlalchemy import inspect, text
#from sqlalchemy.schema import CreateTable
from db_connection import DBConnection as dbc
import pandas as pd

class DBSelectObject:

    def __init__(self, conn_name: str, schema_name: str, table_name: str):
        self.conn_name = conn_name
        self.dbengine = dbc.get_sqla_engine(conn_name)
        self.schema_name = schema_name
        self.table_name = table_name
        self.columns = None
        self.do_inspect()
        self.select_sql = self.get_select_sql()

    def do_inspect(self):
        insp = inspect(self.dbengine)
        self.columns = insp.get_columns(schema=self.schema_name, table_name=self.table_name)

    def get_select_sql(self):
        select_str = 'SELECT'
        cols = ', '.join([
            d['name'] for d in self.columns
        ])
        from_str = f'FROM {self.schema_name}.{self.table_name}'
        return '\n '.join([select_str, cols, from_str])
    
    #def get_create_sql(self):
    #    md = MetaData(schema=self.schema_name)
    #    t = Table(self.table_name, md, autoload_with=self.dbengine)
    #    s = CreateTable(t.__table__).compile(self.dbengine)

    def __iter__(self):
        with self.dbengine.connect() as connection:
            rows = connection.execute(text(self.select_sql))
            for row in rows:
                yield row._mapping

    def get_df(self):
        return pd.DataFrame(self)

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
