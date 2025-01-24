from sqlalchemy import inspect, text
#from sqlalchemy.schema import CreateTable
import pandas as pd

class DBSelectObject:

    def __init__(self, sqa_engine, schema_name: str, table_name: str):
        self._dbengine = sqa_engine
        self._schema_name = schema_name
        self._table_name = table_name
        self._columns = None
        self._pkl = None
        self._do_inspect()
        self._select_sql = self._get_select_sql()

    def _do_inspect(self):
        insp = inspect(self._dbengine)
        self._columns = insp.get_columns(schema=self._schema_name, table_name=self._table_name)
        self._pkl = insp.get_pk_constraint(schema=self._schema_name, table_name=self._table_name)

    def _get_select_sql(self):
        select_str = 'SELECT'
        cols = ', '.join([
            d['name'] for d in self._columns
        ])
        from_str = f'FROM {self._schema_name}.{self._table_name}'
        return '\n '.join([select_str, cols, from_str])
    
    #def get_create_sql(self):
    #    md = MetaData(schema=self.schema_name)
    #    t = Table(self.table_name, md, autoload_with=self.dbengine)
    #    s = CreateTable(t.__table__).compile(self.dbengine)

    def __iter__(self):
        with self._dbengine.connect() as connection:
            rows = connection.execute(text(self._select_sql))
            for row in rows:
                yield row._mapping

    def get_df(self):
        return pd.DataFrame(self)
    
    pkl = property(lambda self: self._pkl['constrained_columns'])
    columns = property(lambda self: [c['name'] for c in self._columns])

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
