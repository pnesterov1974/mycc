from sqlalchemy import URL, create_engine, text, inspect

class DBConnection:

    home_connection_strings = {
        'kladr': {
            'host':'127.0.0.1',    # localhost
            'database':'kladr'
        },
        'gar': {
            'host':'127.0.0.1',    # localhost
            'database':'gar'
        },
    }

    @classmethod
    def test_connection(cls, connection_name: str, silent=False) -> bool:
        conn_str = cls.get_connection_string(connection_name)
        e = create_engine(conn_str)
        with e.connect() as connection:
            try:
                result = connection.execute(text("SELECT NOW() ct;"))
                for row in result:
                    if not silent:
                        print(row.ct)
                return True
            except:
                return False

    @classmethod       
    def get_sqla_engine(cls, connection_name: str):
        conn_str = cls.get_connection_string(connection_name)
        return create_engine(conn_str)

    @classmethod
    def get_connection_string(cls, connection_name: str) -> str:
        conn_params = DBConnection.home_connection_strings[connection_name]
        host = conn_params['host']
        database = conn_params['database']
        conn_str = cls.get_sqla_connection_string_pg(host, database)
        return conn_str

    @classmethod    
    def get_sqla_connection_string_pg(cls, host: str, database: str) -> str:
        url_object = URL.create(
            "postgresql+psycopg2",
            host=host,
            port=5432,
            database=database,
            username="postgres",
            password="my_pass",
        )
        return url_object
    
    @classmethod
    def inspect_db(cls, connection_name: str):
        e = cls.get_sqla_engine(connection_name)
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
