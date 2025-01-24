from more_itertools import ilen
from db_connection_pg import DBConnection as dbc
from db_selectobject import DBSelectObject

def process_using_iter():
    e = dbc.get_sqla_engine("kladr")
    d = DBSelectObject(e, "stg", "kladr")
    c = sum(1 for a in d)
    print(c)
    cc = ilen(x for x in d)
    print(cc)
    cc = ilen(d)
    print(cc)

def process_using_list():
    e = dbc.get_sqla_engine("kladr")
    d = DBSelectObject(e, "stg", "kladr")
    l = list(d)
    print(len(l))
    df = d.get_df()
    print(df.info())

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
