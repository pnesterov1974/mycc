from collections import namedtuple
from more_itertools import ilen
from db_connection_ms import DBConnection as dbc
from sql_producer import DBSelectObject

def process_using_iter():
    e = dbc.get_sqla_engine("DWH")
    d = DBSelectObject(e, "stg_p2_ukdev_tab", "FigureRelation")
    c = sum(1 for a in d)
    print(c)
    cc = ilen(x for x in d)
    print(cc)
    cc = ilen(d)
    print(cc)

def process_using_iter_2():
    engineP2 = dbc.get_sqla_engine("Pifagor2UkDev")
    engineDWH = dbc.get_sqla_engine("DWH")

    dataP2 = DBSelectObject(engineP2, "tab", "FigureRelation")
    list_columns('Pifagor2UkDev: stg_p2_ukdev_tab.FigureRelation', dataP2)
    list_pks('Pifagor2UkDev: stg_p2_ukdev_tab.FigureRelation', dataP2)
    stat_recs('Pifagor2UkDev', dataP2)

    dataDWH = DBSelectObject(engineDWH, "stg_p2_ukdev_tab", "FigureRelation")
    list_columns('DWH: stg_p2_ukdev_tab.FigureRelation', dataDWH)
    list_pks('DWH: stg_p2_ukdev_tab.FigureRelation', dataDWH)
    stat_recs('DWH', dataDWH)

    source_pk_ntpl = namedtuple('source_pk_ntpl', dataP2.pkl)
    source_pks = [source_pk_ntpl(rec) for rec in dataP2]
    stat_pks('Pifagor2UkDev', source_pks)

    #print(str(len(dataDWH.pkl)))
    if len(dataDWH.pkl) > 0:
        target_pk_ntpl = namedtuple('target_pk_ntpl', dataDWH.pkl)
        target_pks = [target_pk_ntpl(rec) for rec in dataDWH]
        stat_pks('DWH', target_pks)
    else:
        print('У DWH нет первичного ключа')

    #source_pks = [rec['ID'] for rec in dataP2]
    #target_pks = [rec['ID'] for rec in dataDWH]

def list_columns(label: str, data):
    c = data.columns
    print(f"Колонки для {label}")
    print(c)

def list_pks(label: str, data):
    k = data.pkl
    print(f"Состав PK для {label}")
    print(k)

def stat_recs(label: str, data):
    total_recs = ilen(data)
    print(f"{label} Количество записей: {total_recs}")

def stat_pks(label: str, lpks: list):
    print(f'{label} количество первичных ключей {len(lpks)}')
    if len(lpks) > 0:
        pks_unq = list(set(lpks))
        pks_diff = len(lpks) - len(pks_unq)
        print(f'{label} количество уникальных ключей {len(lpks)}\t разница {pks_diff}')

# select count(1)--t.*
# from (
# 	select *, row_number() over(partition by [ID] order by mt_proc_id desc) as rn
# 	from [stg_p2_ukdev_tab].[FigureRelation]
# ) t
# where t.rn = 1;

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
