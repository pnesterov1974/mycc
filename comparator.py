from sqlalchemy import text

from db_connection_ms import DBConnection as dbc 

class Comparator:
    # TODO: DWH->TRG

    def __init__(self, src_select_object, dwh_select_object):
        self._src_select_object = src_select_object
        self._dwh_select_object = dwh_select_object

    def _get_src_total_reccount(self):
        dbe = dbc.get_sqla_engine("Pifagor2UkDev")
        sql = self._src_select_object.total_reccount_in_base_sql
        with dbe.connect() as conn:
            cnt = conn.execute(text(sql)).scalar()
            return cnt
        
    def _get_trg_total_reccount(self):
        dbe = dbc.get_sqla_engine("DWH")
        sql = self._dwh_select_object.total_reccount_in_base_sql
        with dbe.connect() as conn:
            cnt = conn.execute(text(sql)).scalar()
            return cnt
        
    def _get_trg_total_deduped_reccount(self):
        dbe = dbc.get_sqla_engine("DWH")
        sql = self._dwh_select_object.total_reccount_in_dedup_sql
        with dbe.connect() as conn:
            cnt = conn.execute(text(sql)).scalar()
            return cnt

    def compare_total_record_count(self) -> dict:
        cnt1 = self._get_src_total_reccount()
        cnt2 = self._get_trg_total_reccount()
        cnt3 = self._get_trg_total_deduped_reccount()
        return {
            "src_total_record_count": cnt1,
            "trg_total_record_count": cnt2,
            "trg_deduped_record_count": cnt3
        }
            

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
