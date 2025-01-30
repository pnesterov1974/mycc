import pprint

from sqlalchemy import inspect, text
import sqlparse

from db_connection_ms import DBConnection as dbc
from logger_conf import logger

# if pkl is empty -> deduped sql = base sql
# rename class - it produces sqls only
class SqlProducer:

    @staticmethod
    def touch_mssql_name(name: str) -> str: #TODO Протестировать на поле с уже [] колонками
        name.replace('[', '').replace(']', '')
        return name.join(['[', ']'])

    def __init__(self, conn_name: str, schema_name: str, table_name: str, business_pkl: list =[]) -> None:
        self._connection_name = conn_name
        self._dbengine = dbc.get_sqla_engine(conn_name)
        self._schema_name = schema_name
        self._table_name = table_name
        self._business_pkl = business_pkl
        self._columns = None
        self._pkl = None
        self._do_inspect()

    def _do_inspect(self, log_start_params=True, log_final_params=True) -> None:
        insp = inspect(self._dbengine)
        self._columns = insp.get_columns(schema=self._schema_name, 
                                         table_name=self._table_name
                                        )
        for c in self._columns:
            c['name'] = SqlProducer.touch_mssql_name(c['name'])

        self._pkl = insp.get_pk_constraint(schema=self._schema_name, 
                                           table_name=self._table_name
                                          )
        for i in range(0, len(self._pkl['constrained_columns'])):
            self._pkl['constrained_columns'][i] = \
                SqlProducer.touch_mssql_name(self._pkl['constrained_columns'][i])
            
        if log_start_params:
            self._log_start_params()

        if log_final_params:
            self._log_final_params()
            
    def _log_start_params(self) -> None:
        logger.debug(f'Объект: {self._connection_name}.{self._schema_name}.{self._table_name}')
        logger.debug('Входная мета === === === ===')
        logger.debug('Columns:')
        logger.debug('\n' + pprint.pformat(self._columns))
        logger.debug('Primary Keys:')
        logger.debug('\n' + pprint.pformat(self._pkl))

    def _log_final_params(self) -> None:
        logger.debug(f'Объект: {self._connection_name}.{self._schema_name}.{self._table_name}')
        logger.debug('SQLs === === === ===')
        logger.debug(' >>>> Base Select SQL:')
        logger.debug(self.base_select_sql)
        logger.debug(' >>>> Dedup Select SQL:')
        logger.debug(self.dedup_select_sql)
        logger.debug(' >>>> Total RecCount on Base Select SQL:')
        logger.debug(self.total_reccount_in_base_sql)
        logger.debug(' >>>> Total RecCount on Dedup Select SQL:')
        logger.debug(self.total_reccount_in_dedup_sql)

    def _get_base_select_sql(self) -> str:
        select_str = 'SELECT'
        cols = ', '.join([d['name'] for d in self._columns])
        from_str = f'FROM {self._schema_name}.{SqlProducer.touch_mssql_name(self._table_name)}'
        return '\n '.join([select_str, cols, from_str])
    
    def _get_dedup_select_sql(self) -> str:
        select_str = 'SELECT '
        cols = ', '.join([d['name'] for d in self._columns])
        from_str = 'FROM ('
        sub_select_str = 'SELECT '
        if len(self._business_pkl) > 0:
            pkl_str = ', '.join(self._business_pkl)
        else:
            pkl_str = ', '.join(self.pkl)
        rwn_str = f'ROW_NUMBER() OVER(PARTITION BY {pkl_str} ORDER BY {pkl_str} DESC) AS [rwn]'
        cols_subq = ', '.join([rwn_str] + [d['name'] for d in self._columns])
        sub_from_str = f'FROM {self._schema_name}.{SqlProducer.touch_mssql_name(self._table_name)}'
        closing_brace =  ') d'
        where_str = 'WHERE [rwn] = 1'
        return '\n'.join(
            [select_str, cols, from_str, sub_select_str, cols_subq,
             sub_from_str, closing_brace, where_str]
            )
    
    def _get_total_reccount_in_base_sql(self) -> str:
        select_from_str = 'SELECT COUNT(*) AS [cnt] FROM ('
        base_query = self.base_select_sql
        closing_brace = ') d'
        return '\n'.join([select_from_str, base_query, closing_brace])
    
    def _get_total_reccount_in_dedup_sql(self) -> str:
        select_from_str = 'SELECT COUNT(*) AS [cnt] FROM ('
        base_query = self.dedup_select_sql
        closing_brace = ') d'
        return '\n'.join([select_from_str, base_query, closing_brace])
    
    connection_name = property(lambda self: self._connextion_name)
    pkl = property(lambda self: [s for s in self._pkl['constrained_columns']])
    columns = property(lambda self: [c['name'] for c in self._columns])

    @property
    def base_select_sql(self):
        return ''.join(['\n',
            sqlparse.format(self._get_base_select_sql(), 
                            reindent=True, keyword_case='upper'
                            )
                        ])
    
    @property
    def dedup_select_sql(self):
        return ''.join(['\n',
            sqlparse.format(self._get_dedup_select_sql(), 
                            reindent=True, keyword_case='upper'
                           )
                        ])
    
    @property
    def total_reccount_in_base_sql(self):
        return ''.join(['\n', 
            sqlparse.format(self._get_total_reccount_in_base_sql(), 
                            reindent=True, keyword_case='upper'
                            )
                        ])
    
    @property
    def total_reccount_in_dedup_sql(self):
        return ''.join(['\n',  
            sqlparse.format(self._get_total_reccount_in_dedup_sql(), 
                            reindent=True, keyword_case='upper'
                            )
                        ])

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
