from rich.console import Console
from rich.table import Table

from comparator import Comparator
from mappings import DBO_MAPPING
from sql_producer import SqlProducer


class Worker:

    def do_work(self):
        for k, v in DBO_MAPPING.items():
            dwh_schema_name = k
            src_schema_info = v
            self.do_work_schema(dwh_schema_name, src_schema_info)

    def do_work_schema(self, dwh_schema_name: str, src_schema_info: dict):
        print(f' === === === Schema: {src_schema_info}')
        self.dlist = []
        src_connection_name = src_schema_info['connection_name']
        dwh_connection_name = 'DWH'
        src_schema_name = src_schema_info['schema_name']
        for table_name in src_schema_info['dwh_tables']:
            full_dwh_table = f'{dwh_schema_name}.{table_name}'
            full_src_table = f'{src_schema_name}.{table_name}'
            print(full_src_table, '=>', full_dwh_table)

            ds_src = SqlProducer(conn_name=src_connection_name, 
                                schema_name=src_schema_name, 
                                table_name=table_name)
            ds_trg = SqlProducer(conn_name=dwh_connection_name, 
                            schema_name=dwh_schema_name, 
                            table_name=table_name,
                            business_pkl=ds_src.pkl)
            w = Comparator(ds_src, ds_trg)
            dresult = w.compare_total_record_count()
            dresult['dwh_schema_name'] = dwh_schema_name
            dresult['src_schema_name'] = src_schema_name
            dresult['table_name'] = table_name
            self.dlist.append(dresult)

        print(self.dlist)

    def print_results(self):

        table = Table(title=f"-= DWH vs Source P2 =-")
        table.add_column("DWH Schema name", justify="left", style="blue", no_wrap=True)
        table.add_column("Source Schema name", justify="right", style="cyan", no_wrap=True)
        table.add_column("Table Name", justify="right", style="magenta", no_wrap=True)
        table.add_column("Source Total Recs.", justify="right", style="cyan", no_wrap=True)
        table.add_column("DWH Total Recs.", justify="right", style="magenta", no_wrap=True)
        table.add_column("DWH Actual Recs.", justify="right", style="green", no_wrap=True)

        for dl in self.dlist:
            table.add_row(
                dl['dwh_schema_name'], dl['src_schema_name'], dl['table_name'],
                str(dl['src_total_record_count']), 
                str(dl['trg_total_record_count']), 
                str(dl['trg_deduped_record_count'])
            )

        console = Console()
        console.print(table)

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
