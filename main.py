import sys
from logger_conf2 import logger
import Pifagor2UkDev_tab_FigureRelation as pif2
#import home_kladr as kladr

# TODO
# MsSql touch column names with []
# full join on pkl (agg)
# select by sqla using table object
# pipe data from source to target db
# если анализируемый объект dwh, то дедубликация 

logger.debug(f'Реальный вызов: {sys.argv}')
logger.debug(f'Executable: {sys.executable}')

def run_main():
    logger.debug('Начало работы ...')
    pif2.process_using_iter_2()

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    run_main()
