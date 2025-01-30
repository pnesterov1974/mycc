
import sys
from loguru import logger

from work import Worker


logger.debug(f'Реальный вызов: {sys.argv}')
logger.debug(f'Executable: {sys.executable}')
logger.info('Начало работы ...')

def main():
    w = Worker()
    w.do_work()

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
    pass
