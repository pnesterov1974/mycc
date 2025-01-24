from pathlib import Path
from datetime import datetime, timedelta
from random import randint

class CurrentLogFile:

    def __init__(self, days_to_keep=10, silent=False):
        self.__min_log_date = (datetime.today() - timedelta(days=days_to_keep)).date()
        self.__init_log_dir()
        self.__init_log_file()
        # self.__rotate_log_files()
        if not silent:
            print(f'лог-файл текущего сеанса: {self.log_filepath}')
        
    def __init_log_dir(self):
        self.__log_dir = Path().absolute() / '_log'
        if not(Path.exists(self.__log_dir) or Path.is_dir(self.__log_dir)):
            Path.mkdir(self.__log_dir)

    def __init_log_file(self):
        self.__log_filename = ''.join(
            ['_'.join(
                ['log', self.__filename_timestamp(datetime.now()), 'x', str(randint(0, 10000))]
                ),
                '.log'
            ]
        )

    # def __rotate_log_files(self):
    #     _logfiles = [f for f in Path.iterdir(self.__log_dir) if f.suffix.lower().endswith('.log')]
    #     for f in _logfiles:
    #         try:  
    #             ddd = datetime.strptime(f.stem.split('_')[1], '%Y-%m-%d').date()
    #             if ddd < self.__min_log_date:
    #                print(f'файл {f} подлежит удалению')
    #                Path.unlink(f)
    #         except:
    #             pass

    def __filename_timestamp(self, d: datetime):
        dt = datetime.date(d)    
        dy = dt.year
        dm = dt.month
        dd = dt.day

        tt = datetime.time(d)
        th = tt.hour
        tm = tt.minute
        ts = tt.second
        tms = tt.microsecond

        def _process_date_parts(date_part: int) -> str:
            _str = str(date_part)
            return _str if len(_str) > 1 else ''.join(['0', _str])
    
        di = '-'.join(list(map(_process_date_parts, [dy, dm, dd])))
        ti = '-'.join(list(map(_process_date_parts, [th, tm, ts, tms])))

        return '_'.join([di, ti])

    log_filename = property(lambda self: self.__log_filename)
    log_filepath = property(lambda self: Path(self.__log_dir, self.__log_filename))
    
# -----------------------------------------------------------------------------
if __name__ == '__main__': pass
