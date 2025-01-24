import logging
from current_logfile import CurrentLogFile

CLF = CurrentLogFile()

logger = logging.getLogger('ml_git_log')
logger.setLevel('DEBUG')


std_formatter = logging.Formatter(
    fmt='{asctime} {levelname:8} {message}', style='{'
)

file_handler = logging.FileHandler(CLF.log_filepath, mode='a', encoding='utf-8')
file_handler.setFormatter(std_formatter)
logger.addHandler(file_handler)

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass
