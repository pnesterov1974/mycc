#import sys
from loguru import logger
from current_logfile import CurrentLogFile

CLF = CurrentLogFile()
#https://betterstack.com/community/guides/logging/loguru/

log_level = "DEBUG"
log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"

logger.remove(0)
logger.add(CLF.log_filepath, level=log_level, format=log_format, colorize=False, backtrace=True, diagnose=True)


# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass
