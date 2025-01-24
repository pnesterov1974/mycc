import sys
from loguru import logger
from current_logfile import CurrentLogFile

CLF = CurrentLogFile()
#https://betterstack.com/community/guides/logging/loguru/

logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
logger.add(CLF.log_filepath, backtrace=True, diagnose=True)

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass
