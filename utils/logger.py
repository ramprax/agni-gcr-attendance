import sys
from logging import getLogger, DEBUG, Formatter, basicConfig, StreamHandler, INFO
from logging.handlers import TimedRotatingFileHandler

def addFileHandler(filepath):
    rootLogger = getLogger()
    fh = TimedRotatingFileHandler(filepath, when='midnight')
    fh.setLevel(DEBUG)
    fh.setFormatter(Formatter(fmt='%(asctime)s %(levelname)-9.9s %(message)s', datefmt='%d-%b-%Y %H:%M:%S'))
    rootLogger.addHandler(fh)
    return fh


def _configureLogger():
    from utils.configuration import getLogFilePath

    basicConfig(
        format='%(asctime)s %(levelname)-9.9s %(message)s',
        datefmt='%d-%b-%Y %H:%M:%S',
        level=DEBUG
    )
    fh = addFileHandler(getLogFilePath())
    for h in getLogger().handlers:
        if isinstance(h, StreamHandler):
            if h.stream == sys.stderr or h.stream == sys.stdout:
                h.setLevel(INFO)

    return fh

_configureLogger()

def flushLogs():
    for h in getLogger().handlers:
        if isinstance(h, StreamHandler):
            h.flush()

getAgniLogger = getLogger
