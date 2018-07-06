from ConfigParser import ConfigParser
from os.path import dirname, abspath, join, exists

from os import getcwd, makedirs
from sys import argv

PROP_ZOOM_API_BASE_URL = 'api_base_url'

PROP_ZOOM_API_TOKEN = 'api_token'
PROP_ZOOM_API_KEY = 'api_key'
PROP_ZOOM_API_SECRET = 'api_secret'

SECTION_ZOOM = 'zoom'

PROP_AGNI_ATT_DEFAULT_DAYS = 'attendance_default_days'

SECTION_AGNI = 'agni'


def getBaseDir():
    runningFile = argv[0]
    if runningFile.lower().endswith('agni_gcr_attendance.exe'):
        bd = abspath(dirname(runningFile))
        print 'Got basedir:', bd
        return bd
    try:
        bd = abspath(dirname(dirname(__file__)))
        print 'Got basedir:', bd
        return bd
    except:
        bd = abspath(getcwd())
        print 'Got basedir:', bd
        return bd


def getLogsDir():
    return join(getBaseDir(), 'logs')


def getLogFilePath():
    logsDir = getLogsDir()
    if not exists(logsDir):
        makedirs(logsDir)
    filename = 'agni_gcr_attendance.log'
    return join(logsDir, filename)


def getOutputDir():
    outputDir = join(getBaseDir(), 'output')
    if not exists(outputDir):
        makedirs(outputDir)
    return outputDir

def getConfigFile():
    return join(getBaseDir(), 'agni-gcr.ini')


def getDbFile():
    return join(getBaseDir(), 'agni-gcr.db')

DEFAULT_CONFIG = (
    (
        SECTION_AGNI, (
            (PROP_AGNI_ATT_DEFAULT_DAYS, 4),
        ),
    ),
    (
        SECTION_ZOOM, (
            (PROP_ZOOM_API_TOKEN, 'ffffffffffffffffffffffffffffffff'),
            (PROP_ZOOM_API_KEY, 'ffffffffffffffff'),
            (PROP_ZOOM_API_SECRET, 'ffffffffffffffff'),
            (PROP_ZOOM_API_BASE_URL, 'https://api.zoom.us/v2/'),
        ),
    ),
)

def generateDefaultConfig():
    cfg = ConfigParser()
    for section, props in DEFAULT_CONFIG:
        cfg.add_section(section)
        for option, value in props:
            cfg.set(section, option, str(value))
    return cfg

def loadOrCreateConfigFile():
    from utils.logger import getAgniLogger
    _logger = getAgniLogger(__name__)

    cfgFile = getConfigFile()
    if not exists(cfgFile):
        _logger.warn('Configuration file %s not found', cfgFile)
        cfg = generateDefaultConfig()
        with open(cfgFile, 'wt') as fp:
            _logger.warn('Writing default configs to %s', cfgFile)
            cfg.write(fp)
        return cfg

    cfg = ConfigParser()
    with open(cfgFile, 'rt') as fp:
        _logger.info('Using config file %s', cfgFile)
        cfg.readfp(fp)

    return cfg

class AgniConfiguration:
    def __init__(self):
        self._cfg = loadOrCreateConfigFile()

    def get(self, section, option):
        return self._cfg.get(section, option)

    def getAgniOption(self, option):
        return self._cfg.get(SECTION_AGNI, option)

    def getZoomOption(self, option):
        return self._cfg.get(SECTION_ZOOM, option)

    def getAgniAttendanceDefaultDays(self):
        return int(self.getAgniOption(PROP_AGNI_ATT_DEFAULT_DAYS))

    def getZoomApiBaseUrl(self):
        return self.getZoomOption(PROP_ZOOM_API_BASE_URL)

    def getZoomApiToken(self):
        return self.getZoomOption(PROP_ZOOM_API_TOKEN)

    def getZoomApiKey(self):
        return self.getZoomOption(PROP_ZOOM_API_KEY)

    def getZoomApiSecret(self):
        return self.getZoomOption(PROP_ZOOM_API_SECRET)

if __name__ == '__main__':
    print 'Nothing to run'
else:
    agni_configuration = AgniConfiguration()
