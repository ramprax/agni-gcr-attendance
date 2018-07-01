from ConfigParser import ConfigParser
from genericpath import exists
from os.path import dirname, abspath, join, exists

from os import getcwd, makedirs

PROP_ZOOM_API_BASE_URL = 'api_base_url'

PROP_ZOOM_API_TOKEN = 'api_token'

SECTION_ZOOM = 'zoom'

PROP_AGNI_ATT_DEFAULT_DAYS = 'attendance_default_days'

SECTION_AGNI = 'agni'


def getBaseDir():
    try:
        return abspath(dirname(dirname(__file__)))
    except:
        return abspath(getcwd())


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
    cfgFile = getConfigFile()
    if not exists(cfgFile):
        cfg = generateDefaultConfig()
        with open(cfgFile, 'wt') as fp:
            cfg.write(fp)

    cfg = ConfigParser()
    with open(cfgFile, 'rt') as fp:
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

agni_configuration = AgniConfiguration()
