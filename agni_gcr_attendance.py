
from agni.attendance import exportAttendanceFromDB, cancelDefaulters
from utils.logger import flushLogs, getAgniLogger
from utils.configuration import getLogFilePath
from zoom.api import askAndMakeZoomApiToken
from zoom.attendance_importer import loadAttendeeReportsToDB
from zoom.common import sanitizeWebinarId


_logger = getAgniLogger(__name__)

_logger.info('Log file location: %s', getLogFilePath())

def getZoomWebinarIdUserInput():
    flushLogs()
    zoomWebinarId = raw_input("Enter zoom webinar id> ")
    zoomWebinarId = sanitizeWebinarId(zoomWebinarId)
    return zoomWebinarId

def processSingleWebinarId():
    zoomWebinarId = getZoomWebinarIdUserInput()
    _logger.info('Processing zoom webinar id: %s', zoomWebinarId)
    loadAttendeeReportsToDB(zoomWebinarId)
    exportAttendanceFromDB(zoomWebinarId)

def processDefaulters():
    zoomWebinarId = getZoomWebinarIdUserInput()
    _logger.info('Processing defaulters for zoom webinar id: %s', zoomWebinarId)
    num = cancelDefaulters(zoomWebinarId)
    _logger.info('%s defaulters cancelled from webinar %s', num, zoomWebinarId)


MENU = (
'''Agni Global Classroom - Attendance
==================================
1. Import attendee reports & generate consolidated attendance report
2. Cancel webinar registrants who are defaulters
3. Generate a Zoom API token for use outside this program
Enter choice> '''
)

def doMenu():
    try:
        choice = int(raw_input(MENU))
    except:
        print 'Leaving menu'
        return

    if choice not in (1, 2, 3):
        print 'Leaving menu'
        return

    choice -= 1
    funcs = [processSingleWebinarId, processDefaulters, askAndMakeZoomApiToken]
    funcs[choice]()


def doAgainLoop(func, prompt='Do again?'):
    yn = 'Y'
    while yn.strip().upper() in ('Y', 'YES', ''):
        func()
        yn = raw_input(prompt+' (Y/N) >')


def main():
    # processNoDB()
    try:
        doAgainLoop(doMenu, prompt='Go back to menu?')
    except:
        _logger.exception('Error occurred')
        raise
    finally:
        flushLogs()
        raw_input('Press <ENTER> key to quit..')


if __name__ == '__main__':
    main()

