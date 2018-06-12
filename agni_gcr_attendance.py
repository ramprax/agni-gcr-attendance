import csv
from collections import OrderedDict
from datetime import datetime
from logging import basicConfig, DEBUG, Formatter, getLogger, INFO, StreamHandler
from logging.handlers import TimedRotatingFileHandler
from os import listdir, makedirs
from os.path import exists, join, isdir
import sqlite3
import sys


def addFileHandler(filepath):
    rootLogger = getLogger()
    fh = TimedRotatingFileHandler(filepath, when='midnight')
    fh.setLevel(DEBUG)
    fh.setFormatter(Formatter(fmt='%(asctime)s %(levelname)-9.9s %(message)s', datefmt='%d-%b-%Y %H:%M:%S'))
    rootLogger.addHandler(fh)
    return fh


def getLogFilePath():
    if not exists('logs'):
        makedirs('logs')
    filename = 'agni_gcr_attendance.log'
    return join('logs', filename)


def _configureLogger():
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
_logger = getLogger(__name__)

def flushLogs():
    for h in getLogger().handlers:
        if isinstance(h, StreamHandler):
            h.flush()

_logger.info('Log file location: %s', getLogFilePath())

TABLE_WEBINAR = '''
    CREATE TABLE IF NOT EXISTS webinar(id INTEGER PRIMARY KEY, zoom_webinar_id TEXT, topic TEXT)
'''
TABLE_WEBINAR_REGISTRANT = '''
    CREATE TABLE IF NOT EXISTS webinar_registrant(id INTEGER PRIMARY KEY, email TEXT, webinar_id INTEGER, original_registration_datetime TEXT, internal_registration_datetime TEXT)
'''
TABLE_WEBINAR_CLASS = '''
    CREATE TABLE IF NOT EXISTS webinar_class(id INTEGER PRIMARY KEY, webinar_id TEXT, internal_datetime TEXT, original_datetime TEXT)
'''
TABLE_ATTENDANCE = '''
    CREATE TABLE IF NOT EXISTS attendance(id INTEGER PRIMARY KEY, webinar_class_id INTEGER, registrant_id INTEGER, attended TEXT)
'''

SECTION_NAMES = (
    'Attendee Report',
    'Topic',
    'Panelist Details',
    'Attendee Details',
    'Other Attended',
)

ZOOM_WEBINAR_DATETIME_FORMAT = '%b %d, %Y %I:%M %p'
ZOOM_REGISTRATION_DATETIME_FORMAT = '%b %d, %Y %H:%M:%S'
INTERNAL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
EXPORT_DATE_FORMAT = '%b %d, %Y'

class AttendeeReportImporter:
    
    def __init__(self, cnx):
        self._sectionHandler = {
            'Attendee Report':self.ignoreLine,
            'Topic':self.processTopicLine,
            'Panelist Details':self.ignoreLine,
            'Attendee Details':self.processAttendeeDetailsLine,
            'Other Attended':self.ignoreLine,
        }
        self._cnx = cnx 
        self.currentClassId = None
        self.currentClassDate = None
        self.currentWebinarId = None
        self.emailColIndex = None
        self.regDateColIndex = None
        self._prepareDB()
    
    def _resetCurrentContext(self):
        self.currentClassId = None
        self.currentClassDate = None
        self.currentWebinarId = None
        self.emailColIndex = None
        self.regDateColIndex = None
    
    def _prepareDB(self):
        cur = self._cnx.cursor()
        cur.execute(TABLE_WEBINAR)
        cur.execute(TABLE_WEBINAR_REGISTRANT)
        cur.execute(TABLE_WEBINAR_CLASS)
        cur.execute(TABLE_ATTENDANCE)
        self._cnx.commit()
        cur.close()

    def processTopicLine(self, line):
        isHeader = line[0].startswith('Topic')
        if isHeader:
            return
        
        topic = line[0]
        if not topic:
            return
        zoomWebinarId = line[1].replace('-', '')
        # Save this to table webinar
        wq = '''
            SELECT id FROM webinar WHERE zoom_webinar_id = ?
        '''
        cur = self._cnx.cursor()
        cur.execute(wq, (zoomWebinarId,))
        rows = cur.fetchall()
        if not rows:
            wins = '''
                INSERT INTO webinar (zoom_webinar_id, topic)
                VALUES (?, ?)
            '''
            cur.execute(wins, (zoomWebinarId, topic))
            self.currentWebinarId = cur.lastrowid
        else:
            self.currentWebinarId = rows[0][0]
        
        originalClassDateStr = line[2]
        classDate = datetime.strptime(originalClassDateStr, ZOOM_WEBINAR_DATETIME_FORMAT)
        internalClassDateStr = classDate.strftime(INTERNAL_DATETIME_FORMAT)
        # Save to table webinar class
        cq = '''
            SELECT id FROM webinar_class WHERE webinar_id = ? AND internal_datetime = ?
        '''
        cur.execute(cq, (self.currentWebinarId, internalClassDateStr))
        rows = cur.fetchall()
        if not rows:
            cins = '''
                INSERT INTO webinar_class(webinar_id, internal_datetime, original_datetime)
                VALUES (?, ?, ?)
            '''
            cur.execute(cins, (self.currentWebinarId, internalClassDateStr, originalClassDateStr))
            self.currentClassId = cur.lastrowid
        else:        
            self.currentClassId = rows[0][0] # Get it from db
        
        self.currentClassDate = internalClassDateStr
        
        self._cnx.commit()
        cur.close()

        pass

    def processAttendeeDetailsLine(self, line):
        isHeader = line[0].startswith('Attendee Details') or line[0].startswith('Attended')
        if isHeader:
            if line[0].startswith('Attended'):
                # Configure columns
                self.emailColIndex = line.index('Email')
                self.regDateColIndex = line.index('Registration Time')
            
            return
        
        hadAttended = line[0]
        if not hadAttended:
            return
        email = line[self.emailColIndex]
        
        registeredDateStr = line[self.regDateColIndex]
        if registeredDateStr:
            registeredDate = datetime.strptime(registeredDateStr, ZOOM_REGISTRATION_DATETIME_FORMAT)
            internalRegisteredDateStr = registeredDate.strftime(INTERNAL_DATETIME_FORMAT)
        else:
            registeredDate = None
            internalRegisteredDateStr = None
        
        rq = '''
            SELECT id FROM webinar_registrant WHERE email = ? AND webinar_id = ?
        '''
        cur = self._cnx.cursor()
        cur.execute(rq, (email, self.currentWebinarId))
        rows = cur.fetchall()
        if not rows:
            # Save to table webinar_registrant
            rins = '''
                INSERT INTO webinar_registrant (
                    email,
                    webinar_id,
                    internal_registration_datetime,
                    original_registration_datetime
                ) VALUES (?, ?, ?, ?)                
            '''
            cur.execute(rins,
                (email, self.currentWebinarId, internalRegisteredDateStr, registeredDateStr)
            )
            registrantId = cur.lastrowid
        else:
            registrantId = rows[0][0]
            if registeredDateStr:
                rupd = '''
                    UPDATE webinar_registrant
                    SET internal_registration_datetime = ?,
                        original_registration_datetime = ?
                    WHERE id = ?
                    AND (
                        internal_registration_datetime IS NULL
                        OR
                        internal_registration_datetime > ?
                    )
                '''
                cur.execute(rupd,
                    (internalRegisteredDateStr, registeredDateStr, registrantId,
                     internalRegisteredDateStr)
                )
        
        aq = '''
            SELECT attended FROM attendance
            WHERE webinar_class_id = ? AND registrant_id = ?
        '''
        cur.execute(aq, (self.currentClassId, registrantId))
        rows = cur.fetchall()
        shouldUpdate = False
        shouldInsert = False
        if rows:
            savedAttended = rows[0][0]
            if savedAttended == 'No' and hadAttended == 'Yes':
                shouldUpdate = True
        else:
            shouldInsert = True
        
        # Save to table attendance
        if shouldInsert:
            ains = '''
                INSERT INTO attendance(webinar_class_id, registrant_id, attended)
                VALUES (?, ?, ?)
            '''
            cur.execute(ains, (self.currentClassId, registrantId, hadAttended))
        elif shouldUpdate:
            aupd = '''
                UPDATE attendance SET attended=?
                WHERE webinar_class_id= ? AND registrant_id = ?
            '''
            cur.execute(aupd, (hadAttended, self.currentClassId, registrantId))
        self._cnx.commit()
        cur.close()
        pass

    def ignoreLine(self, line):
        pass

    def processLine(self, section, line):
        if not section:
            return
        self._sectionHandler[section](line)

    def importAttendeeReport(self, filename):
        self._resetCurrentContext()
        curSection = None
        curLine = 0
        line = None
        try:
            with open(filename, 'rt') as fd:
                rdr = csv.reader(fd, skipinitialspace=True)
                for line in rdr:
                    curLine = rdr.line_num
                    # print 'Line:', curLine, line
                    if not line:
                        continue
                    line = [l.strip() for l in line]
                    for s in SECTION_NAMES:
                        if line[0].strip().startswith(s):
                            _logger.debug('At line %s: Got section %s', curLine, s)
                            curSection = s
                            break
                    self.processLine(curSection, line)
        except:
            _logger.exception('**** Error in file %s at line %s: %s', filename, curLine, line)
            raise


def isDefaulter(attendanceArray, days=4):
    if len(attendanceArray) >= days:
        attSet = set(attendanceArray[-days:])
        if ('Yes' not in attSet) and ('NA' not in attSet):
            return True

    return False


def generateEmailWiseAttendanceFromDB(cnx, zoomWebinarId, attendanceWriter, defaultersWriter, defaultDays=4):
    classDatesQuery = '''
        SELECT wc.original_datetime
        FROM
            webinar w
            INNER JOIN
            webinar_class wc ON (wc.webinar_id = w.id)
        WHERE w.zoom_webinar_id = ?
        ORDER BY wc.internal_datetime ASC
    '''
    attendanceQuery = '''
        SELECT
            wr.email,
            wc.original_datetime,
            CASE a.attended
                WHEN 'Yes'
                    THEN 'Yes'
	            ELSE
	                CASE
	                    WHEN wr.internal_registration_datetime > wc.internal_datetime
	                        THEN 'NA'
	                    ELSE 'No'
	                END
            END AS attended_after_registering
        FROM
            webinar w
            INNER JOIN
            webinar_class wc ON (w.id = wc.webinar_id)
	        INNER JOIN
	        webinar_registrant wr ON (wr.webinar_id = wc.webinar_id)
	        LEFT OUTER JOIN
	        attendance a ON (wr.id = a.registrant_id AND wc.id = a.webinar_class_id)
	    WHERE
	        w.zoom_webinar_id = ?
        ORDER BY
           wr.email ASC,
           wc.internal_datetime ASC
    '''
    count = 0
    cur = cnx.cursor()
    cur.execute(classDatesQuery, (zoomWebinarId,))
    rows = cur.fetchall()
    if not rows:
        _logger.error('Unknown zoom webinar id: %s', zoomWebinarId)
        return
    classDates = [datetime.strptime(r[0], ZOOM_WEBINAR_DATETIME_FORMAT).strftime(EXPORT_DATE_FORMAT) for r in rows]
    header = ['Email']+classDates
    attendanceWriter.writerow(header)
    defaultersWriter.writerow(['Email'])
    count = 0
    defaultersCount = 0
    currEmail = None
    currAttendedArray = []
    cur.execute(attendanceQuery, (zoomWebinarId,))
    for row in cur:
        email = row[0]
        if email != currEmail:
            if currEmail is not None:
                if isDefaulter(currAttendedArray, days=defaultDays):
                    defaultersCount += 1
                    defaultersWriter.writerow([currEmail])
                attendanceWriter.writerow([currEmail]+currAttendedArray)
                count += 1
            currEmail = email
            currAttendedArray = []
        currAttendedArray.append(row[2])
    if currEmail and currAttendedArray:
        if isDefaulter(currAttendedArray, days=defaultDays):
            defaultersCount += 1
            defaultersWriter.writerow([currEmail])
        attendanceWriter.writerow([currEmail]+currAttendedArray)
        count += 1
    cur.close()
    _logger.info('Registrants: %s | Defaulters: %s', count, defaultersCount)


def guessOrInputWebinarDirectoryName(zoomWebinarId):
    webinarDir = None
    for f in listdir('.'):
        if isdir(f) and zoomWebinarId in f:
            webinarDir = f
            _logger.info("Found directory '%s'", webinarDir)
            break

    yn = 'N'
    if webinarDir:
        flushLogs()
        yn = raw_input("Load files from directory '%s'? (Y/N)> "%webinarDir)

    if yn.upper() not in ('YES', 'Y') and yn.strip() != '':
        flushLogs()
        webinarDir = raw_input("Enter the directory to look for attendee report files> ")
        while not isdir(webinarDir):
            _logger.error('Not a valid directory: %s', webinarDir)
            flushLogs()
            webinarDir = raw_input("Enter the directory to look for attendee report files> ")

    return webinarDir


def loadAttendeeReportsToDB(dbfile, webinarId):
    conn = sqlite3.connect(dbfile)
    ai = AttendeeReportImporter(conn)
    webinarDir = guessOrInputWebinarDirectoryName(webinarId)
    for f in listdir(webinarDir):
        fp = join(webinarDir, f)
        if exists(fp) and f.startswith(webinarId+' - Attendee Report'):
            _logger.info('Processing file: %s', fp)
            ai.importAttendeeReport(fp)
            _logger.info('Done')
    conn.close()


def exportAttendanceFromDB(dbfile, zoomWebinarId, outputfilepath, defaultersfilepath, defaultDays=4):
    conn = sqlite3.connect(dbfile)

    with open(outputfilepath, 'wb') as ofd, open(defaultersfilepath, 'wb') as dfd:
        _logger.info('Writing attendance to %s', outputfilepath)
        wrt = csv.writer(ofd)

        _logger.info('Writing defaulters to %s', defaultersfilepath)
        dwrt = csv.writer(dfd)

        generateEmailWiseAttendanceFromDB(conn, zoomWebinarId, wrt, dwrt, defaultDays=defaultDays)

    conn.close()


def getOutputFilePath(zoomWebinarId):
    if not exists('output'):
        makedirs('output')
    return join('output', '%s-AttendanceByEmail.csv'%zoomWebinarId)


def getDefaultersFilePath(zoomWebinarId):
    if not exists('output'):
        makedirs('output')
    return join('output', '%s-Defaulters.csv'%zoomWebinarId)


def getDefaultDays(defaultDays = 4):
    while True:
        flushLogs()
        dd = raw_input('Enter number of consecutive days to check defaulters [default: %s]> '%defaultDays)
        if dd is None or dd.strip() == '':
            _logger.info('Considering %s consecutive days absentee as defaulter', defaultDays)
            return defaultDays
        else:
            try:
                defaultDays = int(dd)
                _logger.info('Considering %s consecutive days absentee as defaulter', defaultDays)
                return defaultDays
            except Exception:
                _logger.error('Not a valid number: %s', dd)


def main():
    # processNoDB()
    try:
        dbfile = 'agni-gcr.db'
        flushLogs()
        zoomWebinarId = raw_input("Enter zoom webinar id> ")
        
        zoomWebinarId = zoomWebinarId.replace('-', '')
        _logger.info('Processing zoom webinar id: %s', zoomWebinarId)
        loadAttendeeReportsToDB(dbfile, zoomWebinarId)

        dd = getDefaultDays(defaultDays=4)

        exportAttendanceFromDB(
            dbfile,
            zoomWebinarId,
            getOutputFilePath(zoomWebinarId),
            getDefaultersFilePath(zoomWebinarId),
            defaultDays=dd
        )
    except:
        _logger.exception('Error occurred')
        raise
    finally:
        flushLogs()
        raw_input('Press <ENTER> key to quit..')


if __name__ == '__main__':
    main()

