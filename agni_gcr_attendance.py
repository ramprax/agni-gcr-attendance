import csv
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
    CREATE TABLE IF NOT EXISTS webinar(
        id INTEGER PRIMARY KEY,
        zoom_webinar_id TEXT NOT NULL UNIQUE,
        topic TEXT NOT NULL UNIQUE
    )
'''
TABLE_WEBINAR_REGISTRANT = '''
    CREATE TABLE IF NOT EXISTS webinar_registrant(
        id INTEGER PRIMARY KEY,
        email TEXT NOT NULL,
        webinar_id INTEGER NOT NULL REFERENCES webinar(id),
        original_registration_datetime TEXT,
        internal_registration_datetime TEXT,
        UNIQUE(email, webinar_id)
    )
'''
TABLE_WEBINAR_CLASS = '''
    CREATE TABLE IF NOT EXISTS webinar_class(
        id INTEGER PRIMARY KEY,
        webinar_id INTEGER NOT NULL REFERENCES webinar(id),
        internal_datetime TEXT NOT NULL,
        original_datetime TEXT,
        UNIQUE(webinar_id, internal_datetime)
    )
'''
TABLE_ATTENDANCE = '''
    CREATE TABLE IF NOT EXISTS attendance(
        id INTEGER PRIMARY KEY,
        webinar_class_id INTEGER NOT NULL REFERENCES webinar_class(id),
        registrant_id INTEGER NOT NULL REFERENCES webinar_registrant(id),
        attended TEXT NOT NULL,
        UNIQUE(webinar_class_id, registrant_id)
    )
'''

SECTION_NAMES = (
    'Attendee Report',
    'Topic',
    'Host Details',
    'Panelist Details',
    'Attendee Details',
    'Other Attended',
)

ZOOM_WEBINAR_DATETIME_FORMAT = '%b %d, %Y %I:%M %p'
ZOOM_REGISTRATION_DATETIME_FORMAT = '%b %d, %Y %H:%M:%S'
INTERNAL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
EXPORT_DATE_FORMAT = '%b %d, %Y'


def sanitizeEmail(email):
    email = email.strip().lower()
    emailParts = email.split('@')

    if len(emailParts) != 2:
        raise Exception('Invalid email id: %s', email)

    for ep in emailParts:
        if not ep:
            raise Exception('Invalid email id: %s', email)

    emailDomainParts = emailParts[1].split('.')

    if len(emailDomainParts) <= 1:
        raise Exception('Invalid email id: %s', email)

    for ed in emailDomainParts:
        if not ed:
            raise Exception('Invalid email id: %s', email)

    return email


def sanitizeWebinarId(originalWebinarId):
    zoomWebinarId = originalWebinarId.strip().replace('-', '')

    try:
        intZoomWebinarId = int(zoomWebinarId)
        return zoomWebinarId
    except:
        _logger.exception("Expected webinar id to be a number with/without '-' in between. Instead got '%s'",
                          originalWebinarId)
        raise Exception("Expected webinar id to be a number with/without '-' in between. Instead got '%s'" %
                        originalWebinarId, originalWebinarId)

class AttendeeReportImporter:
    
    def __init__(self, cnx):
        self._sectionHandler = {
            'Attendee Report':self.ignoreLine,
            'Topic':self.processTopicLine,
            'Panelist Details':self.ignoreLine,
            'Attendee Details':self.processAttendeeDetailsLine,
            'Other Attended':self.ignoreLine,
            'Host Details':self.ignoreLine,
        }
        self._cnx = cnx
        self._resetCurrentContext()
        self._prepareDB()
    
    def _resetCurrentContext(self):
        self.currentClassId = None
        self.currentClassDate = None
        self.currentWebinarId = None
        self.emailColIndex = None
        self.regDateColIndex = None
        self._insertedRegistrantEmails = []
        self.registrantInsertParams = []
        self.registrantUpdateParams = []
        self._insertedAttendances = {}
        self.attendanceInsertParams = []
        self.attendanceUpdateParams = []

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

        originalWebinarId = line[1]
        zoomWebinarId = sanitizeWebinarId(originalWebinarId)

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

    def _insertRegistrants(self, registrantParamsList):
        if not registrantParamsList:
            return 0

        rins = '''
                INSERT INTO webinar_registrant (
                    email,
                    webinar_id,
                    internal_registration_datetime,
                    original_registration_datetime
                ) VALUES (?, ?, ?, ?)                
            '''
        cur = None
        _logger.debug('Registrants to insert: %s', registrantParamsList)
        try:
            cur = self._cnx.cursor()
            cur.executemany(rins, registrantParamsList)
            self._cnx.commit()
            return cur.rowcount
        finally:
            if cur:
                cur.close()

    def _updateRegistrants(self, registrantParamsList):
        if not registrantParamsList:
            return 0

        rupd = '''
                    UPDATE webinar_registrant
                    SET internal_registration_datetime = ?,
                        original_registration_datetime = ?
                    WHERE email = ? AND webinar_id = ?
                    AND (
                        internal_registration_datetime IS NULL
                        OR
                        internal_registration_datetime > ?
                    )
                '''
        cur = None
        _logger.debug('Registrants to update: %s', registrantParamsList)
        try:
            cur = self._cnx.cursor()
            cur.executemany(rupd, registrantParamsList)
            self._cnx.commit()
            return cur.rowcount
        finally:
            if cur:
                cur.close()

    def _insertAttendance(self, attendanceParamsList):
        if not attendanceParamsList:
            return 0

        ains = '''
                INSERT INTO attendance(webinar_class_id, registrant_id, attended)
                SELECT ?, id, ? FROM webinar_registrant WHERE email = ? AND webinar_id = ?
            '''
        cur = None
        _logger.debug('Attendance to insert: %s', attendanceParamsList)
        try:
            cur = self._cnx.cursor()
            cur.executemany(ains, attendanceParamsList)
            self._cnx.commit()
            return cur.rowcount
        finally:
            if cur:
                cur.close()

    def _updateAttendance(self, attendanceParamsList):
        if not attendanceParamsList:
            return 0

        aupd = '''
                UPDATE attendance SET attended=?
                WHERE webinar_class_id= ? AND registrant_id = (SELECT id FROM webinar_registrant WHERE email = ?)
            '''
        cur = None
        _logger.debug('Attendance to update: %s', attendanceParamsList)
        try:
            cur = self._cnx.cursor()
            cur.executemany(aupd, attendanceParamsList)
            self._cnx.commit()
            return cur.rowcount
        finally:
            if cur:
                cur.close()

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

        email = sanitizeEmail(line[self.emailColIndex])
        
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

        aq = '''
                SELECT attended FROM attendance
                WHERE webinar_class_id = ? AND registrant_id = (SELECT id FROM webinar_registrant WHERE email = ? AND webinar_id = ?)
            '''

        cur = None
        try:
            cur = self._cnx.cursor()
            cur.execute(rq, (email, self.currentWebinarId))
            rows = cur.fetchall()

            if not rows and (email not in self._insertedRegistrantEmails):
                self._insertedRegistrantEmails.append(email)
                self.registrantInsertParams.append((email, self.currentWebinarId, internalRegisteredDateStr, registeredDateStr))
            else:
                if registeredDateStr:
                    self.registrantUpdateParams.append((internalRegisteredDateStr, registeredDateStr, email, self.currentWebinarId,
                                              internalRegisteredDateStr))

            shouldUpdateAttendance = False
            shouldInsertAttendance = False

            cur.execute(aq, (self.currentClassId, email, self.currentWebinarId))
            rows = cur.fetchall()
            if rows:
                savedAttended = rows[0][0]
                if savedAttended == 'No' and hadAttended == 'Yes':
                    shouldUpdateAttendance = True
            elif email in self._insertedAttendances:
                savedAttended = self._insertedAttendances[email]
                if savedAttended == 'No' and hadAttended == 'Yes':
                    shouldUpdateAttendance = True
            else:
                shouldInsertAttendance = True

            # Save to table attendance
            if shouldInsertAttendance:
                self._insertedAttendances[email] = hadAttended
                self.attendanceInsertParams.append((self.currentClassId, hadAttended, email, self.currentWebinarId))
            elif shouldUpdateAttendance:
                self.attendanceUpdateParams.append((hadAttended, self.currentClassId, email, self.currentWebinarId))

            self._cnx.commit()

        finally:
            if cur:
                cur.close()

    def ignoreLine(self, line):
        pass

    def processLine(self, section, line):
        if not section:
            return
        handleLine = self._sectionHandler.get(section, self.ignoreLine)
        handleLine(line)

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

            ric = self._insertRegistrants(self.registrantInsertParams)
            _logger.info('%s registrant records inserted', ric)

            ruc = self._updateRegistrants(self.registrantUpdateParams)
            _logger.info('%s registrant records updated', ruc)

            aic = self._insertAttendance(self.attendanceInsertParams)
            _logger.info('%s attendee records inserted', aic)

            auc = self._updateAttendance(self.attendanceUpdateParams)
            _logger.info('%s attendee records updated', auc)

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


def processSingleWebinarId():
    dbfile = 'agni-gcr.db'
    flushLogs()
    zoomWebinarId = raw_input("Enter zoom webinar id> ")

    zoomWebinarId = sanitizeWebinarId(zoomWebinarId)
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


def doAgainLoop(func, prompt='Do again?'):
    yn = 'Y'
    while yn.strip().upper() in ('Y', 'YES', ''):
        func()
        yn = raw_input(prompt+' (Y/N) >')


def main():
    # processNoDB()
    try:
        doAgainLoop(processSingleWebinarId, prompt='Process another webinar?')
    except:
        _logger.exception('Error occurred')
        raise
    finally:
        flushLogs()
        raw_input('Press <ENTER> key to quit..')


if __name__ == '__main__':
    main()

