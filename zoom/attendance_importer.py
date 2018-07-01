import csv
from datetime import datetime
from genericpath import exists
from os import listdir
from os.path import join, isdir

from utils.logger import flushLogs, getAgniLogger
from utils.common import sanitizeEmail
from zoom.common import sanitizeWebinarId
from agni.db import getConnection, prepareDB

_logger = getAgniLogger(__name__)

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
        prepareDB(self._cnx)

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


def loadAttendeeReportsToDB(webinarId):
    conn = None
    try:
        conn = getConnection()
        ai = AttendeeReportImporter(conn)
        webinarDir = guessOrInputWebinarDirectoryName(webinarId)
        for f in listdir(webinarDir):
            fp = join(webinarDir, f)
            if exists(fp) and f.startswith(webinarId+' - Attendee Report'):
                _logger.info('Processing file: %s', fp)
                ai.importAttendeeReport(fp)
                _logger.info('Done')
    finally:
        if conn:
            conn.close()


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