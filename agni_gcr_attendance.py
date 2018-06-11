import csv
from collections import OrderedDict
from datetime import datetime
from os import listdir
from os.path import exists, join
import sqlite3
import sys

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
        classDate = datetime.strptime(originalClassDateStr, '%b %d, %Y %I:%M %p')
        internalClassDateStr = classDate.strftime('%Y-%m-%d %H:%M:%S')
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
            registeredDate = datetime.strptime(registeredDateStr, '%b %d, %Y %H:%M:%S')
            internalRegisteredDateStr = registeredDate.strftime('%Y-%m-%d %H:%M:%S')
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
        try:
            with open(filename, 'rt') as fd:
                rdr = csv.reader(fd, skipinitialspace=True)
                for line in rdr:
                    curLine = rdr.line_num
                    # print 'Line:', curLine, line
                    # line = line.strip()
                    if not line:
                        continue
                    line = [l.strip() for l in line]
                    for s in SECTION_NAMES:
                        if line[0].strip().startswith(s):
                            # print 'Got section', s
                            curSection = s
                            break
                    self.processLine(curSection, line)
        except:
            print >> sys.stderr, '**** Error in file %s at line %s'%(filename, curLine)
            raise

def generateEmailWiseAttendanceFromDB(cnx, zoomWebinarId):
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
        print >> sys.stderr, 'Unknown zoom webinar id: %s' % zoomWebinarId
        return
    classDates = [r[0] for r in rows]
    header = ['Email']+classDates
    yield header
    count += 1
    currEmail = None
    currAttendedArray = []
    cur.execute(attendanceQuery, (zoomWebinarId,))
    for row in cur:
        email = row[0]
        if email != currEmail:
            if currEmail is not None:
                yield ([currEmail]+currAttendedArray)
                count += 1
            currEmail = email
            currAttendedArray = []
        currAttendedArray.append(row[2])
    if currEmail and currAttendedArray:
        yield ([currEmail]+currAttendedArray)
        count += 1
    cur.close()
    print '%s lines yielded'%count


def loadAttendeeReportsToDB(dbfile, webinarId):
    conn = sqlite3.connect(dbfile)
    ai = AttendeeReportImporter(conn)
    for f in listdir('.'):
        if exists(f) and f.startswith(webinarId+' - Attendee Report'):
            print 'Processing file:', f
            ai.importAttendeeReport(f)
    conn.close()


def exportAttendanceFromDB(dbfile, zoomWebinarId, outputfilepath):
    conn = sqlite3.connect(dbfile)
    with open(outputfilepath, 'wt') as ofd:
        print 'Writing to', outputfilepath
        wrt = csv.writer(ofd)
        wrt.writerows(generateEmailWiseAttendanceFromDB(conn, zoomWebinarId))
    conn.close()


def main():
    # processNoDB()
    dbfile = 'agni-gcr.db'
    zoomWebinarId = raw_input("Enter zoom webinar id> ")
    zoomWebinarId = zoomWebinarId.replace('-', '')
    print 'Processing zoom webinar id: ', zoomWebinarId
    loadAttendeeReportsToDB(dbfile, zoomWebinarId)
    exportAttendanceFromDB(dbfile, zoomWebinarId,
                           join('output', '%s-AttendanceByEmail.csv'%zoomWebinarId))


if __name__ == '__main__':
    main()

