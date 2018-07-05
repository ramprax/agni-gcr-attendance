import csv
from datetime import datetime
from genericpath import exists
from os import makedirs
from os.path import join

import requests

from agni.db import getConnection
from utils.configuration import agni_configuration, getOutputDir
from utils.logger import flushLogs, getAgniLogger
from zoom.api import ZoomApi, ACTION_DENY, ACTION_CANCEL
from zoom.attendance_importer import ZOOM_WEBINAR_DATETIME_FORMAT

_logger = getAgniLogger(__name__)

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
            COALESCE(a.attended, 'NA') AS attended_after_registering
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
    defaultersCount = 0
    cur = None
    try:
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
    finally:
        if cur:
            cur.close()

    _logger.info('Registrants: %s | Defaulters: %s', count, defaultersCount)


def exportAttendanceFromDB(zoomWebinarId):
    # dd = getDefaultDays(defaultDays=agni_configuration.getAgniAttendanceDefaultDays())
    dd = agni_configuration.getAgniAttendanceDefaultDays()
    _logger.info('Considering %s consecutive days absentee as defaulter. To change this edit the ini file', dd)

    attendanceReportFilePath = getOutputFilePath(zoomWebinarId)
    defaultersReportFilePath = getDefaultersFilePath(zoomWebinarId)

    conn = None
    try:
        conn = getConnection()

        with open(attendanceReportFilePath, 'wb') as ofd, open(defaultersReportFilePath, 'wb') as dfd:
            _logger.info('Writing attendance to %s', attendanceReportFilePath)
            wrt = csv.writer(ofd)

            _logger.info('Writing defaulters to %s', defaultersReportFilePath)
            dwrt = csv.writer(dfd)

            generateEmailWiseAttendanceFromDB(conn, zoomWebinarId, wrt, dwrt, defaultDays=dd)
    finally:
        if conn:
            conn.close()


def getOutputFilePath(zoomWebinarId):
    outputDir = getOutputDir()
    return join(outputDir, '%s-AttendanceByEmail.csv'%zoomWebinarId)


def getDefaultersFilePath(zoomWebinarId):
    outputDir = getOutputDir()
    return join(outputDir, '%s-Defaulters.csv'%zoomWebinarId)


def getDefaultDays(defaultDays = agni_configuration.getAgniAttendanceDefaultDays()):
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


def isDefaulter(attendanceArray, days=agni_configuration.getAgniAttendanceDefaultDays()):
    if len(attendanceArray) >= days:
        attSet = set(attendanceArray[-days:])
        if ('Yes' not in attSet) and ('NA' not in attSet):
            return True

    return False


EXPORT_DATE_FORMAT = '%b %d, %Y'


def getRegistrantsToUpdateStatus(zoomWebinarId, registrantEmails):
    registrantsToUpdateStatus = []
    for em in registrantEmails:
        registrantsToUpdateStatus.append({
            'email': em.strip().lower()
        })
    return registrantsToUpdateStatus


def denyRegistrants(zoomWebinarId, registrants):
    za = ZoomApi()
    return za.updateWebinarRegistrantsStatus(zoomWebinarId, ACTION_DENY, registrants=registrants)

def cancelRegistrants(zoomWebinarId, registrants):
    za = ZoomApi()
    return za.updateWebinarRegistrantsStatus(zoomWebinarId, ACTION_CANCEL, registrants=registrants)


def cancelDefaulters(zoomWebinarId):
    yn = 'N'
    emails = []
    defaultersFilePath = getDefaultersFilePath(zoomWebinarId)
    if not exists(defaultersFilePath):
        _logger.error('File not found: %s', defaultersFilePath)
        return 0

    with open(defaultersFilePath) as dfd:
        # Strip contents; Split lines; Skip header; Strip each row; Convert to lowercase
        emails = [ e.strip().lower() for e in dfd.read().strip().split('\n')[1:]]

    if not emails:
        return 0

    yn = raw_input('Cancel %s defaulters from attending webinar %s? (Y/N) > '%(len(emails), zoomWebinarId))
    if yn.upper().strip() not in ('Y', 'YES'):
        return 0

    r2c = getRegistrantsToUpdateStatus(zoomWebinarId, emails)
    ret = cancelRegistrants(zoomWebinarId, r2c)
    _logger.info('ret=%s', ret)

    return len(r2c)

