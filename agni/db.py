import sqlite3

from utils.configuration import getDbFile

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

def getConnection():
    dbfile = getDbFile()
    return sqlite3.connect(dbfile)
