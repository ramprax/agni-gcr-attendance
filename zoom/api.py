import json

import requests

from utils.configuration import agni_configuration
from utils.logger import getAgniLogger

_logger = getAgniLogger(__name__)

PARAM_ACCESS_TOKEN = 'access_token'

PARAM_STATUS = 'status'
PARAM_PAGE_SIZE = 'page_size'
PARAM_PAGE_NUMBER = 'page_number'
PARAM_OCCURRENCE_ID = 'occurrence_id'
PARAM_NEXT_PAGE_TOKEN = 'next_page_token'

PARAM_ACTION = 'action'
ACTION_ALLOW = 'allow'
ACTION_CANCEL = 'cancel'
ACTION_DENY = 'deny'

PARAM_REGISTRANTS = 'registrants'
MAX_REGISTRANTS_PER_CALL = 30

ENDPOINT_WEBINARS = 'webinars'
ENDPOINT_WEBINAR_REGISRANTS = 'webinars/{zoomWebinarId}/registrants'
ENDPOINT_UPDATE_WEBINAR_REGISTRANTS_STATUS = ENDPOINT_WEBINAR_REGISRANTS + '/status'

class ZoomApiError(Exception):
    pass

class ZoomApi:
    def __init__(self):
        self._baseUrl = agni_configuration.getZoomApiBaseUrl()
        self._access_token = agni_configuration.getZoomApiToken()

    def checkResponse(self, resp):
        _logger.info('Zoom api returned status code %s', resp.status_code)
        if 200 <= resp.status_code < 300:
            return
        # {u'message': u'Invalid access token.', u'code': 124}
        resp_json = None
        try:
            resp_json = resp.json()
        except:
            pass
        raise ZoomApiError('Zoom api error: status_code=%s json=%s'%(resp.status_code, resp_json), resp.status_code, resp_json)

    def getWebinarRegistrants(self, zoomWebinarId, status='approved', page_size=300, page_number=1,
                              occurrence_id=None, next_page_token=None):
        requestUrl = (self._baseUrl + ENDPOINT_WEBINAR_REGISRANTS).format(
            zoomWebinarId=zoomWebinarId
        )
        requestQuery = {
            PARAM_ACCESS_TOKEN: self._access_token,
            PARAM_STATUS: status,
            PARAM_PAGE_SIZE: page_size,
            PARAM_PAGE_NUMBER: page_number,
        }
        requestBody = {
        }
        if occurrence_id:
            requestQuery[PARAM_OCCURRENCE_ID] = occurrence_id
        if next_page_token:
            requestQuery[PARAM_NEXT_PAGE_TOKEN] = next_page_token

        resp = requests.get(requestUrl, params=requestQuery, data=requestBody)
        self.checkResponse(resp)
        resp_json = resp.json()
        _logger.debug('Got zoom api response: %s', resp_json)
        return resp.json()

    def getAllWebinarRegistrants(self, zoomWebinarId, status='approved', occurrence_id=None):
        allRegistrants = []

        data = self.getWebinarRegistrants(zoomWebinarId, status=status, occurrence_id=occurrence_id)
        page_count = data['page_count']
        total_records = data['total_records']
        next_page_token = data['next_page_token']
        registrants = data['registrants']
        allRegistrants += registrants
        page_number = 1

        if len(registrants) < total_records:
            while page_number < page_count:
                data = self.getWebinarRegistrants(zoomWebinarId, status=status,
                                                  occurrence_id=occurrence_id, next_page_token=next_page_token)
                next_page_token = data['next_page_token']
                registrants = data['registrants']
                allRegistrants += registrants
                page_number += 1

        return allRegistrants

    def updateWebinarRegistrantsStatus(self, zoomWebinarId, action, registrants=None, occurrence_id=None):
        if not registrants:
            registrants = []
        requestUrl = (self._baseUrl + ENDPOINT_UPDATE_WEBINAR_REGISTRANTS_STATUS).format(
            zoomWebinarId=zoomWebinarId
        )
        requestQuery = {
            PARAM_ACCESS_TOKEN: self._access_token,
        }
        if occurrence_id:
            requestQuery[PARAM_OCCURRENCE_ID] = occurrence_id

        requestBody = {
            PARAM_ACTION: action
        }

        respList = []
        if len(registrants) <= MAX_REGISTRANTS_PER_CALL:
            if registrants:
                requestBody[PARAM_REGISTRANTS] = registrants

            #json.dumps(requestBody)
            #_logger.debug('updateWebinarRegistrantsStatus: requestBody = %s', requestBody)
            _logger.debug('updateWebinarRegistrantsStatus: json-requestBody = %s', requestBody)
            resp = requests.put(requestUrl, params=requestQuery, json=requestBody)
            self.checkResponse(resp)
            # resp_json = resp.json()
            # _logger.debug('Got zoom api response: %s', resp_json)
            respList.append(resp)
        else:
            for i in xrange(0, len(registrants), MAX_REGISTRANTS_PER_CALL):
                startIdx = i
                endIdx = (i+MAX_REGISTRANTS_PER_CALL) if (i+MAX_REGISTRANTS_PER_CALL) < len(registrants) else len(registrants)
                requestBody[PARAM_REGISTRANTS] = registrants[startIdx:endIdx]
                _logger.debug('updateWebinarRegistrantsStatus: requestBody = %s', requestBody)
                resp = requests.put(requestUrl, params=requestQuery, json=requestBody)
                self.checkResponse(resp)
                # resp_json = resp.json()
                # _logger.debug('Got zoom api response: %s', resp_json)
                respList.append(resp)

        return respList
