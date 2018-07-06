import base64
import hashlib
import hmac
import json

from datetime import datetime, timedelta
from time import time

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


def generateJwtToken(api_key, api_secret, current_timestamp, expiry):
    header = {
        'alg': 'HS256',
        'typ': 'JWT',
    }
    header_json = json.dumps(header)
    _logger.debug('Header json: %s', header_json)

    claim = {
        'iss': api_key,
        'iat': current_timestamp,
        'exp': expiry,
    }
    claim_json = json.dumps(claim)
    _logger.debug('Claim json: %s', claim_json)

    headerEncoded = base64.urlsafe_b64encode(header_json).replace('=', '')
    claimEncoded = base64.urlsafe_b64encode(claim_json).replace('=', '')

    segments = [headerEncoded, claimEncoded]

    signatureInput = '.'.join(segments)

    signature = base64.urlsafe_b64encode(
        hmac.new(api_secret, msg=signatureInput, digestmod=hashlib.sha256).digest()
    ).replace('=', '')
    segments.append(signature)

    return '.'.join(segments)


def decodeJwtToken(token):
    segments = token.split('.')
    segments = [ s + '='*((4 - len(s)%4)%4) for s in segments]
    segments = [base64.urlsafe_b64decode(s) for s in segments]

    header = json.loads(segments[0])
    claim = json.loads(segments[1])
    sign = segments[2]

    return header, claim, sign

TOKEN_LIFETIME_INPUT_PROMPT = '''
Need token lifetime in <number><unit> format.
Examples:
1d = 1 day
2h = 2 hours
3m = 3 minutes
4s = 4 seconds
Enter token lifetime>
'''.strip()

LIFETIME_SECONDS = {'d':86400, 'h':3600, 'm':60, 's':1}

def askAndMakeZoomApiToken():
    token_life_input = raw_input(TOKEN_LIFETIME_INPUT_PROMPT).strip().lower()
    try:
        token_life = int(token_life_input[:-1])*LIFETIME_SECONDS[token_life_input[-1]]
    except:
        _logger.error('Invalid token lifetime: %s', token_life_input)
        return
    now = int(time())
    expDt = datetime.now() + timedelta(seconds=token_life)
    exp = now + token_life

    print "Here's your new token:\n\n%s\n"%(generateJwtToken(
        agni_configuration.getZoomApiKey(),
        agni_configuration.getZoomApiSecret(),
        now,
        exp
    ))
    print "The token is valid till %s\n"%(expDt)


class ZoomApi:
    def __init__(self):
        self._baseUrl = agni_configuration.getZoomApiBaseUrl()
        self._api_key = agni_configuration.getZoomApiKey()
        self._api_secret = agni_configuration.getZoomApiSecret()
        self._access_token = agni_configuration.getZoomApiToken()
        self._token_expiry = None

    @property
    def accessToken(self):
        now = int(time())
        if self._token_expiry is None:
            if self._access_token:
                h, c, s = decodeJwtToken(self._access_token)
                self._token_expiry = c.get('exp')
        if self._token_expiry:
            if self._token_expiry - now > 10:
                return self._access_token
        _logger.info('No Zoom access token or it is nearing expiry. Generating new access token.')
        self._token_expiry = now + 300 # Five minutes expiry
        self._access_token = generateJwtToken(self._api_key, self._api_secret, now, self._token_expiry)
        return self._access_token

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
            PARAM_ACCESS_TOKEN: self.accessToken,
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
        # _logger.debug('Got zoom api response: %s', resp_json)
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
            PARAM_ACCESS_TOKEN: self.accessToken,
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
            #_logger.debug('updateWebinarRegistrantsStatus: json-requestBody = %s', requestBody)
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
                #_logger.debug('updateWebinarRegistrantsStatus: requestBody = %s', requestBody)
                resp = requests.put(requestUrl, params=requestQuery, json=requestBody)
                self.checkResponse(resp)
                # resp_json = resp.json()
                # _logger.debug('Got zoom api response: %s', resp_json)
                respList.append(resp)

        return respList

def main():
    import sys
    try:
        now = int(time())
        genToken = generateJwtToken(sys.argv[1], sys.argv[2], now, now+300)
        print genToken

        header, claim ,sign = decodeJwtToken(genToken)
        print 'Expiry:', claim.get('exp')
    except:
        _logger.exception('Usage: python %s api-key api-secret', __file__)

if __name__ == '__main__':
    main()
