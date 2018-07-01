from utils.logger import getAgniLogger

_logger = getAgniLogger(__name__)

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