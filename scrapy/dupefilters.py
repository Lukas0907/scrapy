import logging
import warnings

from scrapy.utils.deprecate import ScrapyDeprecationWarning
from scrapy.utils.job import persister_from_settings, DiskPersister
from scrapy.utils.request import referer_str, request_fingerprint


class BaseDupeFilter:

    @classmethod
    def from_settings(cls, settings):
        return cls()

    def request_seen(self, request):
        return False

    def open(self):  # can return deferred
        pass

    def close(self, reason):  # can return a deferred
        pass

    def log(self, request, spider):  # log that a request has been filtered
        pass


class RFPDupeFilter(BaseDupeFilter):
    """Request Fingerprint duplicates filter"""

    def __init__(self, path=None, debug=False, persister=None):
        self.file = None
        self.fingerprints = set()
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if path is not None:
            warnings.warn("Setting the 'path' argument is deprecated. Please create "
                          "a scrapy.utils.job.DiskPersister object and set the "
                          "'persister' argument instead.",
                          ScrapyDeprecationWarning,
                          stacklevel=2)
            self.persister = DiskPersister(path)
        else:
            self.persister = persister
        if persister:
            self.fingerprints.update(
                self.persister.get('requests.seen', fallback=b'')
                .decode('ascii')
                .split('\n')
            )

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        persister = persister_from_settings(settings)
        return cls(debug=debug, persister=persister)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)
        if fp in self.fingerprints:
            return True
        self.fingerprints.add(fp)
        if self.persister:
            self.persister.append('requests.seen', (fp + '\n').encode('ascii'))

    def request_fingerprint(self, request):
        return request_fingerprint(request)

    def close(self, reason):
        if self.persister:
            self.persister.close('requests.seen')

    def log(self, request, spider):
        if self.debug:
            msg = "Filtered duplicate request: %(request)s (referer: %(referer)s)"
            args = {'request': request, 'referer': referer_str(request)}
            self.logger.debug(msg, args, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request: %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)
