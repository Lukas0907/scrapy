import os
import pickle
import warnings

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.utils.deprecate import ScrapyDeprecationWarning
from scrapy.utils.job import persister_from_settings, DiskPersister


class SpiderState:
    """Store and load spider state during a scraping job"""

    def __init__(self, jobdir=None, persister=None):
        if jobdir is not None:
            warnings.warn("Setting the 'jobdir' argument is deprecated. Please create "
                          "a scrapy.utils.job.DiskPersister object and set the "
                          "'persister' argument instead.",
                          ScrapyDeprecationWarning,
                          stacklevel=2)
            self.persister = DiskPersister(jobdir)
        else:
            self.persister = persister

    @classmethod
    def from_crawler(cls, crawler):
        persister = persister_from_settings(crawler.settings)
        if not persister:
            raise NotConfigured

        obj = cls(persister=persister)
        crawler.signals.connect(obj.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(obj.spider_opened, signal=signals.spider_opened)
        return obj

    def spider_closed(self, spider):
        if self.persister:
            self.persister.set('spider.state', pickle.dumps(spider.state, protocol=4))

    def spider_opened(self, spider):
        if self.persister and self.persister.exists('spider.state'):
            spider.state = pickle.loads(self.persister.get('spider.state'))
        else:
            spider.state = {}

    @property
    def statefn(self):
        warnings.warn("Accessing the 'statefn' property is deprecated. Please use "
                      "the 'persister' argument instead.",
                      ScrapyDeprecationWarning,
                      stacklevel=2)
        return os.path.join(self.persister.jobdir, 'spider.state')
