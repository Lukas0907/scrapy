import os
from abc import ABC, abstractmethod

from scrapy.exceptions import NotConfigured
from scrapy.utils.misc import create_instance, load_object


def job_dir(settings):
    return settings['JOBDIR']


class Persister(ABC):

    @abstractmethod
    def get(self, key, fallback=None):
        pass

    @abstractmethod
    def set(self, key, value):
        pass

    @abstractmethod
    def exists(self, key):
        pass

    @abstractmethod
    def append(self, key, value):
        pass

    @abstractmethod
    def remove(self, key):
        pass

    @abstractmethod
    def close(self, key):
        pass

    def _key_path(self, key):
        return os.path.join(self.jobdir, key)


class DiskPersister(Persister):

    @classmethod
    def from_settings(cls, settings):
        jobdir = job_dir(settings)
        if not jobdir:
            raise NotConfigured
        return cls(jobdir=jobdir)

    def __init__(self, jobdir):
        self.jobdir = jobdir
        self._files = {}

    def get(self, key, fallback=None):
        if self.exists(key):
            with open(self._key_path(key), 'rb') as f:
                return f.read()
        else:
            return fallback

    def set(self, key, value):
        os.makedirs(os.path.dirname(self._key_path(key)), exist_ok=True)
        with open(self._key_path(key), 'wb') as f:
            f.write(value)

    def exists(self, key):
        return os.path.exists(self._key_path(key))

    def append(self, key, value):
        if key not in self._files:
            os.makedirs(os.path.dirname(self._key_path(key)), exist_ok=True)
            self._files[key] = open(self._key_path(key), 'ab+')

        self._files[key].write(value)

    def remove(self, key):
        os.unlink(self._key_path(key))
        try:
            # Remove the parent directory if it's empty.
            os.rmdir(os.path.dirname(self._key_path(key)))
        except (FileNotFoundError, OSError):
            pass

    def close(self, key):
        if key in self._files:
            self._files[key].close()
            del self._files[key]


def persister_from_settings(settings):
    persister_cls = settings['JOB_PERSISTER']
    if not persister_cls:
        return None

    try:
        return create_instance(load_object(persister_cls), settings, crawler=None)
    except NotConfigured:
        return None
