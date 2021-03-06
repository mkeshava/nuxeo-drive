"""Common Nuxeo Automation client utilities."""

import sys
import base64
import json
import urllib2
import random
import time
import os
import hashlib
import tempfile
import itertools
from urllib import urlencode
from PyQt4.QtCore import QCoreApplication
from poster.streaminghttp import get_handlers
from nxdrive.logging_config import get_logger
from nxdrive.client.common import BaseClient
from nxdrive.client.common import DEFAULT_REPOSITORY_NAME
from nxdrive.client.common import FILE_BUFFER_SIZE_NO_RATE_LIMIT, FILE_BUFFER_SIZE_WITH_RATE_LIMIT
from nxdrive.client.common import DEFAULT_IGNORED_PREFIXES
from nxdrive.client.common import DEFAULT_IGNORED_SUFFIXES
from nxdrive.client.common import safe_filename
from nxdrive.engine.activity import Action, FileAction
from nxdrive.utils import DEVICE_DESCRIPTIONS
from nxdrive.utils import TOKEN_PERMISSION
from nxdrive.utils import guess_mime_type
from nxdrive.utils import guess_digest_algorithm
from nxdrive.utils import force_decode
from urllib2 import ProxyHandler
from urlparse import urlparse
import socket
import threading
import math
from collections import defaultdict


log = None

CHANGE_SUMMARY_OPERATION = 'NuxeoDrive.GetChangeSummary'
DEFAULT_NUXEO_TX_TIMEOUT = 300

DOWNLOAD_TMP_FILE_PREFIX = '.'
DOWNLOAD_TMP_FILE_SUFFIX = '.nxpart'

# 1s audit time resolution because of the datetime resolution of MYSQL
AUDIT_CHANGE_FINDER_TIME_RESOLUTION = 1.0

LESS_THAN_300KBS = 300
LESS_THAN_1000KBS = 1000
LESS_THAN_10000KBS = 10000
LESS_THAN_100000KBS = 100000
NO_LIMIT = -1

DM_READ_TIMEOUT = 20000  # 20 sec

FILE_BUFFER_SIZES = {
    LESS_THAN_1000KBS: FILE_BUFFER_SIZE_WITH_RATE_LIMIT,
    LESS_THAN_10000KBS: FILE_BUFFER_SIZE_WITH_RATE_LIMIT * 2,
    LESS_THAN_100000KBS: FILE_BUFFER_SIZE_WITH_RATE_LIMIT * 4,
    NO_LIMIT: FILE_BUFFER_SIZE_NO_RATE_LIMIT
}

RATE_STEPS = (LESS_THAN_300KBS, LESS_THAN_1000KBS)

NUMBER_OF_PROCESSORS = {
    (LESS_THAN_300KBS, LESS_THAN_300KBS): (2, 2, 0),
    (LESS_THAN_300KBS, LESS_THAN_1000KBS): (2, 5, 0),
    (LESS_THAN_300KBS, NO_LIMIT): (2, 11, 0),
    (LESS_THAN_1000KBS, LESS_THAN_300KBS): (5, 2, 0),
    (LESS_THAN_1000KBS, LESS_THAN_1000KBS): (5, 5, 0),
    (LESS_THAN_1000KBS, NO_LIMIT): (5, 11, 0),
    (NO_LIMIT, LESS_THAN_300KBS): (11, 2, 0),
    (NO_LIMIT, LESS_THAN_1000KBS): (11, 5, 0),
    (NO_LIMIT, NO_LIMIT): (0, 0, 12)
}

MAX_NUMBER_PROCESSORS = max([sum(v) for v in NUMBER_OF_PROCESSORS.values()])


def _get_rate_step(rate):
    for step in RATE_STEPS:
        if rate != NO_LIMIT and rate <= step:
            result = step
            break
    else:
        result = NO_LIMIT
    return result


def get_file_buffer_size(rate):
    rates = list(FILE_BUFFER_SIZES.keys())
    rates.sort()
    for r in rates:
        if rate <= r:
            size = FILE_BUFFER_SIZES[r]
            break
    else:
        size = FILE_BUFFER_SIZES[NO_LIMIT]
    return size


def get_number_of_processors(upload_rate, download_rate):
    key = (_get_rate_step(upload_rate), _get_rate_step(download_rate))
    try:
        return NUMBER_OF_PROCESSORS[key]
    except KeyError:
        return (0,0,0)


socket.setdefaulttimeout(DEFAULT_NUXEO_TX_TIMEOUT)


class InvalidBatchException(Exception):
    if (log is not None):
        log.warning("Invalid batch exception")
    
    def __str__(self):
        return "Invalid batch exception"


def get_proxies_for_handler(proxy_settings):
    """Return a pair containing proxy string and exceptions list"""
    if proxy_settings.config == 'None':
        # No proxy, return an empty dictionary to disable
        # default proxy detection
        return {}, None
    elif proxy_settings.config == 'System':
        # System proxy, return None to use default proxy detection
        return None, None
    else:
        # Manual proxy settings, build proxy string and exceptions list
        if proxy_settings.authenticated:
            proxy_string = ("%s:%s@%s:%s") % (
                proxy_settings.username,
                proxy_settings.password,
                proxy_settings.server,
                proxy_settings.port)
        else:
            proxy_string = ("%s:%s") % (
                proxy_settings.server,
                proxy_settings.port)
        if proxy_settings.proxy_type is None:
            proxies = {'http': proxy_string, 'https': proxy_string}
        else:
            proxies = {proxy_settings.proxy_type: ("%s://%s" % (proxy_settings.proxy_type, proxy_string))}
        if proxy_settings.exceptions and proxy_settings.exceptions.strip():
            proxy_exceptions = [e.strip() for e in
                                proxy_settings.exceptions.split(',')]
        else:
            proxy_exceptions = None
        return proxies, proxy_exceptions


def get_proxy_config(proxies):
    if proxies is None:
        return 'System'
    elif proxies == {}:
        return 'None'
    else:
        return 'Manual'


def get_proxy_handler(proxies, proxy_exceptions=None, url=None):
    if proxies is None:
        # No proxies specified, use default proxy detection
        return urllib2.ProxyHandler()
    else:
        # Use specified proxies (can be empty to disable default detection)
        if proxies:
            if proxy_exceptions is not None and url is not None:
                hostname = urlparse(url).hostname
                for exception in proxy_exceptions:
                    if exception == hostname:
                        # Server URL is in proxy exceptions,
                        # don't use any proxy
                        proxies = {}
        return urllib2.ProxyHandler(proxies)


def get_opener_proxies(opener):
    for handler in opener.handlers:
        if isinstance(handler, ProxyHandler):
            return handler.proxies
    return None


class AddonNotInstalled(Exception):
    pass


class NewUploadAPINotAvailable(Exception):
    pass


class CorruptedFile(Exception):
    pass


class Unauthorized(Exception):
    def __init__(self, server_url, user_id, code=403):
        self.server_url = server_url
        self.user_id = user_id
        self.code = code

    def __str__(self):
        return ("'%s' is not authorized to access '%s' with"
                " the provided credentials" % (self.user_id, self.server_url))


class TokenStrategy(object):
    def begin(self, thread):
        pass

    def end(self, thread_id):
        pass

    def update(self, thread):
        pass

    def reset(self, thread):
        pass

    def is_next(self, thread):
        return True

    def get_count(self, thread):
        return 0

    def get_last_access_time(self, thread):
        return None

    def get_wait_time(self, thread):
        return None


class NoStrategy(TokenStrategy):
    def __init__(self, name, log=None):
        self.name = name
        self.log = log
        self.wait_time = defaultdict(int)
        self.last_access_time = defaultdict(object)

    def begin(self, thread):
        now = time.time()
        try:
            if thread not in self.last_access_time:
                self.last_access_time[thread] = (now, 0)
                if self.log:
                    self.log.trace("[bucket '%s', thread %d]: +1(%d)", self.name, thread.ident, len(self.last_access_time))
        except KeyError:
            pass

    def end(self, thread_id):
        thread = filter(lambda x: x.ident == thread_id, self.last_access_time.keys())
        if not thread:
            return

        try:
            del self.last_access_time[thread[0]]
            if self.log:
                self.log.trace("[bucket '%s', thread %d]: -1(%d)", self.name, thread_id, len(self.last_access_time))
            # thread terminated, clean up other thread stats
            del self.wait_time[thread[0]]
        except KeyError:
            pass

    def update(self, thread, ready=False):
        # increment access count
        try:
            self.last_access_time[thread] = (self.last_access_time[thread][0], self.last_access_time[thread][1]+1)
            if ready:
                now = time.time()
                # update max wait time and reset access count
                self.wait_time[thread] = max(self.wait_time[thread], now - self.last_access_time[thread][0])
                self.last_access_time[thread] = (now, 0)
                if self.log:
                    self.log.trace("[bucket '%s', thread %d]: max wait time was %3.2f",
                          self.name, thread.ident, self.wait_time[thread])
                if self.wait_time[thread] >= DM_READ_TIMEOUT:
                    if self.log:
                        self.log.warn("bucket '%s': thread %d waited more than %d (%3.2f)",
                             self.name, thread.ident, DM_READ_TIMEOUT / 1000, self.wait_time[thread] / 1000.0)
        except KeyError:
            pass

    def reset(self, thread):
        try:
            if thread in self.last_access_time:
                self.last_access_time[thread] = (time.time(), 0)
                self.wait_time[thread] = 0
        except KeyError:
            pass

    def get_count(self, thread):
        try:
            return self.last_access_time[thread][1]
        except KeyError:
            return 0

    def get_last_access_time(self, thread):
        try:
            return self.last_access_time[thread][0]
        except KeyError:
            return None

    def get_wait_time(self, thread):
        try:
            return self.wait_time[thread]
        except KeyError:
            return None


class RoundRobinStrategy(TokenStrategy):
    def __init__(self, name, log=None):
        self.name = name
        self.log = log
        self.threads = list()
        self.wait_time = defaultdict(int)
        self.last_access_time = defaultdict(object)
        self.round_robin = None
        self.current = None

    def begin(self, thread):
        now = time.time()
        try:
            if thread not in self.last_access_time:
                self.last_access_time[thread] = (now, 0)
            if thread not in self.threads:
                self.threads.append(thread)
                if self.log:
                    self.log.trace("[thread %d] in begin: +1(%d)", thread.ident, len(self.threads))
                self.round_robin = itertools.cycle(self.threads)
                self.current = self.round_robin.next()
        except KeyError:
            pass

    def end(self, thread_id):
        thread = filter(lambda x: x.ident == thread_id, self.threads)
        if not thread:
            return

        try:
            self.threads.remove([thread[0]])
            if self.log:
                self.log.trace("[thread %d]in end: -1(%d)", thread_id, len(self.threads))
            if self.threads:
                self.round_robin = itertools.cycle(self.threads)
                self.current = self.round_robin.next()
            else:
                self.current = None
            # thread terminated, clean up other thread stats
            del self.wait_time[thread[0]]
            del self.last_access_time[thread[0]]
        except (KeyError, ValueError) as e:
            pass

    def update(self, thread, ready=False):
        # increment access count
        try:
            self.last_access_time[thread] = (self.last_access_time[thread][0], self.last_access_time[thread][1]+1)
            if self.log:
                self.log.trace("[thread %d] in update: next thread: %d", thread.ident, self.current.ident)
            if ready:
                now = time.time()
                # update max wait time and reset access count
                self.wait_time[thread] = max(self.wait_time[thread], now - self.last_access_time[thread][0])
                self.last_access_time[thread] = (now, 0)
                if self.log:
                    self.log.trace("[thread %d] in update: max wait time was %3.2f",
                          thread.ident, self.wait_time[thread])
                if self.wait_time[thread] >= DM_READ_TIMEOUT:
                    if self.log:
                        self.log.warn("[thread %d] in update: waited more than %d (%3.2f)",
                             thread.ident, DM_READ_TIMEOUT / 1000, self.wait_time[thread] / 1000.0)
        except KeyError:
            pass

    def reset(self, thread):
        try:
            if thread in self.last_access_time:
                self.last_access_time[thread] = (time.time(), 0)
                self.wait_time[thread] = 0
        except KeyError:
            pass

    def is_next(self, thread):
        result = (thread.ident == self.current.ident)
        self.current = self.round_robin.next()
        if self.log:
            self.log.trace("[thread %d] in is_next: current thread %d - %s", thread.ident, self.current.ident,
                           'ok' if result else 'not ok')
        return result

    def get_count(self, thread):
        try:
            return self.last_access_time[thread][1]
        except KeyError:
            return 0

    def get_last_access_time(self, thread):
        try:
            return self.last_access_time[thread][0]
        except KeyError:
            return None

    def get_wait_time(self, thread):
        try:
            return self.wait_time[thread]
        except KeyError:
            return None


class WaitPriorityStrategy(TokenStrategy):
    MAX_COUNT = 50

    def __init__(self, name, log=None):
        self.name = name
        self.log = log
        self.last_access_time = dict()

    def begin(self, thread):
        try:
            if thread not in self.last_access_time:
                if self.log:
                    self.log.trace("[bucket '%s', thread %d]: +1(%d)", self.name, thread.ident, len(self.last_access_time)+1)
                self.last_access_time[thread] = (time.time(), 0)
        except KeyError:
            pass

    def end(self, thread_id):
        threads = filter(lambda x: x == thread_id, self.last_access_time.keys())
        try:
            if threads:
                del self.last_access_time[threads[0]]
                if self.log:
                    self.log.trace("[bucket '%s', thread %d]: -1(%d)", self.name, threads[0].ident, len(self.last_access_time))
        except KeyError:
            pass

    def update(self, thread, ready=False):
        now = time.time()
        try:
            wait_time = now - self.last_access_time[thread][0]
            next_thread = self._get_max(now)
            if self.log:
                self.log.trace("[bucket '%s', thread %d]: next thread up: %d", self.name, thread.ident, next_thread.ident)
            if ready:
                self.last_access_time[thread] = (now, 0)
                if self.log:
                    self.log.trace("[bucket '%s', thread %d]: max wait time was %3.2f", self.name, thread.ident, wait_time / 1000.0)
                if wait_time >= DM_READ_TIMEOUT:
                    if self.log:
                        self.log.warn("bucket '%s': thread %d waited more than %d (%3.2f)",
                             self.name, thread.ident, DM_READ_TIMEOUT / 1000, wait_time / 1000.0)
        except KeyError:
            pass

    def reset(self, thread):
        try:
            if thread in self.last_access_time:
                self.last_access_time[thread] = (0, 0)
        except KeyError:
            pass

    def is_next(self, thread):
        now = time.time()
        try:
            if len(self.last_access_time) == 1:
                self.last_access_time[thread] = (now, 0)
                return True
            next_thread = self._get_max(now)
            # increment selection counter and update the wait time
            self.last_access_time[thread] = (self.last_access_time[thread][0], self.last_access_time[thread][1]+1)
            result = (thread.ident == next_thread.ident)
            if result:
                self.last_access_time[thread] = (now, 0)
            else:
                if self.last_access_time[thread][1] > self.MAX_COUNT:
                    # thread failed too many times, reset max waiting thread
                    self.last_access_time[next_thread] = (now, 0)
            return result
        except KeyError:
            return False

    def _get_max(self, now):
        if len(self.last_access_time) == 1:
            return self.last_access_time.keys()[0]
        max_wait = 0
        max_thread = None
        for thread, last_access in self.last_access_time.items():
            if last_access[0] > 0 and now - last_access[0] > max_wait:
                max_wait = now - last_access[0]
                max_thread = thread
        return max_thread

    def get_count(self, thread):
        try:
            return self.last_access_time[thread][1]
        except KeyError:
            return 0

    def get_last_access_time(self, thread):
        try:
            return self.last_access_time[thread][0]
        except KeyError:
            return None


class TokenBucket(object):
    """An implementation of the token bucket algorithm.

        bucket = TokenBucket(80, 0.5)
        print bucket.consume(10)
    True
        print bucket.consume(90)
    False
    """

    SMA_SIZE = 5  # moving average over the last 5 processors

    def __init__(self, fill_rate, name=None, factory=TokenStrategy):
        """tokens is the total tokens in the bucket. fill_rate is the
        rate in tokens/second that the bucket will be refilled."""
        assert fill_rate > 0 or fill_rate == NO_LIMIT, 'fill rate must be greater than 0 or unlimited (-1)'
        self.name = name or 'unknown'
        self.strategy = factory(name)
        self.capacity = 0
        self._tokens = 0
        self.rates = []
        self.fill_rate = float(fill_rate)
        self.timestamp = time.time()
        self.avg_rate = 0.
        self.lock = threading.RLock()

    def _set_capacity(self, tokens):
        assert tokens > 0 or tokens == NO_LIMIT, 'capacity must be greater than 0 or unlimited (-1)'
        self.capacity = float(tokens)
        self._tokens = float(tokens)
        self.min_delay = 0.001
        method_name = 'get_' + self.name + '_buffer'
        if hasattr(BaseAutomationClient, method_name):
            method = getattr(BaseAutomationClient, method_name, None)
            buf_size = method()
            self.min_delay = buf_size / (self.get_fill_rate() * 1000.0)

    def consume(self, tokens):
        """Consume tokens from the bucket.
        Returns 0 if there were sufficient tokens otherwise
        the expected time until enough tokens become available."""
        assert tokens >= 0, 'requested tokens must 0 or more'
        if self.capacity == float(NO_LIMIT):
            return 0
        with self.lock:
            existing_tokens = self._tokens
            available_tokens = min(tokens, self.tokens)
            thread = threading.currentThread()
            self.strategy.begin(thread)
            expected_time = (tokens - available_tokens) / self.fill_rate
            count = -1
            if not self.strategy.is_next(thread):
                expected_time = max(expected_time, self.min_delay)

            if expected_time <= 0:
                self._tokens -= available_tokens

            # next thread up
            self.strategy.update(thread, ready=expected_time <= 0)

            log.trace("[bucket '%s', thread '%d']: requested tokens: %d, existing: %d, current available: %d, %swait%s",
                      self.name, thread.ident, tokens, existing_tokens, available_tokens,
                      'no ' if expected_time <= 0 else '', ': ' + str(expected_time) + 's' if expected_time > 0 else '')
            return max(0, expected_time)

    def update_rate(self, stats):
        '''
        Compute a moving average of last SMA_SIZE file transfer rates or a simple average if not enough samples
        '''
        with self.lock:
            if stats.avg_file_rate > 0:
                self.rates.append(stats.avg_file_rate)
                if len(self.rates) < TokenBucket.SMA_SIZE + 1:
                    # compute average
                    if self.rates:
                        self.avg_rate += (stats.avg_file_rate - self.avg_rate) / len(self.rates)
                else:
                    # compute a Simple Moving Average
                    if self.rates:
                        self.avg_rate = self.avg_rate + (stats.avg_file_rate - self.rates[0]) / TokenBucket.SMA_SIZE
                        self.rates.pop(0)

                log.trace("%s average rate is %d", self.name, self.avg_rate)

    @property
    def tokens(self):
        self.lock.acquire()
        if self._tokens < self.capacity:
            now = time.time()
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
            log.trace('added %d tokens', delta)
        value = self._tokens
        self.lock.release()
        log.trace('current tokens: %d', value)
        return value

    def get_fill_rate(self):
        with self.lock:
            return float(self.fill_rate)

    def get_average_rate(self):
        with self.lock:
            return float(self.avg_rate)

    def clear(self, thread_id):
        # thread terminated, reset the strategy data
        self.strategy.end(thread_id)

    def _reset(self):
        self.strategy.reset(threading.currentThread())

    def __str__(self):
        with self.lock:
            return "token bucket=%s, capacity=%d, fill rate=%d, average file transfer rate=%d" % \
                   (self.name, self.capacity, self.fill_rate, self.avg_rate)


class FileTransferStats(object):
    class Stats(object):
        SMA_SIZE = 10  # moving average over last 5 files

        def __init__(self):
            self.reset()

        def reset(self):
            self.rates = []
            self.instant_rate = 0
            self.avg_file_rate = 0
            self.size = 0
            self.total = 0
            self.last_update = 0
            self.start_date = 0
            self.filename = ''

        def in_progress(self):
            return self.size > 0

        def start(self, total_size=0, filename=''):
            self.instant_rate = 0
            self.avg_file_rate = 0
            self.size = 0
            self.total = total_size
            self.filename = filename
            self.last_update = self.start_date

        def end(self):
            self.rates.append(self.instant_rate)
            if len(self.rates) < FileTransferStats.Stats.SMA_SIZE + 1:
                # compute average for file transfer rate
                self.avg_file_rate += (self.instant_rate - self.avg_file_rate) / len(self.rates)
            else:
                # compute a Simple Moving Average for file transfer rate
                self.avg_file_rate = self.avg_file_rate + (self.instant_rate - self.rates[0]) / TokenBucket.SMA_SIZE
                self.rates.pop(0)

        def update(self, size):
            if size > 0:
                self.size += size
                now = time.time()
                delta = now - self.last_update
                self.last_update = now
                if delta > 0:
                    self.instant_rate = size / (delta * 1000.0)

        def get_percent_rate(self):
            return (100. * self.size / self.total) if self.total > 0 else None

        def get_average_rate(self):
            return float(self.avg_file_rate)

        def get_instant_rate(self):
            return float(self.instant_rate)

        def __str__(self):
            return "filename %s, size/total %d/%d, average file transfer rate %d, instant rate=%d" % \
                   (self.filename, self.size, self.total, self.avg_file_rate, self.instant_rate)

    def __init__(self, name=''):
        self.name = name or 'unknown'
        self.stats = defaultdict(FileTransferStats.Stats)
        self.lock = threading.RLock()

    def is_new_transfer(self):
        with self.lock:
            return not (self.stats.has_key(threading.currentThread().ident) and
                        self.stats[threading.currentThread().ident].in_progress())

    def update(self, size):
        with self.lock:
            self.stats[threading.currentThread().ident].update(size)

    def start(self, total_size=0, filename=''):
        with self.lock:
            self.stats[threading.currentThread().ident].start(total_size=total_size, filename=filename)

    def end(self):
        with self.lock:
            self.stats[threading.currentThread().ident].end()
        # clear last access time
        token_bucket = self._get_token_bucket()
        if token_bucket:
            token_bucket._reset()

    def reset(self):
        with self.lock:
            self.stats[threading.currentThread().ident].reset()

    def get_average_rate(self):
        with self.lock:
            return self.stats[threading.currentThread().ident].get_average_rate()

    def get_instant_rate(self):
        with self.lock:
            return self.stats[threading.currentThread().ident].get_instant_rate()

    def get_percent_transfer(self):
        with self.lock:
            return self.stats[threading.currentThread().ident].get_percent_rate()

    def get_current_size(self):
        with self.lock:
            return self.stats[threading.currentThread().ident].size

    def get_total_size(self):
        with self.lock:
            return self.stats[threading.currentThread().ident].total

    def get_filename(self):
        with self.lock:
            return self.stats[threading.currentThread().ident].filename

    def get_stats(self):
        with self.lock:
            return self.stats[threading.currentThread().ident]

    def get_stats_by_thread_id(self, thread_id):
        with self.lock:
            return self.stats[thread_id]

    def clear(self, thread_id):
        try:
            # update stats for this processor
            token_bucket = self._get_token_bucket()
            if token_bucket:
                token_bucket.update_rate(self.stats[thread_id])
            del self.stats[thread_id]
        except (IndexError, KeyError):
            pass

    def _get_token_bucket(self):
        bucket_name = self.name + '_token_bucket'
        if hasattr(BaseAutomationClient, bucket_name):
            return getattr(BaseAutomationClient, bucket_name, None)

    def __str__(self):
        return '\n'.join(['thread=%s, stats=%s' % (item[0], str(item[1])) for item in self.stats.items()])


class BaseAutomationClient(BaseClient):
    """Client for the Nuxeo Content Automation HTTP API

    timeout is a short timeout to avoid having calls to fast JSON operations
    to block and freeze the application in case of network issues.

    blob_timeout is long (or infinite) timeout dedicated to long HTTP
    requests involving a blob transfer.

    Supports HTTP proxies.
    If proxies is given, it must be a dictionary mapping protocol names to
    URLs of proxies.
    If proxies is None, uses default proxy detection:
    read the list of proxies from the environment variables <PROTOCOL>_PROXY;
    if no proxy environment variables are set, then in a Windows environment
    proxy settings are obtained from the registry's Internet Settings section,
    and in a Mac OS X environment proxy information is retrieved from the
    OS X System Configuration Framework.
    To disable autodetected proxy pass an empty dictionary.
    """
    # TODO: handle system proxy detection under Linux,
    # see https://jira.nuxeo.com/browse/NXP-12068

    # Parameters used when negotiating authentication token:
    application_name = 'Nuxeo Drive'
    upload_token_bucket = None
    download_token_bucket = None
    # download transfer stats
    download_stats = FileTransferStats(name='download')
    # upload transfer stats
    upload_stats = FileTransferStats(name='upload')

    @staticmethod
    def get_upload_rate_limit():
        return BaseAutomationClient.upload_token_bucket.get_fill_rate()

    @staticmethod
    def set_upload_rate_limit(bandwidth_limit):
        if bandwidth_limit == NO_LIMIT:
            BaseAutomationClient.upload_token_bucket = TokenBucket(NO_LIMIT, name='upload',
                                                                   factory=NoStrategy)
            BaseAutomationClient.upload_token_bucket._set_capacity(NO_LIMIT)
        else:
            BaseAutomationClient.upload_token_bucket = TokenBucket(bandwidth_limit, name='upload',
                                                                   factory=NoStrategy)
            buffer_size = BaseAutomationClient.get_upload_buffer()
            BaseAutomationClient.upload_token_bucket._set_capacity(1.1 * buffer_size / 1000)

    @staticmethod
    def get_download_rate_limit():
        return BaseAutomationClient.download_token_bucket.get_fill_rate()

    @staticmethod
    def set_download_rate_limit(bandwidth_limit):
        if bandwidth_limit == NO_LIMIT:
            BaseAutomationClient.download_token_bucket = TokenBucket(NO_LIMIT, name='download',
                                                                     factory=NoStrategy)
            BaseAutomationClient.download_token_bucket._set_capacity(NO_LIMIT)
        else:
            BaseAutomationClient.download_token_bucket = TokenBucket(bandwidth_limit, name='download',
                                                                     factory=NoStrategy)
            buffer_size = BaseAutomationClient.get_download_buffer()
            BaseAutomationClient.download_token_bucket._set_capacity(1.1 * buffer_size / 1000)

    @staticmethod
    def use_upload_rate_limit():
        return BaseAutomationClient.upload_token_bucket is not None

    @staticmethod
    def use_download_rate_limit():
        return BaseAutomationClient.download_token_bucket is not None

    def __init__(self, server_url, user_id, device_id, client_version,
                 proxies=None, proxy_exceptions=None,
                 password=None, token=None, repository=DEFAULT_REPOSITORY_NAME,
                 ignored_prefixes=None, ignored_suffixes=None,
                 timeout=20, blob_timeout=60, cookie_jar=None,
                 upload_tmp_dir=None, check_suspended=None):
        global log
        log = get_logger(__name__)
        # Function to check during long-running processing like upload /
        # download if the synchronization thread needs to be suspended
        self.check_suspended = check_suspended

        if timeout is None or timeout < 0:
            timeout = 20
        self.timeout = timeout
        # Dont allow null timeout
        if blob_timeout is None or blob_timeout < 0:
            blob_timeout = 60
        self.blob_timeout = blob_timeout
        if ignored_prefixes is not None:
            self.ignored_prefixes = ignored_prefixes
        else:
            self.ignored_prefixes = DEFAULT_IGNORED_PREFIXES

        if ignored_suffixes is not None:
            self.ignored_suffixes = ignored_suffixes
        else:
            self.ignored_suffixes = DEFAULT_IGNORED_SUFFIXES

        self.upload_tmp_dir = (upload_tmp_dir if upload_tmp_dir is not None
                               else tempfile.gettempdir())

        if not server_url.endswith('/'):
            server_url += '/'
        self.server_url = server_url

        self.repository = repository

        self.user_id = user_id
        self.device_id = device_id
        self.client_version = client_version
        self._update_auth(password=password, token=token)

        self.cookie_jar = cookie_jar
        cookie_processor = urllib2.HTTPCookieProcessor(
            cookiejar=cookie_jar)

        # Get proxy handler
        proxy_handler = get_proxy_handler(proxies,
                                          proxy_exceptions=proxy_exceptions,
                                          url=self.server_url)

        # Build URL openers
        self.opener = urllib2.build_opener(cookie_processor, proxy_handler)
        self.streaming_opener = urllib2.build_opener(cookie_processor,
                                                     proxy_handler,
                                                     *get_handlers())

        # Set Proxy flag
        self.is_proxy = False
        opener_proxies = get_opener_proxies(self.opener)
        log.trace('Proxy configuration: %s, effective proxy list: %r', get_proxy_config(proxies), opener_proxies)
        if opener_proxies:
            self.is_proxy = True

        self.automation_url = server_url + 'site/automation/'
        self.batch_upload_url = 'batch/upload'
        self.batch_execute_url = 'batch/execute'

        # New batch upload API
        self.new_upload_api_available = True
        self.rest_api_url = server_url + 'api/v1/'
        self.batch_upload_path = 'upload'

        self.fetch_api()

    def fetch_api(self):
        base_error_message = (
                                 "Failed to connect to Nuxeo server %s"
                             ) % (self.server_url)
        url = self.automation_url
        headers = self._get_common_headers()
        cookies = self._get_cookies()
        log.trace("Calling %s with headers %r and cookies %r",
                  url, headers, cookies)
        req = urllib2.Request(url, headers=headers)
        try:
            response = json.loads(self.opener.open(
                req, timeout=self.timeout).read())
        except urllib2.HTTPError as e:
            if e.code == 401 or e.code == 403:
                raise Unauthorized(self.server_url, self.user_id, e.code)
            else:
                msg = base_error_message + "\nHTTP error %d" % e.code
                if hasattr(e, 'msg'):
                    msg = msg + ": " + e.msg
                e.msg = msg
                raise e
        except urllib2.URLError as e:
            msg = base_error_message
            if hasattr(e, 'message') and e.message:
                e_msg = force_decode(": " + e.message)
                if e_msg is not None:
                    msg = msg + e_msg
            elif hasattr(e, 'reason') and e.reason:
                if (hasattr(e.reason, 'message')
                    and e.reason.message):
                    e_msg = force_decode(": " + e.reason.message)
                    if e_msg is not None:
                        msg = msg + e_msg
                elif (hasattr(e.reason, 'strerror')
                      and e.reason.strerror):
                    e_msg = force_decode(": " + e.reason.strerror)
                    if e_msg is not None:
                        msg = msg + e_msg
            if self.is_proxy:
                msg = (msg + "\nPlease check your Internet connection,"
                       + " make sure the Nuxeo server URL is valid"
                       + " and check the proxy settings.")
            else:
                msg = (msg + "\nPlease check your Internet connection"
                       + " and make sure the Nuxeo server URL is valid.")
            e.msg = msg
            raise e
        except Exception as e:
            msg = base_error_message
            if hasattr(e, 'msg'):
                msg = msg + ": " + e.msg
            e.msg = msg
            raise e
        self.operations = {}
        for operation in response["operations"]:
            self.operations[operation['id']] = operation
            op_aliases = operation.get('aliases')
            if op_aliases:
                for op_alias in op_aliases:
                    self.operations[op_alias] = operation

        # Is event log id available in change summary?
        # See https://jira.nuxeo.com/browse/NXP-14826
        change_summary_op = self._check_operation(CHANGE_SUMMARY_OPERATION)
        self.is_event_log_id = 'lowerBound' in [
            param['name'] for param in change_summary_op['params']]

    def execute(self, command, url=None, op_input=None, timeout=-1,
                check_params=True, void_op=False, extra_headers=None,
                file_out=None, **params):
        """Execute an Automation operation"""
        if check_params:
            self._check_params(command, params)

        if url is None:
            url = self.automation_url + command
        headers = {
            "Content-Type": "application/json+nxrequest",
            "Accept": "application/json+nxentity, */*",
            "X-NXproperties": "*",
            # Keep compatibility with old header name
            "X-NXDocumentProperties": "*",
        }
        if void_op:
            headers.update({"X-NXVoidOperation": "true"})
        if self.repository != DEFAULT_REPOSITORY_NAME:
            headers.update({"X-NXRepository": self.repository})
        if extra_headers is not None:
            headers.update(extra_headers)
        headers.update(self._get_common_headers())

        json_struct = {'params': {}}
        for k, v in params.items():
            if v is None:
                continue
            if k == 'properties':
                s = ""
                for propname, propvalue in v.items():
                    s += "%s=%s\n" % (propname, propvalue)
                json_struct['params'][k] = s.strip()
            else:
                json_struct['params'][k] = v
        if op_input:
            json_struct['input'] = op_input
        data = json.dumps(json_struct)

        cookies = self._get_cookies()
        log.trace("Calling %s with headers %r, cookies %r"
                  " and JSON payload %r",
                  url, headers, cookies, data)
        req = urllib2.Request(url, data, headers)
        timeout = self.timeout if timeout == -1 else timeout
        try:
            resp = self.opener.open(req, timeout=timeout)
        except Exception as e:
            log_details = self._log_details(e)
            if isinstance(log_details, tuple):
                _, _, _, error = log_details
                if error and error.startswith("Unable to find batch"):
                    raise InvalidBatchException()
            raise e
        current_action = Action.get_current_action()
        if current_action and current_action.progress is None:
            current_action.progress = 0
        if file_out is not None:
            locker = self.unlock_path(file_out)
            try:
                with open(file_out, "wb") as f:
                    while True:
                        # Check if synchronization thread was suspended
                        if self.check_suspended is not None:
                            self.check_suspended('File download: %s'
                                                 % file_out)
                        buffer_ = resp.read(self.get_download_buffer())
                        if buffer_ == '':
                            break
                        if current_action:
                            current_action.progress += (
                                self.get_download_buffer())
                        f.write(buffer_)
                return None, file_out
            finally:
                self.lock_path(file_out, locker)
        else:
            return self._read_response(resp, url)

    def execute_with_blob_streaming(self, command, file_path, filename=None,
                                    mime_type=None, **params):
        """Execute an Automation operation using a batch upload as an input

        Upload is streamed.
        """
        tick = time.time()
        action = FileAction("Upload", file_path, filename)
        try:
            batch_id = None
            if self.is_new_upload_api_available():
                try:
                    # Init resumable upload getting a batch id generated by the server
                    # This batch id is to be used as a resumable session id
                    batch_id = self.init_upload()['batchId']
                except NewUploadAPINotAvailable:
                    log.debug('New upload API is not available on server %s', self.server_url)
                    self.new_upload_api_available = False
            if batch_id is None:
                # New upload API is not available, generate a batch id
                batch_id = self._generate_unique_id()
            upload_result = self.upload(batch_id, file_path, filename=filename,
                                        mime_type=mime_type)
            upload_duration = int(time.time() - tick)
            action.transfer_duration = upload_duration
            # Use upload duration * 2 as Nuxeo transaction timeout
            tx_timeout = max(DEFAULT_NUXEO_TX_TIMEOUT, upload_duration * 2)
            log.trace('Using %d seconds [max(%d, 2 * upload time=%d)] as Nuxeo'
                      ' transaction timeout for batch execution of %s'
                      ' with file %s', tx_timeout, DEFAULT_NUXEO_TX_TIMEOUT,
                      upload_duration, command, file_path)
            if upload_duration > 0:
                log.trace("Speed for %d octets in %d sec : %f o/s", os.stat(file_path).st_size, upload_duration,
                          os.stat(file_path).st_size / upload_duration)
            # upload result may be "empty"
            error_msg = "Bad response from batch upload with id '%s'"\
                        " and file path '%s'" % (batch_id, file_path)
            try:
                # NXDRIVE-433: Compat with 7.4 intermediate state
                if upload_result.get('uploaded') is None:
                    self.new_upload_api_available = False
                if upload_result.get('batchId') is not None:
                    result = self.execute_batch(command, batch_id, '0', tx_timeout,
                                                **params)
                    return result
                else:
                    raise ValueError(error_msg)
            except (AttributeError, TypeError) as e:
                log.debug("invalid response format: %s (%s)", str(upload_result), str(e))
                raise ValueError(error_msg)
            except KeyError as e:
                log.debug("invalid response content: %s (%s)", str(upload_result), str(e))
                raise ValueError(error_msg)
        except InvalidBatchException:
            self.cookie_jar.clear_session_cookies()
        finally:
            self.end_action()

    @staticmethod
    def get_upload_buffer():
        rate = BaseAutomationClient.upload_token_bucket.get_fill_rate()
        if not hasattr(BaseAutomationClient, 'upload_token_bucket') or \
                        BaseAutomationClient.upload_token_bucket is None:
            rate = NO_LIMIT
        if rate == NO_LIMIT and sys.platform != 'win32':
            # create a temp file to get the file system's buffer size
            f = tempfile.NamedTemporaryFile(suffix='.tmp')
            return os.fstatvfs(f.file.fileno()).f_bsize

        return get_file_buffer_size(rate)

    def init_upload(self):
        url = self.rest_api_url + self.batch_upload_path
        headers = self._get_common_headers()
        # Force empty data to perform a POST request
        req = urllib2.Request(url, data='', headers=headers)
        try:
            resp = self.opener.open(req, timeout=self.timeout)
        except Exception as e:
            log_details = self._log_details(e)
            if isinstance(log_details, tuple):
                status, code, message, _ = log_details
                if status == 404:
                    raise NewUploadAPINotAvailable()
                if status == 500:
                    not_found_exceptions = ['com.sun.jersey.api.NotFoundException',
                                            'org.nuxeo.ecm.webengine.model.TypeNotFoundException']
                    for exception in not_found_exceptions:
                        if code == exception or exception in message:
                            raise NewUploadAPINotAvailable()
            raise e
        return self._read_response(resp, url)

    def upload(self, batch_id, file_path, filename=None, file_index=0,
               mime_type=None):
        """Upload a file through an Automation batch

        Uses poster.httpstreaming to stream the upload
        and not load the whole file in memory.
        """
        FileAction("Upload", file_path, filename)
        # Request URL
        if self.is_new_upload_api_available():
            url = self.rest_api_url + self.batch_upload_path + '/' + batch_id + '/' + str(file_index)
        else:
            # Backward compatibility with old batch upload API
            url = self.automation_url.encode('ascii') + self.batch_upload_url

        # HTTP headers
        if filename is None:
            filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        if mime_type is None:
            mime_type = guess_mime_type(filename)
        # Quote UTF-8 filenames even though JAX-RS does not seem to be able
        # to retrieve them as per: https://tools.ietf.org/html/rfc5987
        filename = safe_filename(filename)
        quoted_filename = urllib2.quote(filename.encode('utf-8'))
        headers = {
            "X-File-Name": quoted_filename,
            "X-File-Size": file_size,
            "X-File-Type": mime_type,
            "Content-Type": "application/octet-stream",
            "Content-Length": file_size,
        }
        if not self.is_new_upload_api_available():
            headers.update({"X-Batch-Id": batch_id, "X-File-Idx": file_index})
        headers.update(self._get_common_headers())

        # Request data
        input_file = open(file_path, 'rb')
        # Use file system block size if available for streaming buffer
        fs_block_size = BaseAutomationClient.get_upload_buffer()
        data = self._read_data(input_file, fs_block_size)

        # Execute request
        cookies = self._get_cookies()
        log.trace("Calling %s with headers %r and cookies %r for file %s",
                  url, headers, cookies, file_path)
        req = urllib2.Request(url, data, headers)
        try:
            resp = self.streaming_opener.open(req, timeout=self.blob_timeout)
        except KeyError:
            log.trace('KeyError exception: %s', sys.exc_traceback.tb_lineno)
        except Exception as e:
            log_details = self._log_details(e)
            if isinstance(log_details, tuple):
                _, _, _, error = log_details
                if error and error.startswith("Unable to find batch"):
                    raise InvalidBatchException()
            raise e
        finally:
            input_file.close()
        # CSPII-9144: help diagnose upload problem
        log.trace('Upload completed. File closed.')
        self.end_action()
        return self._read_response(resp, url)

    def end_action(self):
        Action.finish_action()

    def execute_batch(self, op_id, batch_id, file_idx, tx_timeout, **params):
        """Execute a file upload Automation batch"""
        extra_headers = {'Nuxeo-Transaction-Timeout': tx_timeout,}
        if self.is_new_upload_api_available():
            url = (self.rest_api_url + self.batch_upload_path + '/' + batch_id + '/' + file_idx
                   + '/execute/' + op_id)
            return self.execute(None, url=url, timeout=tx_timeout,
                                check_params=False, extra_headers=extra_headers, **params)
        else:
            return self.execute(self.batch_execute_url, timeout=tx_timeout,
                                operationId=op_id, batchId=batch_id, fileIdx=file_idx,
                                check_params=False, extra_headers=extra_headers, **params)

    def is_addon_installed(self):
        return 'NuxeoDrive.GetRoots' in self.operations

    def is_event_log_id_available(self):
        return self.is_event_log_id

    def is_elasticsearch_audit(self):
        return 'NuxeoDrive.WaitForElasticsearchCompletion' in self.operations

    def is_nuxeo_drive_attach_blob(self):
        return 'NuxeoDrive.AttachBlob' in self.operations

    def is_new_upload_api_available(self):
        return self.new_upload_api_available

    def request_token(self, revoke=False):
        """Request and return a new token for the user"""
        base_error_message = (
                                 "Failed to connect to Nuxeo server %s with user %s"
                                 " to acquire a token"
                             ) % (self.server_url, self.user_id)

        parameters = {
            'deviceId': self.device_id,
            'applicationName': self.application_name,
            'permission': TOKEN_PERMISSION,
            'revoke': 'true' if revoke else 'false',
        }
        device_description = DEVICE_DESCRIPTIONS.get(sys.platform)
        if device_description:
            parameters['deviceDescription'] = device_description
        url = self.server_url + 'authentication/token?'
        url += urlencode(parameters)

        headers = self._get_common_headers()
        cookies = self._get_cookies()
        log.trace("Calling %s with headers %r and cookies %r",
                  url, headers, cookies)
        req = urllib2.Request(url, headers=headers)
        try:
            token = self.opener.open(req, timeout=self.timeout).read()
        except urllib2.HTTPError as e:
            if e.code == 401 or e.code == 403:
                raise Unauthorized(self.server_url, self.user_id, e.code)
            elif e.code == 404:
                # Token based auth is not supported by this server
                return None
            else:
                e.msg = base_error_message + ": HTTP error %d" % e.code
                raise e
        except Exception as e:
            if hasattr(e, 'msg'):
                e.msg = base_error_message + ": " + e.msg
            raise
        cookies = self._get_cookies()
        log.trace("Got token '%s' with cookies %r", token, cookies)
        # Use the (potentially re-newed) token from now on
        if not revoke:
            self._update_auth(token=token)
        return token

    def revoke_token(self):
        self.request_token(revoke=True)

    def wait(self):
        # Used for tests
        if self.is_elasticsearch_audit():
            self.execute("NuxeoDrive.WaitForElasticsearchCompletion")
        else:
            # Backward compatibility with JPA audit implementation,
            # in which case we are also backward compatible with date based resolution
            if not self.is_event_log_id_available():
                time.sleep(AUDIT_CHANGE_FINDER_TIME_RESOLUTION)
            self.execute("NuxeoDrive.WaitForAsyncCompletion")

    def make_tmp_file(self, content):
        """Create a temporary file with the given content for streaming upload purpose.

        Make sure that you remove the temporary file with os.remove() when done with it.
        """
        fd, path = tempfile.mkstemp(suffix=u'-nxdrive-file-to-upload',
                                    dir=self.upload_tmp_dir)
        with open(path, "wb") as f:
            f.write(content)
        os.close(fd)
        return path

    def _update_auth(self, password=None, token=None):
        """
        When username retrieved from database, check for unicode and convert to string.
        Note: base64Encoding for unicode type will fail, hence converting to string
        """
        if self.user_id and isinstance(self.user_id, unicode):
            self.user_id = unicode(self.user_id).encode('utf-8')

        # Select the most appropriate auth headers based on credentials
        if token is not None:
            self.auth = ('X-Authentication-Token', token)
        elif password is not None:
            basic_auth = 'Basic %s' % base64.b64encode(
                self.user_id + ":" + password).strip()
            self.auth = ("Authorization", basic_auth)
        else:
            raise ValueError("Either password or token must be provided")

    def _get_common_headers(self):
        """Headers to include in every HTTP requests

        Includes the authentication heads (token based or basic auth if no
        token).

        Also include an application name header to make it possible for the
        server to compute access statistics for various client types (e.g.
        browser vs devices).

        """
        return {
            'X-User-Id': self.user_id,
            'X-Device-Id': self.device_id,
            'X-Client-Version': self.client_version,
            'User-Agent': self.application_name + "/" + self.client_version,
            'X-Application-Name': self.application_name,
            self.auth[0]: self.auth[1],
            'Cache-Control': 'no-cache',
        }

    def _get_cookies(self):
        return list(self.cookie_jar) if self.cookie_jar is not None else []

    def _check_operation(self, command):
        if command not in self.operations:
            if command.startswith('NuxeoDrive.'):
                raise AddonNotInstalled(
                    "Either nuxeo-drive addon is not installed on server %s or"
                    " server version is lighter than the minimum version"
                    " compatible with the client version %s, in which case a"
                    " downgrade of Nuxeo Drive is needed." % (
                        self.server_url, self.client_version))
            else:
                raise ValueError("'%s' is not a registered operations."
                                 % command)
        return self.operations[command]

    def _check_params(self, command, params):
        method = self._check_operation(command)
        required_params = []
        other_params = []
        for param in method['params']:
            if param['required']:
                required_params.append(param['name'])
            else:
                other_params.append(param['name'])

        for param in params.keys():
            if (not param in required_params
                and not param in other_params):
                log.trace("Unexpected param '%s' for operation '%s'", param,
                          command)
        for param in required_params:
            if not param in params:
                raise ValueError(
                    "Missing required param '%s' for operation '%s'" % (
                        param, command))

                # TODO: add typechecking

    def _read_response(self, response, url):
        info = response.info()
        s = response.read()
        content_type = info.get('content-type', '')
        cookies = self._get_cookies()
        if content_type.startswith("application/json"):
            log.trace("Response %d %s for '%s' with cookies %r: %r",
                      response.code, response.msg, url, cookies, s)
            return json.loads(s) if s else None
        else:
            log.trace("Response %d %s for '%s' with cookies %r has content-type %r",
                      response.code, response.msg, url, cookies, content_type)
            return s

    def _log_details(self, e):
        if hasattr(e, "fp"):
            detail = e.fp.read()
            try:
                exc = json.loads(detail)
                message = exc.get('message')
                stack = exc.get('stack')
                error = exc.get('error')
                if message:
                    log.debug('Remote exception message: %s', message)
                if stack:
                    log.debug('Remote exception stack: %r', exc['stack'], exc_info=True)
                else:
                    log.debug('Remote exception details: %r', detail)
                return exc.get('status'), exc.get('code'), message, error
            except:
                # Error message should always be a JSON message,
                # but sometimes it's not
                if '<html>' in detail:
                    message = e
                else:
                    message = detail
                log.error(message)
                if isinstance(e, urllib2.HTTPError):
                    return e.code, None, message, None
        # CSPII-9144: help diagnose upload problem
        log.trace('Client exception: %s', e.message)
        return None

    def _generate_unique_id(self):
        """Generate a unique id based on a timestamp and a random integer"""

        return str(time.time()) + '_' + str(random.randint(0, 1000000000))

    def _read_data(self, file_object, buffer_size):
        total_size = os.fstat(file_object.fileno()).st_size
        filename = file_object.name.encode('utf-8')
        filename = os.path.basename(filename)
        self.update_upload_transfer_rate(-1, total_size=total_size, filename=filename)

        while True:
            current_action = Action.get_current_action()
            if current_action is not None and current_action.suspend:
                break
            # Check if synchronization thread was suspended
            if self.check_suspended is not None:
                self.check_suspended('File upload: %s' % file_object.name)
            r = file_object.read(buffer_size)
            if not r:
                self.update_upload_transfer_rate(0)
                break

            if BaseAutomationClient.use_upload_rate_limit():
                size = int(math.ceil(len(r) / 1000.0))
                wait_time = BaseAutomationClient.upload_token_bucket.consume(size)
                while wait_time > 0:
                    log.trace('waiting to upload: %s sec [rate=%d]', wait_time,
                              BaseAutomationClient.upload_token_bucket.get_fill_rate())
                    time.sleep(wait_time)
                    wait_time = BaseAutomationClient.upload_token_bucket.consume(size)

            if current_action is not None:
                current_action.progress += buffer_size
                if BaseAutomationClient.use_upload_rate_limit():
                    self.update_upload_transfer_rate(len(r))
            yield r

    def do_get(self, url, file_out=None, digest=None, digest_algorithm=None):
        log.trace('Downloading file from %r to %r with digest=%s, digest_algorithm=%s', url, file_out, digest,
                  digest_algorithm)
        h = None
        if digest is not None:
            if digest_algorithm is None:
                digest_algorithm = guess_digest_algorithm(digest)
                log.trace('Guessed digest algorithm from digest: %s', digest_algorithm)
            digester = getattr(hashlib, digest_algorithm, None)
            if digester is None:
                raise ValueError('Unknown digest method: ' + digest_algorithm)
            h = digester()
        headers = self._get_common_headers()
        base_error_message = (
                                 "Failed to connect to Nuxeo server %r with user %r"
                             ) % (self.server_url, self.user_id)
        try:
            log.trace("Calling '%s' with headers: %r", url, headers)
            req = urllib2.Request(url, headers=headers)
            response = self.opener.open(req, timeout=self.blob_timeout)
            current_action = Action.get_current_action()
            # Get the size file
            if response is not None and response.info() is not None:
                total_size = int(response.info().getheader('Content-Length', 0))
            else:
                total_size = 0

            if current_action:
                current_action.size = total_size

            if file_out is not None:
                # filename = os.path.basename(file_out)
                filename = os.path.basename(file_out.encode('utf-8'))
                locker = self.unlock_path(file_out)
                try:
                    with open(file_out, "wb") as f:
                        BaseAutomationClient.download_stats.start(total_size=total_size, filename=filename)
                        while True:
                            # Check if synchronization thread was suspended
                            if self.check_suspended is not None:
                                self.check_suspended('File download: %s'
                                                     % file_out)

                            buffer_ = response.read(self.get_download_buffer())
                            if buffer_ == '':
                                break

                            if BaseAutomationClient.use_download_rate_limit():
                                size = int(math.ceil(len(buffer_) / 1000.0))
                                wait_time = BaseAutomationClient.download_token_bucket.consume(size)
                                while wait_time > 0:
                                    log.trace('waiting to download: %s sec [rate=%d]', wait_time,
                                              BaseAutomationClient.download_token_bucket.get_fill_rate())
                                    time.sleep(wait_time)
                                    wait_time = BaseAutomationClient.download_token_bucket.consume(size)

                            if current_action:
                                current_action.progress += len(buffer_)
                            f.write(buffer_)
                            if h is not None:
                                h.update(buffer_)
                                if BaseAutomationClient.use_download_rate_limit():
                                    self.update_download_transfer_rate(len(buffer_))
                    if digest is not None:
                        actual_digest = h.hexdigest()
                        if digest != actual_digest:
                            if os.path.exists(file_out):
                                os.remove(file_out)
                            raise CorruptedFile("Corrupted file %r: expected digest = %s, actual digest = %s"
                                                % (file_out, digest, actual_digest))
                    return None, file_out
                except Exception as e:
                    e.msg = 'error downloading file: ' + e.message
                    error = e
                    raise e
                finally:
                    self.lock_path(file_out, locker)
                    if BaseAutomationClient.use_download_rate_limit():
                        self.update_download_transfer_rate(0)
            else:
                result = response.read()
                if h is not None:
                    h.update(result)
                    if digest is not None:
                        actual_digest = h.hexdigest()
                        if digest != actual_digest:
                            raise CorruptedFile("Corrupted file: expected digest = %s, actual digest = %s"
                                                % (digest, actual_digest))
                return result, None
        except urllib2.HTTPError as e:
            if e.code == 401 or e.code == 403:
                raise Unauthorized(self.server_url, self.user_id, e.code)
            else:
                e.msg = base_error_message + ": HTTP error %d" % e.code
                raise e
        except KeyError:
            log.trace('KeyError exception: %s', sys.exc_traceback.tb_lineno)
        except Exception as e:
            if hasattr(e, 'msg'):
                e.msg = base_error_message + ": " + e.msg
            raise

    @staticmethod
    def get_download_buffer():
        rate = BaseAutomationClient.download_token_bucket.get_fill_rate()
        if not hasattr(BaseAutomationClient, 'download_token_bucket') or \
                        BaseAutomationClient.download_token_bucket is None:
            rate = NO_LIMIT
        return get_file_buffer_size(rate)

    def update_download_transfer_rate(self, size, total_size=None, error=None, filename=None):
        filename = filename or BaseAutomationClient.download_stats.get_filename()
        identifier = "[thread %s,  object %s, filename %s]" % (threading.current_thread().ident, id(self), filename)
        total_size = total_size or BaseAutomationClient.download_stats.get_total_size()
        # assert total_size is not None and total_size > 0, "invalid total size"
        if total_size is None or total_size <= 0:
            log.debug("invalid total size for download (%d)", total_size)

        if size == -1:
            log.trace('%s download start (%s)', identifier, filename if filename else 'none')
            if not BaseAutomationClient.download_stats.is_new_transfer():
                log.debug("%s is not a new transfer (%s)", identifier, filename if filename else 'none')
            BaseAutomationClient.download_stats.start(total_size=total_size, filename=filename)
            return

        # end of file download
        if size == 0 or error:
            log.trace('%s download complete%s (%s)',
                      identifier, ' with error \'%s\' ' % error.message if error else '',
                      filename if filename else 'none')
            BaseAutomationClient.download_stats.end()
            log.trace("%s download stats at completion: %s", identifier,
                      str(BaseAutomationClient.download_stats.get_stats()))
            # update average rate
            avg_rate = BaseAutomationClient.download_stats.get_average_rate()
            downloaded_size = BaseAutomationClient.download_stats.get_current_size()
            if total_size > 0:
                log.trace("%s download stats: %4.1f%%, avg rate=%5.1f KB/s, %.1f/%.1f KB",
                          identifier, BaseAutomationClient.download_stats.get_percent_transfer(),
                          avg_rate, downloaded_size / 1000, total_size / 1000
                          )
            else:
                log.trace("%s upload stats: avg rate=%5.1f KB/s, %.1f/%.1f KB",
                          identifier, avg_rate, downloaded_size / 1000
                          )

            BaseAutomationClient.upload_stats.reset()
            return

        BaseAutomationClient.download_stats.update(size)
        downloaded_size = BaseAutomationClient.download_stats.get_current_size()
        log.trace("%s download instant rate: %5.1f KB/s, download size: %d, downloaded %d of %d", identifier,
                  BaseAutomationClient.download_stats.get_instant_rate(), size, downloaded_size, total_size)

    def update_upload_transfer_rate(self, size, total_size=None, error=None, filename=None):
        filename = filename or BaseAutomationClient.upload_stats.get_filename()
        identifier = "[thread %s,  object %s, filename %s]" % (threading.current_thread().ident, id(self), filename)
        total_size = total_size or BaseAutomationClient.upload_stats.get_total_size()
        # assert total_size is not None and total_size > 0, "invalid total size"
        if total_size is None or total_size <= 0:
            log.debug("invalid total size for upload (%d)", total_size)

        if size == -1:
            log.trace('%s upload start (%s)', identifier, filename if filename else 'none')
            if not BaseAutomationClient.upload_stats.is_new_transfer():
                log.debug("%s is not a new transfer (%s)", identifier, filename if filename else 'none')
            BaseAutomationClient.upload_stats.start(total_size=total_size, filename=filename)
            return

        # end of file upload
        if size == 0 or error:
            log.trace('%s upload complete%s (%s)',
                      identifier, ' with error \'%s\' ' % error.message if error else '',
                      filename if filename else 'none')
            BaseAutomationClient.upload_stats.end()
            log.trace("%s upload stats at completion: %s", identifier,
                      str(BaseAutomationClient.upload_stats.get_stats()))
            # update average rate
            avg_rate = BaseAutomationClient.upload_stats.get_average_rate()
            uploaded_size = BaseAutomationClient.upload_stats.get_current_size()
            if total_size > 0:
                log.trace("%s upload stats: %4.1f%%, avg rate=%5.1f KB/s, %.1f/%.1f KB",
                          identifier, BaseAutomationClient.upload_stats.get_percent_transfer(),
                          avg_rate, uploaded_size / 1000, total_size / 1000
                          )
            else:
                log.trace("%s upload stats: avg rate=%5.1f KB/s, %.1f/%.1f KB",
                          identifier, avg_rate, uploaded_size / 1000
                          )

            return

        BaseAutomationClient.upload_stats.update(size)
        uploaded_size = BaseAutomationClient.upload_stats.get_current_size()
        log.trace("%s upload instant rate: %5.1f KB/s, upload size: %d, uploaded %d of %d", identifier,
                  BaseAutomationClient.upload_stats.get_instant_rate(), size, uploaded_size, total_size)
