import unittest
from nose.plugins.attrib import attr
from threading import Thread, current_thread
from time import sleep

from nxdrive.client.base_automation_client import TokenStrategy, NoStrategy, RoundRobinStrategy, WaitPriorityStrategy
from nxdrive.logging_config import get_logger
# use an odd number
NUM_THREADS = 5


class ThreadWithReturnValue(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, exit_target=None, exit_args=(), exit_kwargs={}, verbose=None):
        Thread.__init__(self, group=group, target=target, name=name, args=args, kwargs=kwargs, verbose=verbose)
        self._return = None
        self.exit_target = exit_target
        self.exit_args = exit_args
        self.exit_kwargs = exit_kwargs

    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)
            if self.exit_target is not None:
                self.exit_kwargs['thread_id'] = self.ident
                self.exit_target(*self.exit_args, **self.exit_kwargs)

    def join(self):
        Thread.join(self)
        return self._return


@attr(priority=2)
class TestTokenBucket(unittest.TestCase):
    def setUp(self):
        super(TestTokenBucket, self).setUp()
        self.log = get_logger(self.__class__.__name__)

    def test_nostrategy(self):
        strategy = NoStrategy(self.__class__.__name__, log=self.log)

        def test(testcase, strategy, index):
            this = current_thread()
            strategy.begin(this)
            sleep(1.0 * index / NUM_THREADS)
            testcase.assertTrue(strategy.is_next(this))
            strategy.update(this, ready=True)
            result = (index, strategy.get_last_access_time(this), strategy.get_wait_time(this), strategy.get_count(this))
            strategy.end(this.ident)
            return result

        threads = []
        for i in range(0, NUM_THREADS):
            t = ThreadWithReturnValue(
                name='thread' + str(i),
                target=test,
                args=(self, strategy, i)
            )
            t.start()
            threads.append(t)

        results = []
        for t in threads:
            results.append(t.join())

        # sort results by index
        results.sort(key=lambda x: x[0])
        # assert that last access time is also sorted in ascending order
        self.assertTrue(all(results[i][1] <= results[i + 1][1] for i in xrange(len(results) - 1)))
        # assert that wait time is also sorted in ascending order
        self.assertTrue(all(results[i][2] <= results[i + 1][2] for i in xrange(len(results) - 1)))
        # assert that access count is 0
        self.assertTrue(all(result[3] == 0 for result in results))

    def test_nostrategy_multiple_updates(self):
        strategy = NoStrategy(self.__class__.__name__, log=self.log)

        def test(testcase, strategy, index):
            this = current_thread()
            strategy.begin(this)
            sleep(index / NUM_THREADS)
            # no strategy implies a thread is always the "next" one to execute
            testcase.assertTrue(strategy.is_next(this))
            strategy.update(this)
            sleep(index / NUM_THREADS)
            strategy.update(this)
            result = (index, strategy.get_last_access_time(this), strategy.get_wait_time(this), strategy.get_count(this))
            strategy.end(this.ident)
            return result

        threads = []
        for i in range(0, NUM_THREADS):
            t = ThreadWithReturnValue(
                name='thread' + str(i),
                target=test,
                args=(self, strategy, i)
            )
            t.start()
            threads.append(t)

        results = []
        for t in threads:
            results.append(t.join())

        # sort results by index
        results.sort(key=lambda x: x[0])
        # assert that last access time is also sorted in ascending order
        self.assertTrue(all(results[i][1] <= results[i + 1][1] for i in xrange(len(results) - 1)))
        # assert that wait time is also sorted in ascending order
        self.assertTrue(all(results[i][2] <= results[i + 1][2] for i in xrange(len(results) - 1)))
        # assert that access count is 2
        self.assertTrue(all(result[3] == 2 for result in results))

    def test_roundrobin_strategy(self):
        strategy = RoundRobinStrategy(self.__class__.__name__, log=self.log)

        def test(testcase, strategy, index):
            this = current_thread()
            mid_index = (NUM_THREADS - 1) / 2
            strategy.begin(this)
            # delay the threads in reverse order of their index
            sleep(0.1 * (NUM_THREADS - index - 1))
            # 3rd thread ('thread2') goes thru immediately, the others wait
            allow = strategy.is_next(this)
            if index == mid_index:
                testcase.assertTrue(allow)
            else:
                testcase.assertFalse(allow)
            strategy.update(this, ready=allow)
            result = (index, strategy.get_last_access_time(this), strategy.get_wait_time(this), strategy.get_count(this))
            strategy.end(this.ident)
            return result

        threads = []
        for i in range(0, NUM_THREADS):
            t = ThreadWithReturnValue(
                name='thread' + str(i),
                target=test,
                args=(self, strategy, i)
            )
            # start the threads in order of the loop index, at 0, 10, 20, 30, 40ms respectively
            delay = i / 100.0
            if delay > 0:
                sleep(delay)
            t.start()
            threads.append(t)

        results = []
        for t in threads:
            results.append(t.join())

        # sort results by index in reverse
        results.sort(key=lambda x: x[0], reverse=True)
        mid_index = (NUM_THREADS - 1) / 2
        # assert that middle's thread access count has been reset to 0
        self.assertEqual(results[mid_index][3], 0,
                         'thread%d (%d) counter should be 0' % (mid_index, threads[mid_index].ident))
        # assert that last access time is sorted in ascending order
        # 1st thread has waited the longest
        all(results[i][1] <= results[i + 1][1] for i in xrange(len(results) - 1))
        # assert that wait time is also sorted in ascending order
        all(results[i][2] <= results[i + 1][2] for i in xrange(len(results) - 1))
        # assert that access count is 1 for all but the middle thread
        all(result[3] == 1 for result in results if results[0] != mid_index)

    def test_waitpriority_strategy(self):
        strategy = WaitPriorityStrategy(self.__class__.__name__, log=self.log)

        def test(testcase, strategy, index):
            this = current_thread()
            strategy.begin(this)
            # make thread2 wait 1s
            if index == 2:
                sleep(1)
            # make thread3 wait 0.5s
            if index == 3:
                sleep(0.8)
            # wait your turn
            while not strategy.is_next(this):
                sleep(0.1)

            strategy.update(this, ready=True)
            result = (index, strategy.get_last_access_time(this), strategy.get_count(this))
            strategy.end(this.ident)
            return result

        threads = []
        for i in range(0, NUM_THREADS):
            t = ThreadWithReturnValue(
                name='thread' + str(i),
                target=test,
                args=(self, strategy, i)
            )
            # start the threads in order of the loop index, at 0, 10, 20, 30, 40ms respectively
            delay = i / 100.0
            if delay > 0:
                sleep(delay)
            t.start()
            threads.append(t)

        results = []
        for t in threads:
            results.append(t.join())

        # sort results by last access time
        results.sort(key=lambda x: x[1], reverse=True)
        # assert that the order the threads returned is 2, 3, 4, 1 and 0
        self.assertEqual(results[0][0], 2,
                         'thread%d (%d) should be the first' % (results[0][0], threads[results[0][0]].ident))
        self.assertEqual(results[1][0], 3,
                         'thread%d (%d) should be the second' % (results[1][0], threads[results[1][0]].ident))
        self.assertEqual(results[2][0], 4,
                         'thread%d (%d) should be the third' % (results[2][0], threads[results[2][0]].ident))
        self.assertEqual(results[3][0], 1,
                         'thread%d (%d) should be the fourth' % (results[3][0], threads[results[3][0]].ident))
        self.assertEqual(results[4][0], 0,
                         'thread%d (%d) should be the fifth' % (results[4][0], threads[results[4][0]].ident))