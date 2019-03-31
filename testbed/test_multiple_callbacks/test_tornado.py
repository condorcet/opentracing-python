from __future__ import print_function

import random

from tornado import gen, ioloop

from opentracing.mocktracer import MockTracer
from opentracing.scope_managers.tornado import TornadoScopeManager, \
        tracer_stack_context
from ..testcase import OpenTracingTestCase
from ..utils import get_logger, stop_loop_when, get_one_by_operation_name


random.seed()
logger = get_logger(__name__)


class TestTornado(OpenTracingTestCase):
    def setUp(self):
        self.tracer = MockTracer(TornadoScopeManager())
        self.loop = ioloop.IOLoop.current()

    def test_main(self):
        @gen.coroutine
        def main_task():
            with self.tracer.start_active_span('parent'):
                tasks = self.submit_callbacks()
                yield tasks

        with tracer_stack_context():
            self.loop.add_callback(main_task)

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) == 4)
        self.loop.start()

        spans = self.tracer.finished_spans()
        self.assertEquals(len(spans), 4)
        self.assertNamesEqual(spans, ['task', 'task', 'task', 'parent'])

        for i in range(3):
            self.assertSameTrace(spans[i], spans[-1])
            self.assertIsChildOf(spans[i], spans[-1])

    def test_concurrent_same_context(self):
        """
        This test modelling two concurrent requests in same context.
        Each request sleeping before "work" and after that making child span.
        Time start of requests are shifted: the second request starts later and
        changes context while first request is sleeping. When first request
        wake up to make child span, the context will be different and it child
        span will take parent from second request.
        """

        self.loop.add_callback(
            self.make_request('req1', sleep_before=0.5, sleep_after=0.5))
        self.loop.add_callback(
            self.make_request('req2', delay=0.25, sleep_before=0.5))
        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) >= 4)
        self.loop.start()
        spans = self.tracer.finished_spans()
        self.assertEqual(len(spans), 4)
        req1 = get_one_by_operation_name(spans, 'req1')
        req2 = get_one_by_operation_name(spans, 'req2')
        req1_work = get_one_by_operation_name(spans, 'req1:work')
        req2_work = get_one_by_operation_name(spans, 'req2:work')
        self.assertNotSameTrace(req1, req2)
        self.assertIsChildOf(req2_work, req2)
        # The child should have parent from first request but context was
        # messed up.
        self.assertIsChildOf(req1_work, req2)

    def test_concurrent_different_contexts(self):
        """
        This test modelling two concurrent requests wrapped by
        `tracer_stack_context` that provides their own context. This test
        similar to previous: when first request wake up and make child span,
        active root span already was made by second request. Because first
        request started in stack context the first request make child span with
        right parent.
        """
        with tracer_stack_context():
            self.loop.add_callback(
                self.make_request('req1', sleep_before=0.5, sleep_after=0.5))
        with tracer_stack_context():
            self.loop.add_callback(
                self.make_request('req2', delay=0.25, sleep_before=0.5))
        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) >= 4)
        self.loop.start()
        spans = self.tracer.finished_spans()
        self.assertEqual(len(spans), 4)

        req1 = get_one_by_operation_name(spans, 'req1')
        req2 = get_one_by_operation_name(spans, 'req2')
        req1_work = get_one_by_operation_name(spans, 'req1:work')
        req2_work = get_one_by_operation_name(spans, 'req2:work')
        self.assertNotSameTrace(req1, req2)
        # Each child span have correct parent.
        self.assertIsChildOf(req1_work, req1)
        self.assertIsChildOf(req2_work, req2)

    @gen.coroutine
    def task(self, interval, parent_span):
        logger.info('Starting task')

        # NOTE: No need to reactivate the parent_span, as TracerStackContext
        # keeps track of it, BUT a limitation is that, yielding
        # upon multiple coroutines, we cannot mess with the context,
        # so no active span set here.
        assert self.tracer.active_span is not None
        self.assertEqual(self.tracer.active_span, parent_span)
        with self.tracer.start_span('task'):
            yield gen.sleep(interval)

    def submit_callbacks(self):
        parent_span = self.tracer.scope_manager.active.span
        tasks = []
        for i in range(3):
            interval = 0.1 + random.randint(200, 500) * 0.001
            t = self.task(interval, parent_span)
            tasks.append(t)

        return tasks

    def make_request(self, operation, delay=0, sleep_before=0, sleep_after=0):
        """
        Make coroutine that modelling incoming request. This coroutine invokes
        another coroutine (`_work`) that will start new child span.
        """
        @gen.coroutine
        def _work():
            yield gen.sleep(sleep_before)
            # After sleeping context may be lost, so new span may have "wrong"
            # parent.
            with self.tracer.start_active_span(
                operation_name='{}:{}'.format(operation, 'work')
            ):
                pass
            yield gen.sleep(sleep_after)

        @gen.coroutine
        def _request():
            yield gen.sleep(delay)
            with self.tracer.start_active_span(
                operation_name=operation, ignore_active_span=True
            ):
                yield _work()

        return _request
