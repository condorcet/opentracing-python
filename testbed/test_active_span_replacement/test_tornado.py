# coding: utf-8
from __future__ import print_function

from tornado import gen, ioloop

from opentracing.mocktracer import MockTracer
from opentracing.scope_managers.tornado import TornadoScopeManager, \
        tracer_stack_context
from ..testcase import OpenTracingTestCase
from ..utils import stop_loop_when, get_one_by_operation_name


class TestTornado(OpenTracingTestCase):
    def setUp(self):
        self.tracer = MockTracer(TornadoScopeManager())
        self.loop = ioloop.IOLoop.current()

    def test_main(self):
        # Start an isolated task and query for its result -and finish it-
        # in another task/thread
        span = self.tracer.start_span('initial')
        with tracer_stack_context():
            self.submit_another_task(span)

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) >= 3)
        self.loop.start()

        spans = self.tracer.finished_spans()
        self.assertEqual(len(spans), 3)
        self.assertNamesEqual(spans, ['initial', 'subtask', 'task'])

        # task/subtask are part of the same trace,
        # and subtask is a child of task
        self.assertSameTrace(spans[1], spans[2])
        self.assertIsChildOf(spans[1], spans[2])

        # initial task is not related in any way to those two tasks
        self.assertNotSameTrace(spans[0], spans[1])
        self.assertEqual(spans[0].parent_id, None)

    @gen.coroutine
    def task(self, span):
        # Create a new Span for this task
        with self.tracer.start_active_span('task'):

            with self.tracer.scope_manager.activate(span, True):
                # Simulate work strictly related to the initial Span
                pass

            # Use the task span as parent of a new subtask
            with self.tracer.start_active_span('subtask'):
                pass

    def submit_another_task(self, span):
        self.loop.add_callback(self.task, span)

    def test_fire_and_forget_fail(self):
        """
        Этот тест показывает, что вызов корутины (gen.engine/gen.coroutine)
        по принципу fire & forget внутри другой корутины, не вернет прежний
        контекст родительскому спану и он будет потерян.
        """

        @gen.engine
        def fire_and_forget_coro(dct):
            with self.tracer.start_active_span('fire_and_forget') as scope:
                dct['fire_and_forget_span'] = scope.span
                yield gen.sleep(1)

        @gen.engine
        def main_coro():
            with self.tracer.start_active_span('main_coro') as scope:
                main_span = scope.span
                dct = {}  # словарь, в который будет дочерний спан корутины
                fire_and_forget_coro(dct)
                # Здесь мы имеем уже спан от корутины fire_and_forget и он
                # является активным.
                assert main_span != self.tracer.active_span
                assert dct['fire_and_forget_span']
                assert dct['fire_and_forget_span'] == self.tracer.active_span

        with tracer_stack_context():
            main_coro()

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) >= 1)
        self.loop.start()

        spans = self.tracer.finished_spans()
        # Имеем только один спан -- дочерней корутины, которая перхватила
        # контекст у родительской.
        self.assertEqual(len(spans), 1)
        fire_and_forget = get_one_by_operation_name(spans, 'fire_and_forget')
        assert fire_and_forget
        assert fire_and_forget.parent_id is not None

    def test_fire_and_forget_ok(self):
        """
        В этом тесте вызов корутины внутри другой корутины (fire & forget)
        оборачивается в tracer_stack_context отдельно, после чего родительский
        спан активируется еще раз чтобы сохранить преемственность спанов.
        """

        @gen.engine
        def fire_and_forget_coro():
            with self.tracer.start_active_span('fire_and_forget'):
                yield gen.sleep(1)

        @gen.engine
        def main_coro():
            with self.tracer.start_active_span('main_coro') as scope:
                main_span = scope.span
                with tracer_stack_context():
                    with self.tracer.scope_manager.activate(scope.span, False):
                        fire_and_forget_coro()
                assert main_span == self.tracer.active_span

        with tracer_stack_context():
            main_coro()

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) >= 2)
        self.loop.start()

        spans = self.tracer.finished_spans()
        self.assertEqual(len(spans), 2)
        main = get_one_by_operation_name(spans, 'main_coro')
        fire_and_forget = get_one_by_operation_name(spans, 'fire_and_forget')
        self.assertSameTrace(main, fire_and_forget)
        self.assertIsChildOf(fire_and_forget, main)
