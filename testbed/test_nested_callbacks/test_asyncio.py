from __future__ import print_function


import asyncio

from opentracing.mocktracer import MockTracer
from opentracing.scope_managers.asyncio import AsyncioScopeManager,\
    null_scope
from ..testcase import OpenTracingTestCase
from ..utils import stop_loop_when


class TestAsyncio(OpenTracingTestCase):
    def setUp(self):
        self.tracer = MockTracer(AsyncioScopeManager())
        self.loop = asyncio.get_event_loop()

    def test_main(self):
        # Start a Span and let the callback-chain
        # finish it when the task is done
        async def task():
            with self.tracer.start_active_span('one', finish_on_close=False):
                self.submit()

        self.loop.create_task(task())

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) == 1)
        self.loop.run_forever()

        spans = self.tracer.finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].operation_name, 'one')

        for i in range(1, 4):
            self.assertEqual(spans[0].tags.get('key%s' % i, None), str(i))

    def submit(self):
        span = self.tracer.scope_manager.active.span

        async def task1():
            with self.tracer.scope_manager.activate(span, False):
                span.set_tag('key1', '1')

                async def task2():
                    with self.tracer.scope_manager.activate(span, False):
                        span.set_tag('key2', '2')

                        async def task3():
                            with self.tracer.scope_manager.activate(span,
                                                                    False):
                                span.set_tag('key3', '3')
                                span.finish()

                        self.loop.create_task(task3())

                self.loop.create_task(task2())

        self.loop.create_task(task1())


class TestAutoContextPropagationAsyncio(TestAsyncio):

    def submit(self):
        span = self.tracer.scope_manager.active.span

        async def task1():
            span.set_tag('key1', '1')

            async def task2():
                span.set_tag('key2', '2')

                async def task3():
                    span.set_tag('key3', '3')
                    span.finish()

                self.loop.create_task(task3())

            self.loop.create_task(task2())

        self.loop.create_task(task1())


class TestAsyncioScheduleInLoop(OpenTracingTestCase):

    def setUp(self):
        self.tracer = MockTracer(AsyncioScopeManager())
        self.loop = asyncio.get_event_loop()

    def test_schedule_callbacks(self):

        def callback(op_name):
            with self.tracer.start_active_span(
                operation_name=op_name,
                child_of=self.tracer.active_span,
            ):
                pass

        def callback_with_nested_callback(op_name):
            with self.tracer.start_active_span(
                operation_name=op_name,
                child_of=self.tracer.active_span,
            ):
                self.loop.call_soon(callback, 'childof:{}'.format(op_name))

        with self.tracer.start_active_span('root'):
            self.loop.call_soon(callback_with_nested_callback, 'first')
            self.loop.call_soon(callback, 'second')

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) == 4)
        self.loop.run_forever()

        root, first, second, childof_first = self.tracer.finished_spans()
        self.assertEmptySpan(root, 'root')
        self.assertEmptySpan(first, 'first')
        self.assertEmptySpan(second, 'second')
        self.assertEmptySpan(childof_first, 'childof:first')

        self.assertIsChildOf(first, root)
        self.assertIsChildOf(childof_first, first)
        self.assertIsChildOf(second, root)

    def test_coroutines_schedule_callbacks(self):

        def callback(op_name):
            with self.tracer.start_active_span(
                operation_name=op_name,
                child_of=self.tracer.active_span
            ):
                pass

        async def task(op_name):
            with self.tracer.start_active_span(
                operation_name=op_name,
                child_of=self.tracer.active_span
            ):
                self.loop.call_later(
                    0.1, callback, 'childof:{}'.format(op_name)
                )
        with self.tracer.start_active_span('root'):
            self.loop.create_task(task('task1'))
            self.loop.create_task(task('task2'))

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) == 5)
        self.loop.run_forever()

        root, task1, task2, child1, child2 = self.tracer.finished_spans()

        self.assertEmptySpan(root, 'root')
        self.assertEmptySpan(task1, 'task1')
        self.assertEmptySpan(task2, 'task2')
        self.assertEmptySpan(child1, 'childof:task1')
        self.assertEmptySpan(child2, 'childof:task2')

        self.assertIsChildOf(task1, root)
        self.assertIsChildOf(task2, root)
        self.assertIsChildOf(child1, task1)
        self.assertIsChildOf(child2, task2)

    def test_coroutines_schedule_tasks(self):

        async def _task(op_name):
            await asyncio.sleep(0.1)
            with self.tracer.start_active_span(
                operation_name=op_name,
                child_of=self.tracer.active_span
            ):
                pass

        async def task(op_name):
            with self.tracer.start_active_span(
                operation_name=op_name,
                child_of=self.tracer.active_span
            ):
                self.loop.create_task(_task('childof:{}'.format(op_name)))

        with self.tracer.start_active_span('root'):
            self.loop.create_task(task('task1'))
            self.loop.create_task(task('task2'))

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) == 5)
        self.loop.run_forever()

        root, task1, task2, child1, child2 = self.tracer.finished_spans()

        self.assertEmptySpan(root, 'root')
        self.assertEmptySpan(task1, 'task1')
        self.assertEmptySpan(task2, 'task2')
        self.assertEmptySpan(child1, 'childof:task1')
        self.assertEmptySpan(child2, 'childof:task2')

        self.assertIsChildOf(task1, root)
        self.assertIsChildOf(task2, root)
        self.assertIsChildOf(child1, task1)
        self.assertIsChildOf(child2, task2)

    def test_recursive_scheduling_task(self):

        tasks = 3

        async def task():
            await asyncio.sleep(5)
            with self.tracer.start_active_span('periodic_task'):
                # do somethng
                pass

        async def task(n=0):
            await asyncio.sleep(0.1)
            with self.tracer.start_active_span(
                operation_name=str(n),
                child_of=self.tracer.active_span
            ):
                if n < tasks:
                    self.loop.create_task(task(n+1))


        self.loop.create_task(task())

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) == tasks)
        self.loop.run_forever()

        spans = self.tracer.finished_spans()

        for i in range(tasks):
            self.assertEmptySpan(spans[i], str(i))
            if i == 0:
                self.assertIsNone(spans[i].parent_id)
            else:
                self.assertIsChildOf(spans[i], spans[i-1])

    def test_recursive_scheduling_break_scope(self):

        tasks = 4

        async def task(n=0):
            await asyncio.sleep(0.1)
            with self.tracer.start_active_span(
                operation_name=str(n),
                # child_of=self.tracer.active_span
            ):
                if n + 1  < tasks / 2:
                    self.loop.create_task(task(n+1))
                elif n < tasks:
                    with null_scope():
                        self.loop.create_task(task(n+1))


        self.loop.create_task(task())

        stop_loop_when(self.loop,
                       lambda: len(self.tracer.finished_spans()) == tasks)
        self.loop.run_forever()

        s0, s1, s2, s3 = self.tracer.finished_spans()

        self.assertEmptySpan(s0, '0')
        self.assertHasNoParent(s0)

        self.assertEmptySpan(s1, '1')
        self.assertIsChildOf(s1, s0)

        self.assertEmptySpan(s2, '2')
        self.assertHasNoParent(s2)

        self.assertEmptySpan(s3, '3')
        self.assertHasNoParent(s3)
