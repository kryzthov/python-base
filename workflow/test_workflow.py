#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Unit-tests for Workflow and Task."""

import http.client
import logging
import os
import re
import sys
import time
import unittest

from base import base
from workflow import workflow


FLAGS = base.FLAGS

FLAGS.add_float(
    name='slow_task_duration',
    default=1.0,
    help='Duration of slow tasks, in seconds.',
)


class SlowTask(workflow.Task):
    def run(self):
        time.sleep(FLAGS.slow_task_duration)
        return self.SUCCESS


class SuccessTask(workflow.Task):
    def run(self):
        return self.SUCCESS


class FailureTask(workflow.Task):
    def run(self):
        return self.FAILURE


class TestWorkflow(unittest.TestCase):
    def test_empty_workflow(self):
        flow = workflow.Workflow()
        flow.build()
        flow.process()

    def testLinearWorkflowSuccess(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2', runs_after=[task1])
        task3 = SuccessTask(workflow=flow, task_id='task3', runs_after=[task2])
        task4 = SuccessTask(workflow=flow, task_id='task4', runs_after=[task3])
        task5 = SuccessTask(workflow=flow, task_id='task5', runs_after=[task4])
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task2.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task3.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task4.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task5.state)

        print(flow.dump_as_dot())

    def testParallelWorkflowSuccess(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task4 = SuccessTask(workflow=flow, task_id='task4')
        task5 = SuccessTask(workflow=flow, task_id='task5')
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task2.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task3.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task4.state)
        self.assertEqual(workflow.TaskState.SUCCESS, task5.state)

    def testLinearWorkflowFailure(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = FailureTask(workflow=flow, task_id='task2', runs_after=[task1])
        task3 = SuccessTask(workflow=flow, task_id='task3', runs_after=[task2])
        task4 = SuccessTask(workflow=flow, task_id='task4', runs_after=[task3])
        task5 = SuccessTask(workflow=flow, task_id='task5', runs_after=[task4])
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.FAILURE, task2.state)
        self.assertEqual(workflow.TaskState.FAILURE, task3.state)
        self.assertEqual(workflow.TaskState.FAILURE, task4.state)
        self.assertEqual(workflow.TaskState.FAILURE, task5.state)

    def testWorkflowFailure(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = FailureTask(workflow=flow, task_id='task2', runs_after=[task1])
        task3 = SuccessTask(workflow=flow, task_id='task3', runs_after=[task2])
        task4 = SuccessTask(workflow=flow, task_id='task4', runs_after=[task2, task3])
        flow.build()
        flow.process(nworkers=10)
        self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
        self.assertEqual(workflow.TaskState.FAILURE, task2.state)
        self.assertEqual(workflow.TaskState.FAILURE, task3.state)
        self.assertEqual(workflow.TaskState.FAILURE, task4.state)

    def testCircularDep(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1', runs_after=['task3'])
        task2 = FailureTask(workflow=flow, task_id='task2', runs_after=[task1])
        task3 = SuccessTask(workflow=flow, task_id='task3', runs_after=[task2])
        try:
            flow.build()
            self.fail()
        except workflow.CircularDependencyError:
            pass

    def testCircularDep2(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = FailureTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task2.must_run_after(task3)
        task2.must_run_before(task3)
        try:
            flow.build()
            self.fail()
        except workflow.CircularDependencyError:
            pass

    def testPersistentTaskRunNoFile(self):
        """Persistent task runs if output file does not exist.

        No output file is created if task fails.
        """
        class PTask(workflow.LocalFSPersistentTask):
            def run(self):
                return self.FAILURE

        flow = workflow.Workflow()
        file_path = base.random_alpha_num_word(16)
        task = PTask(
            output_file_path=file_path,
            task_id='task',
            workflow=flow,
        )
        flow.build()
        self.assertFalse(os.path.exists(file_path))
        self.assertFalse(flow.process())
        self.assertFalse(os.path.exists(file_path))

    def testPersistentTaskNoRunIfFile(self):
        """Persistent task does not run if output file already exists."""
        class PTask(workflow.LocalFSPersistentTask):
            def run(self):
                return self.FAILURE

        flow = workflow.Workflow()
        file_path = base.random_alpha_num_word(16)
        base.touch(file_path)
        try:
            task = PTask(
                output_file_path=file_path,
                task_id='task',
                workflow=flow,
            )
            flow.build()
            self.assertTrue(os.path.exists(file_path))
            self.assertTrue(flow.process())
            self.assertTrue(os.path.exists(file_path))
        finally:
            base.remove(file_path)

    def testPersistentTaskRunCreateFile(self):
        """Running a persistent task with success creates the output file."""
        class PTask(workflow.LocalFSPersistentTask):
            def run(self):
                return self.SUCCESS

        flow = workflow.Workflow()
        file_path = base.random_alpha_num_word(16)
        try:
            task = PTask(
                output_file_path=file_path,
                task_id='task',
                workflow=flow,
            )
            flow.build()
            self.assertFalse(os.path.exists(file_path))
            self.assertTrue(flow.process())
            self.assertTrue(os.path.exists(file_path))
        finally:
            base.remove(file_path)

    def test_dump_as_svg(self):
        flow1 = workflow.Workflow()
        task1 = SuccessTask(workflow=flow1, task_id='task1')
        task2 = FailureTask(workflow=flow1, task_id='task2')
        task3 = SuccessTask(workflow=flow1, task_id='task3')
        task2.must_run_after(task1)
        task3.must_run_after(task2)
        flow1.build()

        svg_source = flow1.dump_as_svg()
        logging.debug('SVG source:\n%s', svg_source)

        server = workflow.WorkflowHTTPMonitor(interface='127.0.0.1', port=0)
        server.start()
        try:
            flow1.process()

            conn = http.client.HTTPConnection(host='127.0.0.1', port=server.server_port)
            conn.connect()
            try:
                conn.request(method='GET', url='')
                response = conn.getresponse()
                self.assertEqual(404, response.getcode())

                server.set_workflow(flow1)

                conn.request(method='GET', url='/svg')
                response = conn.getresponse()
                self.assertEqual(200, response.getcode())
                self.assertEqual('image/svg+xml', response.getheader('Content-Type'))
                logging.debug('HTTP response: %r', response.read())
            finally:
                conn.close()

        finally:
            server.stop()

    def test_dump_state_as_table(self):
        flow1 = workflow.Workflow()
        task1 = SlowTask(workflow=flow1, task_id='task1')
        task2 = SlowTask(workflow=flow1, task_id='task2')
        task3 = SlowTask(workflow=flow1, task_id='task3')
        task2.must_run_after(task1)
        task3.must_run_after(task2)
        flow1.build()

        svg_source = flow1.dump_state_as_table()
        logging.debug('Workflow table:\n%s', svg_source)

        server = workflow.WorkflowHTTPMonitor(interface='127.0.0.1', port=0)
        server.start()
        server.set_workflow(flow1)
        try:
            flow1.process()

            conn = http.client.HTTPConnection(
                    host='127.0.0.1',
                    port=server.server_port,
            )
            conn.connect()
            try:
                conn.request(method='GET', url='')
                response = conn.getresponse()
                self.assertEqual(200, response.getcode())
                self.assertEqual('text/plain', response.getheader('Content-Type'))
                logging.debug('HTTP response: %r', response.read())
            finally:
                conn.close()

        finally:
            server.stop()

    def disabled_test_workflow_diff(self):
        flow1 = workflow.Workflow()
        task1 = SuccessTask(workflow=flow1, task_id='task1')
        task2 = SuccessTask(workflow=flow1, task_id='task2')
        task3 = SuccessTask(workflow=flow1, task_id='task3')
        task2.must_run_after(task1)
        task3.must_run_after(task2)
        flow1.build()

        flow2 = workflow.Workflow()
        task1 = SuccessTask(workflow=flow2, task_id='task1')
        task2b = SuccessTask(workflow=flow2, task_id='task2b')
        task3 = SuccessTask(workflow=flow2, task_id='task3')
        task2b.must_run_after(task1)
        task3.must_run_after(task2b)
        task3.must_run_after(task1)
        flow2.build()

        workflow.diff_workflow(flow1, flow2)

    def test_get_upstream_tasks(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task4 = SuccessTask(workflow=flow, task_id='task4')
        task5 = SuccessTask(workflow=flow, task_id='task5')
        task6 = SuccessTask(workflow=flow, task_id='task6')
        task7 = SuccessTask(workflow=flow, task_id='task7')

        task3.must_run_after(task1)
        task3.must_run_after(task2)
        task4.must_run_after(task3)
        task5.must_run_after(task3)

        task7.must_run_after(task6)

        self.assertEqual(
            set({task1}),
            workflow.get_upstream_tasks(flow, [task1]))

        self.assertEqual(
            set({task1, task2, task3}),
            workflow.get_upstream_tasks(flow, [task3]))

        self.assertEqual(
            set({task1, task2, task3, task4}),
            workflow.get_upstream_tasks(flow, [task4]))

        self.assertEqual(
            set({task1, task2, task3, task5}),
            workflow.get_upstream_tasks(flow, [task5]))

        self.assertEqual(
            set({task1, task2, task3, task4, task5}),
            workflow.get_upstream_tasks(flow, [task4, task5]))

        self.assertEqual(
            set({task1, task2, task3, task4, task5}),
            workflow.get_upstream_tasks(flow, [task3, task4, task5]))

        self.assertEqual(
            set({task6}),
            workflow.get_upstream_tasks(flow, [task6]))

        self.assertEqual(
            set({task6, task7}),
            workflow.get_upstream_tasks(flow, [task7]))

        self.assertEqual(
            set({task1, task2, task3, task4, task6, task7}),
            workflow.get_upstream_tasks(flow, [task4, task7]))

    def test_get_downstream_tasks(self):
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task4 = SuccessTask(workflow=flow, task_id='task4')
        task5 = SuccessTask(workflow=flow, task_id='task5')
        task6 = SuccessTask(workflow=flow, task_id='task6')
        task7 = SuccessTask(workflow=flow, task_id='task7')

        task3.must_run_after(task1)
        task3.must_run_after(task2)
        task4.must_run_after(task3)
        task5.must_run_after(task3)

        task7.must_run_after(task6)

        self.assertEqual(
            set({task1, task3, task4, task5}),
            workflow.get_downstream_tasks(flow, [task1]))

        self.assertEqual(
            set({task3, task4, task5}),
            workflow.get_downstream_tasks(flow, [task3]))

        self.assertEqual(
            set({task4}),
            workflow.get_downstream_tasks(flow, [task4]))

        self.assertEqual(
            set({task5}),
            workflow.get_downstream_tasks(flow, [task5]))

        self.assertEqual(
            set({task4, task5}),
            workflow.get_downstream_tasks(flow, [task4, task5]))

        self.assertEqual(
            set({task3, task4, task5}),
            workflow.get_downstream_tasks(flow, [task3, task4, task5]))

        self.assertEqual(
            set({task6, task7}),
            workflow.get_downstream_tasks(flow, [task6]))

        self.assertEqual(
            set({task7}),
            workflow.get_downstream_tasks(flow, [task7]))

        self.assertEqual(
            set({task3, task4, task5, task6, task7}),
            workflow.get_downstream_tasks(flow, [task3, task6]))

    def testAddDep(self):
        """Tests that forward declaration of dependencies are properly set."""
        flow = workflow.Workflow()
        flow.AddDep('task1', 'task2')
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        self.assertTrue('task1' in task2.runs_after)
        self.assertTrue('task2' in task1.runs_before)

    def test_prune(self):
        """Tests that forward declaration of dependencies are properly set."""
        flow = workflow.Workflow()
        task1 = SuccessTask(workflow=flow, task_id='task1')
        task2 = SuccessTask(workflow=flow, task_id='task2')
        task3 = SuccessTask(workflow=flow, task_id='task3')
        task4 = SuccessTask(workflow=flow, task_id='task4')
        task5 = SuccessTask(workflow=flow, task_id='task5')
        task6 = SuccessTask(workflow=flow, task_id='task6')
        task7 = SuccessTask(workflow=flow, task_id='task7')

        task3.must_run_after(task1)
        task3.must_run_after(task2)
        task4.must_run_after(task3)
        task5.must_run_after(task3)

        task7.must_run_after(task6)

        flow.prune(tasks={task4}, direction=workflow.UPSTREAM)
        self.assertEqual({'task1', 'task2', 'task3', 'task4'}, flow.tasks.keys())


# ------------------------------------------------------------------------------


def main(args):
    args = list(args)
    args.insert(0, sys.argv[0])
    unittest.main(argv=args)


if __name__ == '__main__':
    base.run(main)
