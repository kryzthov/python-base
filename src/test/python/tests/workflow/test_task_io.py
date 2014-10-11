#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Unit-tests for task I/O."""

import abc
import http.client
import logging
import os
import re
import sys
import time
import unittest

from base import base

from workflow import workflow


# --------------------------------------------------------------------------------------------------


class Task1(workflow.IOTask):
    def run_with_io(self, output):
        logging.info('Task1 is running NOW')
        output.timestamp = base.timestamp()
        return self.SUCCESS


class Task2(workflow.IOTask):
    def get_task_run_id(self):
        return '.'.join([self.task_id, 'task1:timestamp=%s' % self.input.task1.timestamp])

    def run_with_io(self, output, task1):
        logging.info('Task2 input task1=%r' % task1)
        return self.SUCCESS


# --------------------------------------------------------------------------------------------------


class TestWorkflow(unittest.TestCase):
    def test_task_io(self):
        try:
            flow = workflow.Workflow()
            task1 = Task1(workflow=flow, task_id='task1')
            task2 = Task2(workflow=flow, task_id='task2')

            task2.bind_input_to_task_output('task1', task1)

            flow.build()
            flow.process(nworkers=10)

            self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
            self.assertEqual(workflow.TaskState.SUCCESS, task2.state)
        finally:
            os.remove(task1.get_task_run_id())
            os.remove(task2.get_task_run_id())

    def test_should_task_run(self):
        class TestTask(workflow.IOTask):
            def __init__(self, should_task_run=True, **kwargs):
                super().__init__(**kwargs)
                self._should_counter = 0
                self._run_counter = 0
                self._should_task_run = should_task_run

            def should_task_run(self, task_run_id, output, **inputs):
                self._should_counter += 1
                return self._should_task_run

            def run_with_io(self, output, **inputs):
                self._run_counter += 1
                output.time = time.time()
                return self.SUCCESS

        flow = workflow.Workflow()
        task = TestTask(workflow=flow, task_id="TestTask")
        flow.build()
        flow.process()

        self.assertEqual(0, task._should_counter)
        self.assertEqual(1, task._run_counter)

        flow = workflow.Workflow()
        task = TestTask(workflow=flow, task_id="TestTask", should_task_run=True)
        flow.build()
        flow.process()

        self.assertEqual(1, task._should_counter)
        self.assertEqual(1, task._run_counter)

        flow = workflow.Workflow()
        task = TestTask(workflow=flow, task_id="TestTask", should_task_run=False)
        flow.build()
        flow.process()

        self.assertEqual(1, task._should_counter)
        self.assertEqual(0, task._run_counter)

        os.remove(task.get_task_run_id())



# --------------------------------------------------------------------------------------------------


def main(args):
    args = list(args)
    args.insert(0, sys.argv[0])
    unittest.main(argv=args)


if __name__ == '__main__':
    base.run(main)
