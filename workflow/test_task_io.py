#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

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


class Task1(workflow.IOTask):
  def RunWithIO(self, output):
    logging.info('Task1 is running NOW')
    output.timestamp = base.Timestamp()
    return self.SUCCESS


class Task2(workflow.IOTask):
  def GetTaskRunID(self):
    return '.'.join([
        self.task_id,
        'task1:timestamp=%s' % self.input.task1.timestamp,
    ])

  def RunWithIO(self, output, task1):
    logging.info('Task2 input task1=%r' % task1)
    return self.SUCCESS


class TestWorkflow(unittest.TestCase):
  def testTaskIO(self):
    flow = workflow.Workflow()
    task1 = Task1(workflow=flow, task_id='task1')
    task2 = Task2(workflow=flow, task_id='task2')

    task2.BindInputToTaskOutput('task1', task1)

    flow.Build()
    flow.Process(nworkers=10)

    self.assertEqual(workflow.TaskState.SUCCESS, task1.state)
    self.assertEqual(workflow.TaskState.SUCCESS, task2.state)

    print(flow.DumpAsDot())


# ------------------------------------------------------------------------------


def Main(args):
  args = list(args)
  args.insert(0, sys.argv[0])
  unittest.main(argv=args)


if __name__ == '__main__':
  base.Run(Main)
