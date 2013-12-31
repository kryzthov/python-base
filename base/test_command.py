#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

"""Tests for module base.base"""

import logging
import os
import sys
import unittest

from base import base
from base import command


class TestCommand(unittest.TestCase):

  def testCommand(self):
    cmd = command.Command('/bin/ls', '-ald', '/tmp', exit_code=0)
    self.assertEqual(0, cmd.exit_code)
    self.assertTrue('/tmp' in cmd.output_text)

  def testExitCode(self):
    cmd = command.Command('false', exit_code=1)

  def testArgsList(self):
    cmd = command.Command(args=['false'], exit_code=1)


def Main(args):
  args = list(args)
  args.insert(0, sys.argv[0])
  unittest.main(argv=args)


if __name__ == '__main__':
  base.Run(Main)
