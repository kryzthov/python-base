#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

"""Unit-tests for the Record object."""

import logging
import sys
import tempfile
import time
import unittest

from base import base
from base import record

Undefined = base.Undefined


class TestRecord(unittest.TestCase):
  def testConfig(self):
    conf = record.Record()

    logging.info('Step 1')
    self.assertFalse('x' in conf)
    self.assertEqual(Undefined, conf.x)
    self.assertEqual(Undefined, conf.Get('x'))

    logging.info('Step 2')
    conf.x = 1
    self.assertTrue('x' in conf)
    self.assertEqual(1, conf.x)
    self.assertEqual(1, conf['x'])

    logging.info('Step 3')
    conf.x = 2
    self.assertTrue('x' in conf)
    self.assertEqual(2, conf.x)
    self.assertEqual(2, conf['x'])

    logging.info('Step 4')
    del conf['x']
    self.assertFalse('x' in conf)
    self.assertEqual(Undefined, conf.x)
    self.assertEqual(Undefined, conf.Get('x'))

    conf.x = 1
    del conf.x
    self.assertFalse('x' in conf)

  def testWriteLoad(self):
    conf = record.Record()
    conf.x = 1

    with tempfile.NamedTemporaryFile() as f:
      conf.WriteToFile(f.name)
      logging.info('Writing record: %r', conf)
      new = record.LoadFromFile(f.name)
      logging.info('Loaded record: %r', new)
      self.assertEqual(1, new.x)


# ------------------------------------------------------------------------------


def Main(args):
  args = list(args)
  args.insert(0, sys.argv[0])
  unittest.main(argv=args)


if __name__ == '__main__':
  base.Run(Main)
