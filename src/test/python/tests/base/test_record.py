#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Unit-tests for the Record object."""

import logging
import sys
import tempfile
import time
import unittest

from base import base
from base import record

UNDEFINED = base.UNDEFINED


class TestRecord(unittest.TestCase):
    def testConfig(self):
        conf = record.Record()
        self.assertEqual([], dir(conf))

        logging.info('Step 1')
        self.assertFalse('x' in conf)
        self.assertEqual(UNDEFINED, conf.x)
        self.assertEqual(UNDEFINED, conf.get('x'))

        logging.info('Step 2')
        conf.x = 1
        self.assertTrue('x' in conf)
        self.assertEqual(1, conf.x)
        self.assertEqual(1, conf['x'])
        self.assertEqual(['x'], dir(conf))

        logging.info('Step 3')
        conf.x = 2
        self.assertTrue('x' in conf)
        self.assertEqual(2, conf.x)
        self.assertEqual(2, conf['x'])
        self.assertEqual(['x'], dir(conf))

        logging.info('Step 4')
        del conf['x']
        self.assertFalse('x' in conf)
        self.assertEqual(UNDEFINED, conf.x)
        self.assertEqual(UNDEFINED, conf.get('x'))
        self.assertEqual([], dir(conf))

        conf.x = 1
        del conf.x
        self.assertFalse('x' in conf)


    def test_write_load(self):
        conf = record.Record()
        conf.x = 1

        with tempfile.NamedTemporaryFile() as f:
            conf.write_to_file(f.name)
            logging.info('Writing record: %r', conf)
            new = record.load_from_file(f.name)
            logging.info('Loaded record: %r', new)
            self.assertEqual(1, new.x)


# ------------------------------------------------------------------------------


def main(args):
    args = list(args)
    args.insert(0, sys.argv[0])
    unittest.main(argv=args)


if __name__ == '__main__':
    base.run(main)
