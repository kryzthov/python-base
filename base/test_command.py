#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Tests for module base.base"""

import logging
import os
import sys
import unittest

from base import base
from base import command


class TestCommand(unittest.TestCase):

    def test_command(self):
        cmd = command.Command('/bin/ls', '-ald', '/tmp', exit_code=0)
        self.assertEqual(0, cmd.exit_code)
        self.assertTrue('/tmp' in cmd.output_text)

    def test_exit_code(self):
        cmd = command.Command('false', exit_code=1)

    def test_args_list(self):
        cmd = command.Command(args=['false'], exit_code=1)


def main(args):
    args = list(args)
    args.insert(0, sys.argv[0])
    unittest.main(argv=args)


if __name__ == '__main__':
    base.run(main)
