#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Tests for module base.base"""

import logging
import os
import sys
import unittest

from base import base


class TestBase(unittest.TestCase):
    """Tests for the base module."""

    def test_touch(self):
        path = base.RandomAlphaNumWord(16)
        try:
            self.assertFalse(os.path.exists(path))
            base.touch(path)
            self.assertTrue(os.path.exists(path))
            base.touch(path)
            self.assertTrue(os.path.exists(path))
        finally:
            os.remove(path)

    def test_un_camel_case(self):
        self.assertEqual('jira', base.un_camel_case('JIRA'))
        self.assertEqual('jira_tool', base.un_camel_case('JIRATool'))
        self.assertEqual('jira_tool', base.un_camel_case('jira_tool'))
        self.assertEqual('jira_tool', base.un_camel_case('jira tool'))
        self.assertEqual('jira_tool', base.un_camel_case('Jira tool'))
        self.assertEqual('jira_tool', base.un_camel_case(' Jira tool'))
        self.assertEqual('status_csv', base.un_camel_case(' StatusCSV'))


def main(args):
    args = list(args)
    args.insert(0, sys.argv[0])
    unittest.main(argv=args)


if __name__ == '__main__':
    base.run(main)
