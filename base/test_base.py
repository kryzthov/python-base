#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

"""Tests for module base.base"""

import logging
import unittest
import os

from base import base


class TestBase(unittest.TestCase):

  def testTouch(self):
    path = base.RandomAlphaNumWord(16)
    try:
      self.assertFalse(os.path.exists(path))
      base.Touch(path)
      self.assertTrue(os.path.exists(path))
      base.Touch(path)
      self.assertTrue(os.path.exists(path))
    finally:
      os.remove(path)

if __name__ == '__main__':
  unittest.main()