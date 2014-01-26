#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""CLI tool actions."""

import abc
import os

from base import base


HelpFlag = base.MakeTuple('HelpFlag',
  ADD_HANDLE = 0,
  ADD_NO_HANDLE = 1,
  NO_ADD = 2,
)


class Action(object, metaclass=abc.ABCMeta):
  """Abstract base class for CLI actions."""

  # Name of this action, optionally overridden by sub-classes.
  # Default behavior is to use the un-camel-cased class name.
  NAME = None

  @classmethod
  def GetName(cls):
    """Returns this action's name."""
    name = cls.NAME
    if name is None:
      name = cls.__name__
    return base.UnCamelCase(name, separator='-')

  # Usage of this action, overridden by sub-classes:
  USAGE = """
    |Usage:
    |  %(this)s <flags> ...
    """

  def __init__(
      self,
      parent_flags=None,
      help_flag=HelpFlag.ADD_HANDLE,
  ):
    """Initializes the action.

    Most of the initialization relies on command-line flags.

    Args:
      parent_flags: Parent flags to inherit from.
      help_flag: Whether to add and/or handle a --help flag.
          Default is to add and to handle a --help flag.
    """
    name = 'Flags for %s (%s.%s)' % (
        self.GetName(),
        type(self).__module__,
        type(self).__name__)
    self._flags = base.Flags(name=name, parent=parent_flags)
    self._help_flag = help_flag
    if self._help_flag in (HelpFlag.ADD_HANDLE, HelpFlag.ADD_NO_HANDLE):
      if 'help' not in self._flags:
        self._flags.AddBoolean(
            name='help',
            default=False,
            help='Print the help message for this action.',
        )
    self.RegisterFlags()

  def RegisterFlags(self):
    """Sub-classes may override this method to register flags."""
    pass

  @property
  def flags(self):
    return self._flags

  @abc.abstractmethod
  def Run(self, args):
    """Sub-classes must override this method to implement the action.

    Args:
      args: Command-line arguments for this action.
    Returns:
      Exit code.
    """
    raise Error('Abstract method')

  def __call__(self, args):
    """Allows invoking an action as a function.

    Args:
      args: Command-line arguments specific to the action.
    Returns:
      Exit code.
    """
    if not self._flags.Parse(args):
      return os.EX_USAGE

    if (self._help_flag == HelpFlag.ADD_HANDLE) and self.flags.help:
      print(base.StripMargin(self.USAGE.strip() % {
          'this': '%s %s' % (base.GetProgramName(), self.GetName()),
      }))
      print()
      self.flags.PrintUsage()
      return os.EX_OK

    return self.Run(self._flags.GetUnparsed())


if __name__ == '__main__':
  raise Error('%r cannot be used as a standalone script.' % args[0])
