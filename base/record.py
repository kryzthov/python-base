#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

import logging
import pickle

from base import base


class Record(object):
  """Record that behaves like a dictionary and an object."""

  def __init__(self, params=None):
    """Initializes a new record."""
    if params is None:
      params = dict()
    self.__dict__['_params'] = params

  def __setattr__(self, key, value):
    self._params[key] = value

  def __getattr__(self, key):
    return self._params.get(key, base.Undefined)

  def __delattr__(self, key):
    try:
      del self._params[key]
    except KeyError as err:
      raise AttributeError(key)

  def __contains__(self, key):
    return key in self._params

  def __setitem__(self, key, value):
    self._params[key] = value

  def __delitem__(self, key):
    del self._params[key]

  def __getitem__(self, key):
    return self._params[key]

  def Get(self, key, default=base.Undefined):
    return self._params.get(key, default)

  def Copy(self):
    """Returns: a copy of this record."""
    return self.With()

  def With(self, **kwargs):
    """Creates a copy of this record with some parameters updated.

    Args:
      kwargs: Collection of parameters to set.
    Returns:
      A copy of this record with the specified parameters updated.
    """
    params = dict(self._params)
    params.update(kwargs)
    return Record(params)

  def __str__(self):
    return ('Record{%s}'
            % ','.join(map(lambda item: '%s=%s' % item,
                           self._params.items())))

  def __repr__(self):
    return ('Record(%s)'
            % ','.join(map(lambda item: '%s=%r' % item,
                           self._params.items())))

  def __getnewargs__(self):
    """Customize deserialization through pickle."""
    return ()

  def __getstate__(self):
    """Customize serialization through pickle."""
    return dict(_params=self._params)

  def __setstate__(self, state):
    """Customize deserialization through pickle."""
    self.__dict__['_params'] = state['_params']

  def WriteToFile(self, file_path):
    with open(file_path, 'wb') as f:
      pickler = pickle.Pickler(f)
      pickler.dump(self)


def LoadFromFile(file_path):
  """Loads a record from a file.

  Args:
    file_path: Path of the file to load the record from.
  Returns:
    The record loaded from the given file.
  """
  with open(file_path, 'rb') as f:
    pickler = pickle.Unpickler(f)
    return pickler.load()
