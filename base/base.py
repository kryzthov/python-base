#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""Shared base utilities for Python applications.

Mainly:
 - Configures the logging system with both console and file logging.
 - Command-line flags framework allowing other modules to declare
   their own flags.
 - Program startup procedure.

Template for a Python application:
  from kiji import base

  FLAGS = base.FLAGS

  def Main(args):
    ...
    return os.EX_OK

  if __name__ == '__main__':
    base.Run(Main)
"""


import collections
import datetime
import getpass
import http.server
import json
import logging
import os
import random
import re
import signal
import socketserver
import subprocess
import sys
import tempfile
import threading
import time


# Constant to be used in place of None for default parameter values.
# Useful when None is a valid parameter value and cannot be used
# to mean "use the default".
Default = object()


class Error(Exception):
  """Errors used in this module."""
  pass


# ------------------------------------------------------------------------------
# Time utilities


def NowMS():
  """Returns: the current time, in ms since the Epoch."""
  return int(1000 * time.time())


def NowNS():
  """Returns: the current time, in ns since the Epoch."""
  return int(1000000000 * time.time())


def NowDateTime():
  """Returns: the current time as a date/time object (local timezone)."""
  return datetime.datetime.now()


def Timestamp():
  """Reports the current time as a human-readable timestamp.

  Timestamp has micro-second precision.
  Formatted as 'yyyymmdd-hhmmss-mmmmmm-tz'.

  Returns:
    the current time as a human-readable timestamp.
  """
  now_secs = time.time()  # in seconds since Epoch
  now = time.localtime(now_secs)
  now_subsecs = (now_secs - int(now_secs))
  microsecs = int(now_subsecs * 1000000)
  return '%04d%02d%02d-%02d%02d%02d-%06d-%s' % (
      now.tm_year,
      now.tm_mon,
      now.tm_mday,
      now.tm_hour,
      now.tm_min,
      now.tm_sec,
      microsecs,
      time.tzname[now.tm_isdst],
  )


# ------------------------------------------------------------------------------
# JSON utilities


JSON_DECODER = json.JSONDecoder()
JSON_ENCODER = json.JSONEncoder(
  indent=2,
  sort_keys=True,
)


def JsonDecode(json_str):
  """Decodes a JSON encoded string into a Python value.

  Args:
    json_str: JSON encoded string.
  Returns:
    The Python value decoded from the specified JSON string.
  """
  return JSON_DECODER.decode(json_str)


def JsonEncode(py_value):
  """Encodes a Python value into a JSON string.

  Args:
    py_value: Python value to encode as a JSON string.
  Returns:
    The specified Python value encoded as a JSON string.
  """
  return JSON_ENCODER.encode(py_value)


# ------------------------------------------------------------------------------
# Text utilities


def Truth(text):
  """Parses a human truth value.

  Accepts 'true', 'false', 'yes', 'no', 'y', 'n'.
  Parsing is case insensitive.

  Args:
    text: Input to parse.
  Returns:
    Parsed truth value as a bool.
  """
  lowered = text.lower()
  if lowered in frozenset(['y', 'yes', 'true']):
    return True
  elif lowered in frozenset(['n', 'no', 'false']):
    return False
  else:
    raise Error('Invalid truth value: %r' % text)


def RandomAlphaNumChar():
  """Generates a random character in [A-Za-z0-9]."""
  num = random.randint(0, 26 + 26 + 10)
  if num < 26:
    return chr(num + 65)
  num -= 26
  if num < 26:
    return chr(num + 97)
  return chr(num + 48)


def RandomAlphaNumWord(length):
  """Generates a random word with the specified length.

  Uses characters from the set [A-Za-z0-9].

  Args:
    length: Length of the word to generate.
  Returns:
    A random word of the request length.
  """
  return ''.join([RandomAlphaNumChar() for _ in range(0, length)])


def StripPrefix(string, prefix):
  """Strips a required prefix from a given string.

  Args:
    string: String required to start with the specified prefix.
    prefix: Prefix to remove from the string.
  Returns:
    The given string without the prefix.
  """
  assert string.startswith(prefix)
  return string[len(prefix):]


def StripOptionalPrefix(string, prefix):
  """Strips an optional prefix from a given string.

  Args:
    string: String, potentially starting with the specified prefix.
    prefix: Prefix to remove from the string.
  Returns:
    The given string with the prefix removed, if applicable.
  """
  if string.startswith(prefix):
    string = string[len(prefix):]
  return string


def StripSuffix(string, suffix):
  """Strips a required suffix from a given string.

  Args:
    string: String required to end with the specified suffix.
    suffix: Suffix to remove from the string.
  Returns:
    The given string with the suffix removed.
  """
  assert string.endswith(suffix)
  return string[:-len(suffix)]


def StripOptionalSuffix(string, suffix):
  """Strips an optional suffix from a given string.

  Args:
    string: String required to end with the specified suffix.
    suffix: Suffix to remove from the string.
  Returns:
    The given string with the suffix removed, if applicable.
  """
  if string.endswith(suffix):
    string = string[:-len(suffix)]
  return string


def StripMargin(text, separator='|'):
  """Strips the left margin from a given text block.

  Args:
    text: Multi-line text with a delimited left margin.
    separator: Separator used to delimit the left margin.
  Returns:
    The specified text with the left margin removed.
  """
  lines = text.split('\n')
  lines = map(lambda line: line.split(separator, 1)[-1], lines)
  return '\n'.join(lines)


def AddMargin(text, margin):
  """Adds a left margin to a given text block.

  Args:
    text: Multi-line text block.
    margin: Left margin to add at the beginning of each line.
  Returns:
    The specified text with the added left margin.
  """
  lines = text.split('\n')
  lines = map(lambda line: margin + line, lines)
  return '\n'.join(lines)


def WrapText(text, ncolumns):
  """Wraps a text block to fit in the specified number of columns.

  Args:
    text: Multi-line text block.
    ncolumns: Maximum number of columns allowed for each line.
  Returns:
    The specified text block, where each line is ncolumns at most.
  """
  def ListWrappedLines():
    for line in text.split('\n'):
      while len(line) > ncolumns:
        yield line[:ncolumns]
        line = line[ncolumns:]
      yield line
  return '\n'.join(ListWrappedLines())


def CamelCase(text, separator='_'):
  """Camel-cases a given character sequence.

  E.g. 'this_string' becomes 'ThisString'.

  Args:
    text: Sequence of characters to convert to camel-case.
    separator: Separator to use to identify words.
  Returns:
    The camel-cased sequence of characters.
  """
  return ''.join(map(str.capitalize, text.split(separator)))


def UnCamelCase(text, separator='_'):
  """Un-camel-cases a camel-cased word.

  For example:
   - 'ThisString' becomes 'this_string',
   - 'JIRA' becomes 'jira',
   - 'JIRATool' becomes 'jira_tool'.

  Args:
    text: Camel-case sequence of characters.
    separator: Separator to use to identify words.
  Returns:
    The un-camel-cased sequence of characters.
  """
  split = re.findall(r'[A-Z][a-z0-9]*', text)
  split = map(str.lower, split)
  split = list(split)

  words = []

  while len(split) > 0:
    word = split[0]
    split = split[1:]

    if len(word) == 1:
      while (len(split) > 0) and (len(split[0]) == 1):
        word += split[0]
        split = split[1:]

    words.append(word)

  return separator.join(words)


def Truncate(text, width, ellipsis='..'):
  """Truncates a text to the specified number of characters.

  Args:
    text: Text to truncate.
    width: Number of characters allowed.
    ellipsis: Optional ellipsis text to append, if truncation occurs.
  Returns:
    The given text, truncated to at most width characters.
    If truncation occurs, the truncated text ends with the ellipsis.
  """
  if len(text) > width:
    text = text[:(width - len(ellipsis))] + ellipsis
  return text


_RE_NO_IDENT_CHARS = re.compile(r'[^A-Za-z0-9_]+')


def MakeIdent(text, sep='_'):
  """Makes an identifier out of a random text.

  Args:
    text: Text to create an identifier from.
    sep: Separator used to replace non-identifier characters.
  Returns:
    An identifier made after the specified text.
  """
  return _RE_NO_IDENT_CHARS.sub(sep, text)


# ------------------------------------------------------------------------------


def GetProgramName():
  """Returns: this program's name."""
  return os.path.basename(sys.argv[0])


def ShellCommandOutput(command):
  """Runs a shell command and returns its output.

  Args:
    command: Shell command to run.
  Returns:
    The output from the shell command (stderr merged with stdout).
  """
  process = subprocess.Popen(
      args=['/bin/bash', '-c', command],
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
  )
  process.wait()
  output = process.stdout.read().decode()
  assert process.returncode == 0, (
      'Shell command failed: %r : %s' % (command, output))
  return output


class MultiThreadedHTTPServer(
    socketserver.ThreadingMixIn,
    http.server.HTTPServer,
):
  """Multi-threaded HTTP server."""
  pass


# ------------------------------------------------------------------------------
# Files utilities

def MakeDir(path):
  """Creates a directory if necessary.

  Args:
    path: Path of the directory to create.
  Returns:
    True if the directory was created, false if it already existed.
  """
  if os.path.exists(path):
    return False
  else:
    os.makedirs(path, exist_ok=True)
    return True


def Remove(path):
  """Removes the file with the given path.

  Does nothing if no file exists with the specified path.

  Returns:
    Whether the file was deleted.
  """
  try:
    os.remove(path)
    return True
  except FileNotFoundError:
    return False


def Touch(path, atime=None, mtime=None):
  """Equivalent of the shell command 'touch'.

  Args:
    path: Path of the file to touch.
    atime: Access time, in seconds since the Epoch.
    mtime: Modification time, in seconds since the Epoch.
  """
  assert ((atime is None) == (mtime is None))
  if atime is None:
    times = None
  else:
    times = (atime, mtime)
  with open(path, 'ab+') as f:
    # Note: there is a race condition here.
    os.utime(path, times=times)


def Exit():
  self_pid = os.getpid()
  logging.info('Forcibly terminating program (PID=%s)', self_pid)
  os.kill(self_pid, signal.SIGKILL)


# ------------------------------------------------------------------------------


class ImmutableDict(dict):
  """Dictionary guaranteed immutable.

  All mutations raise an exception.
  Behaves exactly as a dict otherwise.
  """

  def __init__(self, items=None, **kwargs):
    if items is not None:
      super(ImmutableDict, self).__init__(items)
      assert (len(kwargs) == 0)
    else:
      super(ImmutableDict, self).__init__(**kwargs)

  def __setitem__(self, key, value):
    raise Exception(
        'Attempting to map key %r to value %r in ImmutableDict %r'
        % (key, value, self))

  def __delitem__(self, key):
    raise Exception(
        'Attempting to remove mapping for key %r in ImmutableDict %r'
        % (key, self))

  def clear(self):
    raise Exception('Attempting to clear ImmutableDict %r' % self)

  def update(self, items=None, **kwargs):
    raise Exception(
        'Attempting to update ImmutableDict %r with items=%r, kwargs=%r'
        % (self, args, kwargs))

  def pop(self, key, default=None):
    raise Exception(
        'Attempting to pop key %r from ImmutableDict %r' % (key, self))

  def popitem(self):
    raise Exception('Attempting to pop item from ImmutableDict %r' % self)


# ------------------------------------------------------------------------------


RE_FLAG = re.compile(r'^--?([^=]+)(?:=(.*))?$')


class Flags(object):
  """Wrapper for command-line flags."""

  class StringFlag(object):
    """Basic string flag parser."""

    def __init__(self, name, default=None, help=None):
      """Constructs a specification for a string CLI flag.

      Args:
        name: Command-line flag name, eg. 'flag-name' or 'flag_name'.
            Dashes are normalized to underscores.
        default: Optional default value for the flag if unspecified.
        help: Help message to include when displaying the usage text.
      """
      self._name = name.replace('-', '_')
      self._value = default
      self._default = default
      self._help = help

    def Parse(self, argument):
      """Parses the command-line argument.

      Args:
        argument: Command-line argument, as a string.
      """
      self._value = argument

    @property
    def type(self):
      return 'string'

    @property
    def name(self):
      return self._name

    @property
    def value(self):
      return self._value

    @property
    def default(self):
      return self._default

    @property
    def help(self):
      return self._help

  class IntegerFlag(StringFlag):
    @property
    def type(self):
      return 'integer'

    def Parse(self, argument):
      self._value = int(argument)

  class FloatFlag(StringFlag):
    @property
    def type(self):
      return 'float'

    def Parse(self, argument):
      self._value = float(argument)

  class BooleanFlag(StringFlag):
    @property
    def type(self):
      return 'boolean'

    def Parse(self, argument):
      if argument is None:
        self._value = True
      else:
        self._value = Truth(argument)

  # ----------------------------------------------------------------------------

  def __init__(self):
    # Map: flag name -> flag definition
    self._defs = {}

    # After parsing, tuple of unparsed arguments:
    self._unparsed = None

  def Add(self, flag_def):
    assert (flag_def.name not in self._defs), (
        'Flag %r already defined' % flag_def.name)
    self._defs[flag_def.name] = flag_def

  def AddInteger(self, name, **kwargs):
    self.Add(Flags.IntegerFlag(name, **kwargs))

  def AddString(self, name, **kwargs):
    self.Add(Flags.StringFlag(name, **kwargs))

  def AddBoolean(self, name, **kwargs):
    self.Add(Flags.BooleanFlag(name, **kwargs))

  def AddFloat(self, name, **kwargs):
    self.Add(Flags.FloatFlag(name, **kwargs))

  def Parse(self, args):
    """Parses the command-line arguments.

    Args:
      args: List of command-line arguments.
    Returns:
      Whether successful.
    """
    unparsed = []

    skip_parse = False

    for arg in args:
      if arg == '--':
        skip_parse = True
        continue

      if skip_parse:
        unparsed.append(arg)
        continue

      match = RE_FLAG.match(arg)
      if match is None:
        unparsed.append(arg)
        continue

      key = match.group(1).replace('-', '_')
      value = match.group(2)

      if key not in self._defs:
        unparsed.append(arg)
        continue

      self._defs[key].Parse(value)

    self._unparsed = tuple(unparsed)
    return True

  def __getattr__(self, name):
    assert (self._unparsed is not None), (
        'Flags have not been parsed yet: cannot access flag %r' % name)
    return self._defs[name].value

  def GetUnparsed(self):
    assert (self._unparsed is not None), 'Flags have not been parsed yet'
    return self._unparsed

  def ListFlags(self):
    """Lists the declared flags.

    Yields:
      Pair: (flag name, flag descriptor).
    """
    yield from self._defs.items()

  def PrintUsage(self, header='Flags:'):
    indent = 8
    ncolumns = Terminal.columns

    print(header)
    for (name, flag) in sorted(self._defs.items()):
      flag_help = WrapText(text=flag.help, ncolumns=ncolumns - indent)
      print(' --%s: %s = %r\n%s\n' % (
          name,
          flag.type,
          flag.default,
          AddMargin(StripMargin(flag_help), ' ' * indent),
      ))


FLAGS = Flags()


# ------------------------------------------------------------------------------


def MakeTuple(name, **kwargs):
  """Creates a read-only named-tuple with the specified key/value pair.

  Args:
    name: Name of the named-tuple.
    **kwargs: Key/value pairs in the named-tuple.
  Returns:
    A read-only named-tuple with the specified name and key/value pairs.
  """
  tuple_class = collections.namedtuple(
      typename=name,
      field_names=kwargs.keys(),
  )
  return tuple_class(**kwargs)


# Shell exit codes:
ExitCode = MakeTuple('ExitCode',
  SUCCESS = os.EX_OK,
  FAILURE = 1,
  USAGE   = os.EX_USAGE,
)


# ------------------------------------------------------------------------------


LogLevel = MakeTuple('LogLevel',
  FATAL         = 50,
  ERROR         = 40,
  WARNING       = 30,
  INFO          = 20,
  DEBUG         = 10,
  DEBUG_VERBOSE = 5,
  ALL           = 0,
)


def ParseLogLevelFlag(level):
  """Parses a logging level command-line flag.

  Args:
    level: Logging level command-line flag (string).
  Returns:
    Logging level (integer).
  """
  log_level = getattr(LogLevel, level.upper(), None)
  if type(log_level) == int:
    return log_level

  try:
    return int(level)
  except ValueError:
    level_names = sorted(LogLevel._asdict().keys())
    raise Error('Invalid logging-level %r. Use one of %s or an integer.'
                % (level, ', '.join(level_names)))


FLAGS.AddString(
    name='log_level',
    default='DEBUG',
    help=('Master log level (integer or level name).\n'
          'Overrides specific logging levels.'),
)

FLAGS.AddString(
    name='log_console_level',
    default='INFO',
    help='Filter log statements sent to the console (integer or level name).',
)

FLAGS.AddString(
    name='log_file_level',
    default='ALL',
    help='Filter log statements sent to the log file (integer or level name).',
)

FLAGS.AddString(
    name='log_dir',
    default=None,
    help='Directory where to write logs.',
)


# ------------------------------------------------------------------------------


def Synchronized(lock=None):
  """Decorator for synchronized functions.

  Similar but not equivalent to Java synchronized methods.
  """
  if lock is None:
    lock = threading.Lock()

  def _Wrap(function):
    def _SynchronizedWrapper(*args, **kwargs):
      with lock:
        return function(*args, **kwargs)
    return _SynchronizedWrapper

  return _Wrap


# ------------------------------------------------------------------------------


def Memoize():
  """Returns a decorator to memoize the function it is applied to.

  Memoization is incompatible with functions whose parameters are mutable.
  This memoization implementation is fairly incomplete and primitive.
  In particular, positional and keyword parameters are not normalized.
  """
  UNKNOWN = object()

  def Decorator(function):
    """Wraps a function and returns its memoized version.

    Args:
      function: Function to wrap with memoization.
    Returns:
      The memoized version of the specified function.
    """
    # Map: args tuple -> function(args)
    memoize = dict()

    def MemoizeWrapper(*args, **kwargs):
      all_args = (args, tuple(sorted(kwargs.items())))
      value = memoize.get(all_args, UNKNOWN)
      if value is UNKNOWN:
        value = function(*args, **kwargs)
        memoize[all_args] = value
      return value

    return MemoizeWrapper

  return Decorator


# ------------------------------------------------------------------------------


class _Terminal(object):
  """Map of terminal colors."""

  # Escape sequence to clear the screen:
  clear = '\033[2J'

  # Escape sequence to move the cursor to (y,x):
  move = '\033[%s;%sH'

  def MoveTo(self, x, y):
    """Makes an escape sequence to move the cursor."""
    return _Terminal.move % (y,x)

  normal    = '\033[0m'
  bold      = '\033[1m'
  underline = '\033[4m'
  blink     = '\033[5m'
  reverse   = '\033[7m'

  FG = MakeTuple('ForegroundColor',
    black   = '\033[01;30m',
    red     = '\033[01;31m',
    green   = '\033[01;32m',
    yellow  = '\033[01;33m',
    blue    = '\033[01;34m',
    magenta = '\033[01;35m',
    cyan    = '\033[01;36m',
    white   = '\033[01;37m',
  )

  BG = MakeTuple('BackgroundColor',
    black   = '\033[01;40m',
    red     = '\033[01;41m',
    green   = '\033[01;42m',
    yellow  = '\033[01;43m',
    blue    = '\033[01;44m',
    magenta = '\033[01;45m',
    cyan    = '\033[01;46m',
    white   = '\033[01;47m',
  )

  @property
  def columns(self):
    """Reports the number of columns in the calling shell.

    Returns:
      The number of columns in the terminal.
    """
    return int(ShellCommandOutput('tput cols'))

  @property
  def lines(self):
    """Reports the number of lines in the calling shell.

    Returns:
      The number of lines in the terminal.
    """
    return int(ShellCommandOutput('tput lines'))


Terminal = _Terminal()


# ------------------------------------------------------------------------------


RE_TEMPLATE_FIELD_LINE = re.compile(r'^(\w+):\s*(.*)\s*$')


def ParseTemplate(template):
  """Parses an issue template.

  A template is a text file describing fields with lines such as:
      'field_name: the field value'.
  A field value may span several lines, until another 'field_name:' is found.
  Lines starting with '#' are comments and are discarded.
  Field values are stripped of their leading/trailing spaces and new lines.

  Args:
    template: Filled-in template text.
  Yields:
    Pairs (field name, field text).
  """
  field_name = None
  field_value = []

  for line in template.strip().split('\n') + ['end:']:
    if line.startswith('#'): continue
    match = RE_TEMPLATE_FIELD_LINE.match(line)
    if match:
      if field_name is not None:
        yield (field_name, '\n'.join(field_value).strip())
      elif len(field_value) > 0:
        logging.warning('Ignoring lines: %r', field_value)

      field_name = match.group(1)
      field_value = [match.group(2)]
    else:
      field_value.append(line)


def InputTemplate(template, fields):
  """Queries the user for inputs through a template file.

  Args:
    template: Template with placeholders for the fields.
        Specified as a Python format with named % markers.
    fields: Dictionary with default values for the template.
  Returns:
    Fields dictionary as specified/modified by the user.
  """
  editor = os.environ.get('EDITOR', '/usr/bin/vim')
  with tempfile.NamedTemporaryFile('w+t') as f:
    f.write(template % fields)
    f.flush()
    user_command = '%s %s' % (editor, f.name)
    if os.system(user_command) != 0:
      raise Error('Error acquiring user input (command was %r).' % user_command)
    with open(f.name, 'r') as f:
      filled_template = f.read()

  fields = dict(ParseTemplate(filled_template))
  return fields


# ------------------------------------------------------------------------------


HttpMethod = MakeTuple('HttpMethod',
  GET = 'GET',
  POST = 'POST',
  PUT = 'PUT',
  DELETE = 'DELETE',
  HEAD = 'HEAD',
  OPTIONS = 'OPTIONS',
  TRACE = 'TRACE',
  CONNECT = 'CONNECT',
)


ContentType = MakeTuple('ContentType',
  JSON = 'application/json',
  XML = 'application/xml',
  PATCH = 'text/x-patch',
  PLAIN = 'text/plain',
)


# ------------------------------------------------------------------------------


def Run(main):
  """Runs a Python program's Main() function.

  Args:
    main: Main function, with an expected signature like
      exit_code = main(args)
      where args is a list of the unparsed command-line arguments.
  """
  # Parse command-line arguments:
  program_name = GetProgramName()
  if not FLAGS.Parse(sys.argv[1:]):
    FLAGS.PrintUsage()
    return os.EX_USAGE

  # Initialize the logging system:
  try:
    log_root_level = ParseLogLevelFlag(FLAGS.log_level)
    log_console_level = ParseLogLevelFlag(FLAGS.log_console_level)
    log_file_level = ParseLogLevelFlag(FLAGS.log_file_level)
  except Error as err:
    print(err)
    return os.EX_USAGE

  log_formatter = logging.Formatter(
      fmt='%(asctime)s %(levelname)s %(filename)s:%(lineno)s : %(message)s',
  )
  # Override the log date formatter to include the time zone:
  def FormatTime(record, datefmt=None):
    time_tuple = time.localtime(record.created)
    tz_name = time.tzname[time_tuple.tm_isdst]
    return '%(date_time)s-%(millis)03d-%(tz_name)s' % dict(
        date_time=time.strftime('%Y%m%d-%H%M%S', time_tuple),
        millis=record.msecs,
        tz_name=tz_name,
    )
  log_formatter.formatTime = FormatTime

  logging.root.setLevel(log_root_level)

  console_handler = logging.StreamHandler()
  console_handler.setFormatter(log_formatter)
  console_handler.setLevel(log_console_level)
  logging.root.addHandler(console_handler)

  logging.addLevelName(LogLevel.DEBUG_VERBOSE, 'DEBUG_VERBOSE')
  logging.addLevelName(LogLevel.ALL, 'ALL')

  # Initialize log dir:
  timestamp = Timestamp()
  pid = os.getpid()

  if FLAGS.log_dir is None:
    tmp_dir = os.path.join('/tmp', getpass.getuser(), program_name)
    if not os.path.exists(tmp_dir): os.makedirs(tmp_dir)
    FLAGS.log_dir = tempfile.mkdtemp(
        prefix='%s.%d.' % (timestamp, pid),
        dir=tmp_dir)
  logging.info('Using log dir: %s', FLAGS.log_dir)
  if not os.path.exists(FLAGS.log_dir):
    os.makedirs(FLAGS.log_dir)

  log_file = os.path.join(FLAGS.log_dir,
                          '%s.%s.%d.log' % (program_name, timestamp, pid))

  file_handler = logging.FileHandler(filename=log_file)
  file_handler.setFormatter(log_formatter)
  file_handler.setLevel(log_file_level)
  logging.root.addHandler(file_handler)

  # Run program:
  sys.exit(main(FLAGS.GetUnparsed()))


if __name__ == '__main__':
  raise Error('%r cannot be used as a standalone script.' % sys.argv[0])
