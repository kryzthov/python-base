#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

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


class Default(object):
    """Singleton used to in place of None for default parameter values.

    Useful when None is a valid parameter value and cannot be used
    to mean "use the default".
    """
    pass

DEFAULT = Default()


class Undefined(object):
    """Singleton used in place of None to represent a missing dictionary entry."""
    pass

UNDEFINED = Undefined()


# --------------------------------------------------------------------------------------------------


class Error(Exception):
    """Errors used in this module."""
    pass


# --------------------------------------------------------------------------------------------------
# Time utilities


def now_ms():
    """Returns: the current time, in ms since the Epoch."""
    return int(1000 * time.time())


def now_ns():
    """Returns: the current time, in ns since the Epoch."""
    return int(1000000000 * time.time())


def now_date_time():
    """Returns: the current time as a date/time object (local timezone)."""
    return datetime.datetime.now()


def timestamp(tstamp=None):
    """Reports the current time as a human-readable timestamp.

    Timestamp has micro-second precision.
    Formatted as 'yyyymmdd-hhmmss-mmmmmm-tz'.

    Args:
        tstamp: Optional explicit timestamp, in seconds since Epoch.
    Returns:
        the current time as a human-readable timestamp.
    """
    if tstamp is None:
        tstamp = time.time()  # in seconds since Epoch
    now = time.localtime(tstamp)
    ts_subsecs = (tstamp - int(tstamp))
    microsecs = int(ts_subsecs * 1000000)
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


# --------------------------------------------------------------------------------------------------
# JSON utilities


JSON_DECODER = json.JSONDecoder()
PRETTY_JSON_ENCODER = json.JSONEncoder(
    indent=2,
    sort_keys=True,
)
COMPACT_JSON_ENCODER = json.JSONEncoder(
    indent=None,
    sort_keys=True,
    separators=(',', ':'),
)


def json_decode(json_str):
    """Decodes a JSON encoded string into a Python value.

    Args:
        json_str: JSON encoded string.
    Returns:
        The Python value decoded from the specified JSON string.
    """
    return JSON_DECODER.decode(json_str)


def json_encode(py_value, pretty=True):
    """Encodes a Python value into a JSON string.

    Args:
        py_value: Python value to encode as a JSON string.
        pretty: True means pretty, False means compact.
    Returns:
        The specified Python value encoded as a JSON string.
    """
    encoder = PRETTY_JSON_ENCODER if pretty else COMPACT_JSON_ENCODER
    return encoder.encode(py_value)


# --------------------------------------------------------------------------------------------------
# Text utilities


def truth(text):
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


def random_alpha_num_char():
    """Generates a random character in [A-Za-z0-9]."""
    num = random.randint(0, 26 + 26 + 10)
    if num < 26:
        return chr(num + 65)
    num -= 26
    if num < 26:
        return chr(num + 97)
    return chr(num + 48)


def random_alpha_num_word(length):
    """Generates a random word with the specified length.

    Uses characters from the set [A-Za-z0-9].

    Args:
        length: Length of the word to generate.
    Returns:
        A random word of the request length.
    """
    return ''.join([random_alpha_num_char() for _ in range(0, length)])


def strip_prefix(string, prefix):
    """Strips a required prefix from a given string.

    Args:
        string: String required to start with the specified prefix.
        prefix: Prefix to remove from the string.
    Returns:
        The given string without the prefix.
    """
    assert string.startswith(prefix)
    return string[len(prefix):]


def strip_optional_prefix(string, prefix):
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


def strip_suffix(string, suffix):
    """Strips a required suffix from a given string.

    Args:
        string: String required to end with the specified suffix.
        suffix: Suffix to remove from the string.
    Returns:
        The given string with the suffix removed.
    """
    assert string.endswith(suffix)
    return string[:-len(suffix)]


def strip_optional_suffix(string, suffix):
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


def strip_margin(text, separator='|'):
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


def add_margin(text, margin):
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


def wrap_text(text, ncolumns):
    """Wraps a text block to fit in the specified number of columns.

    Args:
        text: Multi-line text block.
        ncolumns: Maximum number of columns allowed for each line.
    Returns:
        The specified text block, where each line is ncolumns at most.
    """
    def list_wrapped_lines():
        """Parses the input text into a stream of lines."""
        for line in text.split('\n'):
            while len(line) > ncolumns:
                yield line[:ncolumns]
                line = line[ncolumns:]
            yield line
    return '\n'.join(list_wrapped_lines())


def camel_case(text, separator='_'):
    """Camel-cases a given character sequence.

    E.g. 'this_string' becomes 'ThisString'.

    Args:
        text: Sequence of characters to convert to camel-case.
        separator: Separator to use to identify words.
    Returns:
        The camel-cased sequence of characters.
    """
    return ''.join(map(str.capitalize, text.split(separator)))


def un_camel_case(text, separator='_'):
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
    split = re.findall(r'(?:[A-Z][a-z0-9]*|[a-z0.9]+)', text)
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


def truncate(text, width, ellipsis='..'):
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


def make_ident(text, sep='_'):
    """Makes an identifier out of a random text.

    Args:
        text: Text to create an identifier from.
        sep: Separator used to replace non-identifier characters.
    Returns:
        An identifier made after the specified text.
    """
    return _RE_NO_IDENT_CHARS.sub(sep, text)


# --------------------------------------------------------------------------------------------------


def _compute_program_name():
    """Reports this program's name."""
    program_path = os.path.abspath(sys.argv[0])
    if not os.path.exists(program_path):
        return 'unknown'
    else:
        return os.path.basename(program_path)


_PROGRAM_NAME = _compute_program_name()


def set_program_name(program_name):
    """Sets this program's name."""
    global _PROGRAM_NAME
    _PROGRAM_NAME = program_name


def get_program_name():
    """Returns: this program's name."""
    global _PROGRAM_NAME
    return _PROGRAM_NAME


def shell_command_output(command):
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
    assert process.returncode == 0, ('Shell command failed: %r : %s' % (command, output))
    return output


class MultiThreadedHTTPServer(
        socketserver.ThreadingMixIn,
        http.server.HTTPServer,
):
    """Multi-threaded HTTP server."""
    pass


# --------------------------------------------------------------------------------------------------
# Files utilities

def make_dir(path):
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


def remove(path):
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


def touch(path, atime=None, mtime=None):
    """Equivalent of the shell command 'touch'.

    Args:
        path: Path of the file to touch.
        atime: Access time, in seconds since the Epoch.
        mtime: Modification time, in seconds since the Epoch.
    """
    assert ((atime is None) == (mtime is None)), 'atime and mtime are exclusive'
    if atime is None:
        times = None
    else:
        times = (atime, mtime)
    with open(path, 'ab+') as file:
        # Note: there is a race condition here.
        os.utime(path, times=times)


def shutdown():
    """Forcibly terminates this program."""
    self_pid = os.getpid()
    logging.info('Forcibly terminating program (PID=%s)', self_pid)
    os.kill(self_pid, signal.SIGKILL)


def list_java_processes():
    """Lists the Java processes.

    Yields:
        Pairs (PID, Java class name).
    """
    for line in shell_command_output('jps -l').splitlines():
        line = line.strip()
        if len(line) == 0:
            continue
        (pid, class_name) = line.split()
        yield (int(pid), class_name)


def process_exists(pid):
    """Tests whether a process exists.

    Args:
        pid: PID of the process to test the existence of.
    Returns:
        Whether a process with the specified PID exists.
    """
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


# --------------------------------------------------------------------------------------------------


class ImmutableDict(dict):
    """Dictionary guaranteed immutable.

    All mutations raise an exception.
    Behaves exactly as a dict otherwise.
    """

    def __init__(self, items=None, **kwargs):
        if items is not None:
            super(ImmutableDict, self).__init__(items)
            assert (len(kwargs) == 0), 'items and **kwargs are exclusive'
        else:
            super(ImmutableDict, self).__init__(**kwargs)

    def __setitem__(self, key, value):
        raise Exception('Attempting to map key %r to value %r in ImmutableDict %r'
                        % (key, value, self))

    def __delitem__(self, key):
        raise Exception('Attempting to remove mapping for key %r in ImmutableDict %r' % (key, self))

    def clear(self):
        raise Exception('Attempting to clear ImmutableDict %r' % self)

    def update(self, items=None, **kwargs):
        raise Exception('Attempting to update ImmutableDict %r with items=%r, kwargs=%r'
                        % (self, args, kwargs))

    def pop(self, key, default=None):
        raise Exception('Attempting to pop key %r from ImmutableDict %r' % (key, self))

    def popitem(self):
        raise Exception('Attempting to pop item from ImmutableDict %r' % self)


# --------------------------------------------------------------------------------------------------


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

        def parse(self, argument):
            """Parses the command-line argument.

            Args:
                argument: Command-line argument, as a string.
            """
            self._value = argument

        @property
        def type(self):
            """Returns: the type of this flag."""
            return 'string'

        @property
        def name(self):
            """Returns: the name of this flag."""
            return self._name

        @property
        def value(self):
            """Returns: the parsed value of this flag."""
            return self._value

        @property
        def default(self):
            """Returns: the default value of this flag."""
            return self._default

        @property
        def help(self):
            """Returns: the help string of this flag."""
            return self._help

    class IntegerFlag(StringFlag):
        """Command-line flag whose value is an integer."""

        @property
        def type(self):
            """Returns: the type of this flag."""
            return 'integer'

        def parse(self, argument):
            self._value = int(argument)

    class FloatFlag(StringFlag):
        """Command-line flag whose value is an float."""

        @property
        def type(self):
            """Returns: the type of this flag."""
            return 'float'

        def parse(self, argument):
            self._value = float(argument)

    class BooleanFlag(StringFlag):
        """Command-line flag whose value is a boolean."""

        @property
        def type(self):
            """Returns: the type of this flag."""
            return 'boolean'

        def parse(self, argument):
            if argument is None:
                self._value = True
            else:
                self._value = Truth(argument)

    # ----------------------------------------------------------------------------------------------

    def __init__(self, name, parent=None):
        """Initializes a new flags parser.

        Args:
            name: Name for this collection of flags.
                    Used in PrintUsage().
            parent: Optional parent flags environment.
        """
        self._name = name
        self._parent = parent

        # Map: flag name -> flag definition
        self._defs = {}

        # After parsing, tuple of unparsed arguments:
        self._unparsed = None

    def add(self, flag_def):
        """Registers a flag definition.

        Args:
            flag_def: Flag definition to register.
        """
        assert (flag_def.name not in self), \
                ('Flag %r already defined' % flag_def.name)
        self._defs[flag_def.name] = flag_def

    def add_integer(self, name, **kwargs):
        """Defines a new integer command-line flag.

        Args:
            name: flag name.
        """
        self.add(Flags.IntegerFlag(name, **kwargs))

    def add_string(self, name, **kwargs):
        """Defines a new text command-line flag.

        Args:
            name: flag name.
        """
        self.add(Flags.StringFlag(name, **kwargs))

    def add_boolean(self, name, **kwargs):
        """Defines a new boolean command-line flag.

        Args:
            name: flag name.
        """
        self.add(Flags.BooleanFlag(name, **kwargs))

    def add_float(self, name, **kwargs):
        """Defines a new float command-line flag.

        Args:
            name: flag name.
        """
        self.add(Flags.FloatFlag(name, **kwargs))

    # Deprecated
    AddString = add_string
    AddInteger = add_integer
    AddBoolean = add_boolean
    AddFloat = add_float

    def parse(self, args, config_file=None):
        """Parses the command-line arguments.

        Args:
            args: List of command-line arguments.
            config_file: Location of a file containing a json object with base flag values
                (values in args will take precedence over these values).
        Returns:
            Whether successful.
        """
        unparsed = []

        skip_parse = False

        if config_file is not None:
            with open(config_file) as config_file_handle:
                config_dict = json.load(fp=config_file_handle)

                # Load configuration file values.
                for key, value in config_dict.items():
                    if key not in self._defs:
                        logging.warning('Found unused configuration entry. %s: %r', key, value)
                        continue

                    self._defs[key].parse(value)

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

            self._defs[key].parse(value)

        self._unparsed = tuple(unparsed)
        return True

    def __contains__(self, name):
        if name in self._defs:
            return True
        if self._parent is not None:
            return name in self._parent
        return False

    def __getattr__(self, name):
        assert (self._unparsed is not None), \
                ('Flags have not been parsed yet: cannot access flag %r' % name)
        if name in self._defs:
            return self._defs[name].value
        elif self._parent is not None:
            return getattr(self._parent, name)
        else:
            raise AttributeError(name)

    def get_unparsed(self):
        """Returns: the command-line arguments that were not parsed."""
        assert (self._unparsed is not None), 'Flags have not been parsed yet'
        return self._unparsed

    def list_flags(self):
        """Lists the declared flags.

        Yields:
            Pair: (flag name, flag descriptor).
        """
        yield from self._defs.items()

    def print_usage(self):
        """Prints a help/usage string."""
        indent = 8
        ncolumns = Terminal.columns

        print('-' * 80)
        print('%s:' % self._name)
        for (name, flag) in sorted(self._defs.items()):
            if flag.help is not None:
                flag_help = wrap_text(text=flag.help, ncolumns=ncolumns - indent)
            else:
                flag_help = 'Undocumented'
            print(' --%s: %s = %r\n%s\n' % (
                name,
                flag.type,
                flag.default,
                add_margin(StripMargin(flag_help), ' ' * indent),
            ))

        if self._parent is not None:
            self._parent.print_usage()

    # Deprecated
    GetUnparsed = get_unparsed
    ListFlags = list_flags
    PrintUsage = print_usage


FLAGS = Flags(name='Global flags')


# --------------------------------------------------------------------------------------------------


def make_tuple(name, **kwargs):
    """Creates a read-only named-tuple with the specified key/value pair.

    Args:
        name: Name of the named-tuple.
        **kwargs: Key/value pairs in the named-tuple.
    Returns:
        A read-only named-tuple with the specified name and key/value pairs.
    """
    tuple_class = collections.namedtuple(typename=name, field_names=kwargs.keys())
    return tuple_class(**kwargs)


# Shell exit codes:
EXIT_CODE = make_tuple(
    name='ExitCode',
    SUCCESS=os.EX_OK,
    FAILURE=1,
    USAGE=os.EX_USAGE,
)


# --------------------------------------------------------------------------------------------------


LOG_LEVEL = make_tuple(
    name='LogLevel',
    FATAL=50,
    ERROR=40,
    WARNING=30,
    INFO=20,
    DEBUG=10,
    DEBUG_VERBOSE=5,
    ALL=0,
)


def parse_log_level_flag(level):
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


FLAGS.add_string(
    name='log_level',
    default='DEBUG',
    help=('Master log level (integer or level name).\n'
          'Overrides specific logging levels.'),
)

FLAGS.add_string(
    name='log_console_level',
    default='INFO',
    help='Filter log statements sent to the console (integer or level name).',
)

FLAGS.add_string(
    name='log_file_level',
    default='ALL',
    help='Filter log statements sent to the log file (integer or level name).',
)

FLAGS.add_string(
    name='log_dir',
    default=None,
    help='Directory where to write logs.',
)


# --------------------------------------------------------------------------------------------------


def synchronized(lock=None):
    """Decorator for synchronized functions.

    Similar but not equivalent to Java synchronized methods.
    """
    if lock is None:
        lock = threading.Lock()

    def _wrap(function):
        def _synchronized_wrapper(*args, **kwargs):
            with lock:
                return function(*args, **kwargs)
        return _synchronized_wrapper

    return _wrap


# --------------------------------------------------------------------------------------------------


def memoize():
    """Returns a decorator to memoize the function it is applied to.

    Memoization is incompatible with functions whose parameters are mutable.
    This memoization implementation is fairly incomplete and primitive.
    In particular, positional and keyword parameters are not normalized.
    """
    unknown = object()

    def decorator(function):
        """Wraps a function and returns its memoized version.

        Args:
            function: Function to wrap with memoization.
        Returns:
            The memoized version of the specified function.
        """
        # Map: args tuple -> function(args)
        memoized = dict()

        def memoize_wrapper(*args, **kwargs):
            """Memoization function wrapper."""
            all_args = (args, tuple(sorted(kwargs.items())))
            value = memoized.get(all_args, unknown)
            if value is unknown:
                value = function(*args, **kwargs)
                memoized[all_args] = value
            return value

        return memoize_wrapper

    return decorator


# --------------------------------------------------------------------------------------------------


def get_terminal_size():
    """Reports the terminal size.

    From http://stackoverflow.com/questions/566746

    Returns:
        A pair (nlines, ncolumns).
    """
    def ioctl_gwinsz_fd(fd):
        """Use GWINSZ ioctl on stdin, stdout, stderr.

        Args:
            fd: File descriptor.
        Returns:
            Pair (nlines, ncolumns) if the ioctl succeeded, or None.
        """
        try:
            import fcntl
            import termios
            import struct
            return struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
        except:
            return None

    def ioctl_gwinsz_path(path):
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                return ioctl_gwinsz_fd(fd)
            finally:
                os.close(fd)
        except:
            return None

    return ioctl_gwinsz_fd(0) \
        or ioctl_gwinsz_fd(1) \
        or ioctl_gwinsz_fd(2) \
        or ioctl_gwinsz_path(os.ctermid()) \
        or (os.environ.get('LINES', 25), os.environ.get('COLUMNS', 80))


class _Terminal(object):
    """Map of terminal colors."""

    # Escape sequence to clear the screen:
    clear = '\033[2J'

    # Escape sequence to move the cursor to (y,x):
    move = '\033[%s;%sH'

    def move_to(self, x, y):
        """Makes an escape sequence to move the cursor."""
        return _Terminal.move % (y, x)

    normal = '\033[0m'
    bold = '\033[1m'
    underline = '\033[4m'
    blink = '\033[5m'
    reverse = '\033[7m'

    fg = make_tuple(
        name='ForegroundColor',
        black='\033[01;30m',
        red='\033[01;31m',
        green='\033[01;32m',
        yellow='\033[01;33m',
        blue='\033[01;34m',
        magenta='\033[01;35m',
        cyan='\033[01;36m',
        white='\033[01;37m',
    )

    bg = make_tuple(
        name='BackgroundColor',
        black='\033[01;40m',
        red='\033[01;41m',
        green='\033[01;42m',
        yellow='\033[01;43m',
        blue='\033[01;44m',
        magenta='\033[01;45m',
        cyan='\033[01;46m',
        white='\033[01;47m',
    )

    @property
    def columns(self):
        """Reports the number of columns in the calling shell.

        Returns:
            The number of columns in the terminal.
        """
        return get_terminal_size()[1]

    @property
    def lines(self):
        """Reports the number of lines in the calling shell.

        Returns:
            The number of lines in the terminal.
        """
        return get_terminal_size()[0]


TERMINAL = _Terminal()


# --------------------------------------------------------------------------------------------------


RE_TEMPLATE_FIELD_LINE = re.compile(r'^(\w+):\s*(.*)\s*$')


def parse_template(template):
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
        if line.startswith('#'):
            continue
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


def input_template(template, fields):
    """Queries the user for inputs through a template file.

    Args:
        template: Template with placeholders for the fields.
                Specified as a Python format with named % markers.
        fields: Dictionary with default values for the template.
    Returns:
        Fields dictionary as specified/modified by the user.
    """
    editor = os.environ.get('EDITOR', '/usr/bin/vim')
    with tempfile.NamedTemporaryFile('w+t') as ofile:
        ofile.write(template % fields)
        ofile.flush()
        user_command = '%s %s' % (editor, ofile.name)
        if os.system(user_command) != 0:
            raise Error('Error acquiring user input (command was %r).' % user_command)
        with open(ofile.name, 'r') as ifile:
            filled_template = ifile.read()

    fields = dict(parse_template(filled_template))
    return fields


# --------------------------------------------------------------------------------------------------


HTTP_METHOD = make_tuple(
    name='HttpMethod',
    GET='GET',
    POST='POST',
    PUT='PUT',
    DELETE='DELETE',
    HEAD='HEAD',
    OPTIONS='OPTIONS',
    TRACE='TRACE',
    CONNECT='CONNECT',
)


CONTENT_TYPE = make_tuple(
    name='ContentType',
    JSON='application/json',
    XML='application/xml',
    PATCH='text/x-patch',
    PLAIN='text/plain',
)


# --------------------------------------------------------------------------------------------------


_LOGGING_INITIALIZED = False


def setup_logging(
        level,
        console_level,
        file_level,
):
    """Initializes the logging system.

    Args:
        level: Root logger level.
        console_level: Log level for the console handler.
        file_level: Log level for the file handler.
    """
    global _LOGGING_INITIALIZED
    if _LOGGING_INITIALIZED:
        logging.debug('SetupLogging: logging system already initialized')
        return

    program_name = get_program_name()
    logging.addLevelName(LogLevel.DEBUG_VERBOSE, 'DEBUG_VERBOSE')
    logging.addLevelName(LogLevel.ALL, 'ALL')

    # Initialize the logging system:

    log_formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s %(filename)s:%(lineno)s : %(message)s',
    )
    # Override the log date formatter to include the time zone:
    def format_time(record, datefmt=None):
        """Formats a log statement timestamp."""
        time_tuple = time.localtime(record.created)
        tz_name = time.tzname[time_tuple.tm_isdst]
        return '%(date_time)s-%(millis)03d-%(tz_name)s' % dict(
            date_time=time.strftime('%Y%m%d-%H%M%S', time_tuple),
            millis=record.msecs,
            tz_name=tz_name,
        )
    log_formatter.formatTime = format_time

    logging.root.handlers.clear()
    logging.root.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(console_level)
    logging.root.addHandler(console_handler)


    # Initialize log dir:
    tstamp = timestamp()
    pid = os.getpid()

    if FLAGS.log_dir is None:
        tmp_dir = os.path.join('/tmp', getpass.getuser(), program_name)
        make_dir(tmp_dir)
        FLAGS.log_dir = tempfile.mkdtemp(
            prefix='%s.%d.' % (tstamp, pid),
            dir=tmp_dir)
    logging.info('Using log dir: %s', FLAGS.log_dir)
    make_dir(FLAGS.log_dir)

    log_file = os.path.join(FLAGS.log_dir, '%s.%s.%d.log' % (program_name, tstamp, pid))

    file_handler = logging.FileHandler(filename=log_file)
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(file_level)
    logging.root.addHandler(file_handler)

    _LOGGING_INITIALIZED = True


def run(main):
    """Runs a Python program's Main() function.

    Args:
        main: Main function, with an expected signature like
            exit_code = main(args)
            where args is a list of the unparsed command-line arguments.
    """
    # Parse command-line arguments:
    if not FLAGS.parse(sys.argv[1:]):
        FLAGS.print_usage()
        return os.EX_USAGE

    try:
        log_level = parse_log_level_flag(FLAGS.log_level)
        log_console_level = parse_log_level_flag(FLAGS.log_console_level)
        log_file_level = parse_log_level_flag(FLAGS.log_file_level)
    except Error as err:
        print(err)
        return os.EX_USAGE

    setup_logging(
        level=log_level,
        console_level=log_console_level,
        file_level=log_file_level,
    )

    logging.debug("Process ID: %s", os.getpid())
    logging.debug("Current working directory: %s", os.getcwd())
    logging.debug("Command-line arguments: %r", sys.argv)
    logging.debug("Environment: %r", os.environ)

    # Run program:
    sys.exit(main(FLAGS.get_unparsed()))


# --------------------------------------------------------------------------------------------------


def deprecated(fun):
    def wrapped(*args, **kwargs):
        logging.warn("Deprecated use of function: %r", fun)
        return fun(*args, **kwargs)
    return wrapped


# Deprecated - for compatibility only
Default = DEFAULT
Undefined = UNDEFINED
NowMS = now_ms
NowNS = now_ns
NowDateTime = now_date_time
Timestamp = timestamp
JsonDecode = json_decode
JsonEncode = json_encode
Truth = truth
RandomAlphaNumChar = random_alpha_num_char
RandomAlphaNumWord = random_alpha_num_word
StripPrefix = strip_prefix
StripOptionalPrefix = strip_optional_prefix
StripSuffix = strip_suffix
StripOptionalSuffix = strip_optional_suffix
StripMargin = strip_margin
AddMargin = add_margin
WrapText = wrap_text
CamelCase = camel_case
UnCamelCase = un_camel_case
Truncate = truncate
MakeIdent = make_ident
SetProgramName = set_program_name
GetProgramName = get_program_name
ShellCommandOutput = shell_command_output
MakeDir = make_dir
Remove = remove
Touch = touch
Exit = shutdown
ListJavaProcesses = list_java_processes
ProcessExists = process_exists
MakeTuple = make_tuple
ParseLogLevelFlag = parse_log_level_flag
Synchronized = synchronized
Memoize = memoize
ParseTemplate = parse_template
InputTemplate = input_template
SetupLogging = setup_logging
Run = run
ExitCode = EXIT_CODE
LogLevel = LOG_LEVEL
HttpMethod = HTTP_METHOD
ContentType = CONTENT_TYPE
Terminal = TERMINAL


# --------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    raise Error('%r cannot be used as a standalone script.' % sys.argv[0])

