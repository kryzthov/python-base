#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Wrapper for shell commands."""

import logging
import os
import signal
import subprocess
import sys
import threading

from base import base

FLAGS = base.FLAGS
LOG_LEVEL = base.LOG_LEVEL


RULER = "-" * 80


class Error(Exception):
    """Errors used in this module."""
    pass


class CommandError(Error):
    """Error while running a shell command."""
    pass


# --------------------------------------------------------------------------------------------------


class CommandID(object):
    """ID generator for shell-commands."""
    _LOCK = threading.Lock()
    _COUNTER = 0

    @classmethod
    def get_new_id(cls):
        """Generates a new unique command ID.

        Returns:
            A new unique ID to identify a shell command.
        """
        cls._LOCK.acquire()
        try:
            command_id = cls._COUNTER
            cls._COUNTER += 1
            return command_id
        finally:
            cls._LOCK.release()


# --------------------------------------------------------------------------------------------------


class Command(object):
    """Runs a shell command."""

    def __init__(
        self,
        *arglist,
        args=None,
        exit_code=None,
        work_dir=None,
        env=None,
        log_dir=None,
        start=True,
        wait_for=True
    ):
        """Runs a shell command.

        The command runs in the specified working directory and with the given
        environment.
        The command takes no standard input.
        Its output and error streams are captured in files, and are exposed as
        properties once the process has completed.

        Args:
            *arglist: Command-line, as an array of command-line arguments.
                First argument is the path to the executable.
            args: Keyword argument, alternative to *arglist.
            exit_code: Optional command exit code to require, or None.
            work_dir: Working directory. None means current workding directory.
            env: Optional environment variables for the subprocess, or None.
            log_dir: Optional directory where to write files capturing the command output streams.
                Defaults to the log directory (FLAGS.log_dir).
            start: Whether to start running the command right away.
            wait_for: Whether to wait for the command to complete.
        Raises:
            CommandError: if the sub-process exit code does not match exit_code.
        """
        self._command_id = CommandID.get_new_id()
        assert (args is None) ^ (len(arglist) == 0)
        if args is None:
            self._args = tuple(arglist)
        else:
            self._args = tuple(args)
        self._required_exit_code = exit_code
        self._work_dir = work_dir or os.getcwd()
        self._env = env or os.environ
        log_dir = log_dir or FLAGS.log_dir

        name = os.path.basename(self._args[0])
        timestamp = base.timestamp()

        self._input_path = "/dev/null"
        self._output_path = os.path.join(log_dir, "%s.%s.%d.out" % (name, timestamp, os.getpid()))
        self._error_path = os.path.join(log_dir, "%s.%s.%d.err" % (name, timestamp, os.getpid()))

        self._process = None
        self._output_bytes = None
        self._error_bytes = None

        if start:
            self.start(wait_for=wait_for)

    def start(self, wait_for=True):
        """Starts the process running this command.

        Args:
            wait_for: Whether to wait for this command to complete.
        """
        assert (self._process is None), "Command is already started."

        if logging.getLogger().isEnabledFor(LOG_LEVEL.DEBUG_VERBOSE):
            logging.log(
                LOG_LEVEL.DEBUG_VERBOSE,
                ("Running command #%d in %r:\n"
                 "%s\n"
                 "With environment:\n"
                 "%s"),
                self._command_id, self._work_dir,
                " \\\n\t".join(map(repr, self._args)),
                "\n".join(map(lambda kv: "\t%r: %r" % kv, sorted(self._env.items()))),
            )
        else:
            logging.debug("Running command #%d in %r:\n%s",
                          self._command_id, self._work_dir, " \\\n\t".join(map(repr, self._args)))

        with open(self._input_path, mode="rb") as input_file, \
             open(self._output_path, mode="wb") as output_file, \
             open(self._error_path, mode="wb") as error_file:
            self._process = subprocess.Popen(
                args=self._args,
                stdin=input_file,
                stdout=output_file,
                stderr=error_file,
                cwd=self._work_dir,
                env=self._env,
            )
        if wait_for:
            self.wait_for()

    def wait_for(self, timeout=None):
        """Waits for this command to complete.

        Args:
            timeout: Maximum amount of time to wait for the process, in seconds.
        Raises:
            TimeoutExpired: if the timeout is reached.
        """
        assert (self._process is not None), "Command has not been started."
        assert (self._output_bytes is None), "Command has already completed."

        self._process.wait(timeout=timeout)

        with open(self._output_path, mode="rb") as file:
            self._output_bytes = file.read()
        with open(self._error_path, mode="rb") as file:
            self._error_bytes = file.read()

        if logging.getLogger().isEnabledFor(LOG_LEVEL.DEBUG_VERBOSE):
            logging.log(
                LOG_LEVEL.DEBUG_VERBOSE,
                ("Command #%d exited with code: %d\n"
                 "%s\n"
                 "In directory %r\n"
                 "With environment:\n%s\n"
                 "%s\n"  # ruler
                 "Output:\n%s\n"
                 "%s\n"  # ruler
                 "Error:\n%s\n"
                 "%s\n"),  # ruler
                self._command_id,
                self.exit_code,
                " \\\n\t".join(map(repr, self._args)),
                self._work_dir,
                "\n".join(map(lambda kv: "\t%r: %r" % kv, sorted(self._env.items()))),
                RULER,
                self.output_text,
                RULER,
                self.error_text,
                RULER,
            )
        else:
            logging.debug(
                ("Command #%d exited with code: %d\n"
                 " - output: %r\n"
                 " - error:  %r"),
                self._command_id, self.exit_code,
                self.output_text,
                self.error_text)

        os.remove(self._output_path)
        os.remove(self._error_path)

        if ((self._required_exit_code is not None)
                and (self.exit_code != self._required_exit_code)):
            raise CommandError(
                "Exit code %d does not match required code %d "
                "for command in directory %s\n%s\nOutput:\n%s\nError:\n%s\n" % (
                    self.exit_code,
                    self._required_exit_code,
                    self._work_dir,
                    " \\\n\t".join(self._args),
                    self.output_text,
                    self.error_text)
            )

    def kill(self, sig=signal.SIGTERM):
        """Sends a signal to the process for this command.

        Args:
            sig: Signal to send.
        """
        os.kill(self._process.pid, sig)

    # Deprecated
    Start = start
    WaitFor = wait_for
    Kill = kill

    @property
    def output_bytes(self):
        """Returns: the command output stream as an array of bytes."""
        assert (self._output_bytes is not None), "Command has not terminated."
        return self._output_bytes

    @property
    def output_text(self):
        """Returns: the command output stream as a text string."""
        return self.output_bytes.decode()

    @property
    def output_lines(self):
        """Returns: the command output stream as an array of text lines."""
        return self.output_text.split("\n")

    @property
    def error_bytes(self):
        """Returns: the command error stream as an array of bytes."""
        assert (self._error_bytes is not None), "Command has not terminated."
        return self._error_bytes

    @property
    def error_text(self):
        """Returns: the command error stream as a text string."""
        return self.error_bytes.decode()

    @property
    def error_lines(self):
        """Returns: the command error stream as an array of text lines."""
        return self.error_text.split("\n")

    @property
    def exit_code(self):
        """Returns: the command exit code."""
        assert (self._process is not None), "Command has not been started."
        return self._process.returncode

    @property
    def pid(self):
        """Returns: the process ID running this command."""
        assert (self._process is not None), "Command has not been started."
        return self._process.pid


if __name__ == "__main__":
    raise Error("%r cannot be used as a standalone script." % sys.argv[0])
