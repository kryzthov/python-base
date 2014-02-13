#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

"""General purpose workflow of tasks with dependencies.

Once started, a workflow cannot be modified.
In particular, tasks and dependencies cannot be changed.

Usage:
  1. Workflow definition:

    workflow = Workflow()
    task1 = Task(...)
    task2 = Task(...)
    task1.RunsAfter(task2)
    task2.RunsBefore(...)
    ...
    workflow.Build()

  2. Workflow execution:

    workflow.Process(...)
    workflow.Wait()
"""

import abc
import collections
import copy
import datetime
import http.server
import itertools
import logging
import os
import queue
import re
import sys
import tempfile
import threading
import traceback

from base import base
from base import command
from base import record


FLAGS = base.FLAGS
LogLevel = base.LogLevel
Default = base.Default
Undefined = base.Undefined


class Error(Exception):
  """Errors used in this module."""
  pass


class CircularDependencyError(Error):
  """Raised when a circular dependency is detected."""
  pass


# ------------------------------------------------------------------------------
# Task abstract base class:


# Task states:
TaskState = base.MakeTuple('TaskState',
  # Task is being initialized:
  INIT     = 1,

  # Task initialization is complete,
  # task is either runnable or waiting for some upstream dependency:
  PENDING  = 2,

  # Task completed successfully:
  SUCCESS  = 3,

  # Tasks failed:
  FAILURE  = 4,
)


def GetTaskID(task_or_id):
  """Gets the ID of a task, given a parameter that is either a Task or an ID.

  Args:
    task_or_id: Either a Task, or a task ID.
  Returns:
    The task ID.
  """
  if isinstance(task_or_id, Task):
    return task_or_id.task_id
  else:
    return task_or_id


class Task(object, metaclass=abc.ABCMeta):
  """Base class for a task."""

  FAILURE = TaskState.FAILURE
  SUCCESS = TaskState.SUCCESS

  @classmethod
  def TaskName(cls):
    """Returns: the name of this task."""
    if cls.__module__ == '__main__':
      return cls.__name__
    else:
      return '%s.%s' % (cls.__module__, cls.__name__)


  def __init__(
      self,
      workflow,
      task_id,
      runs_after=frozenset(),
      runs_before=frozenset(),
  ):
    """Initializes a new task with the specified ID and dependencies.

    Args:
      workflow: Workflow this task belongs to.
      task_id: Task unique ID. Must be hashable and immutable.
          Most often, a string, a tuple or named tuple.
      runs_after: Tasks (or task IDs) this task must run after.
      runs_before: Tasks (or task IDs) this task must run before.
    """
    self._state = TaskState.INIT
    self._task_id = task_id
    self._workflow = workflow

    self._workflow._AddTask(self)

    # Set of task IDs, frozen after call to Task._Build():
    self._runs_after = set()
    self._runs_before = set()

    # Initialize dependencies from existing workflow state:
    for dep in self._workflow._deps:
      if dep.before == self._task_id:
        self._runs_before.add(dep.after)
      if dep.after == self._task_id:
        self._runs_after.add(dep.before)

    # Initialize dependencies from constructor parameters:
    for dep in runs_after:
      self.RunsAfter(dep)
    for dep in runs_before:
      self.RunsBefore(dep)

    # While workflow runs, lists task IDs this task is waiting for:
    self._pending_deps = None

    # datetime instances set when the task runs:
    self._start_time = None
    self._end_time = None

  @property
  def workflow(self):
    """Returns: the workflow this task belongs to."""
    return self._workflow

  @property
  def task_id(self):
    """Returns: the unique ID for this task."""
    return self._task_id

  @property
  def state(self):
    """Returns: the task state."""
    return self._state

  @property
  def is_runnable(self):
    """Returns: whether this task is runnable."""
    return (self._state == TaskState.PENDING) \
        and (len(self._pending_deps) == 0)

  @property
  def pending_deps(self):
    return frozenset(self._pending_deps)

  _COMPLETION_STATES = frozenset({TaskState.SUCCESS, TaskState.FAILURE})

  @property
  def completed(self):
    """Returns: whether this task has completed."""
    return (self._state in self._COMPLETION_STATES)

  @property
  def runs_after(self):
    """Returns: IDs of the tasks this task depends on, ie. runs before."""
    return self._runs_after

  @property
  def runs_before(self):
    """Returns: IDs of the tasks that depend on, ie. run after this task."""
    return self._runs_before

  def RunsAfter(self, task):
    """Declares a dependency from this task to a given task.

    Args:
      task: Task or task ID to add a dependency upon.
    Returns:
      This task.
    """
    assert (self._state == TaskState.INIT)
    task_id = GetTaskID(task)
    if task_id not in self._runs_after:
      self._runs_after.add(task_id)
      self._workflow._AddDep(Dependency(before=task_id, after=self._task_id))
    return self

  def RunsBefore(self, task):
    """Declares a dependency from a given task to this task.

    Args:
      task: Task or task ID to add a dependency upon.
    Returns:
      This task.
    """
    assert (self._state == TaskState.INIT)
    task_id = GetTaskID(task)
    if task_id not in self._runs_before:
      self._runs_before.add(task_id)
      self._workflow._AddDep(Dependency(before=self._task_id, after=task_id))
    return self

  def _Build(self):
    """Completes the definition of this task.

    Called internally by Workflow.Build().
    """
    assert (self._state == TaskState.INIT)
    self._state = TaskState.PENDING

    self._runs_after = frozenset(self._runs_after)
    self._runs_before = frozenset(self._runs_before)

    self._pending_deps = set(self._runs_after)

  def __str__(self):
    """Returns: a debug representation of this task."""
    return ('Task(id=%s, runs_after=%s, runs_before=%s)'
            % (self.task_id, self._runs_after, self._runs_before))

  def __repr__(self):
    return str(self)

  @abc.abstractmethod
  def Run(self):
    """Subclasses must override this method with the task's logic.

    Returns:
      Task completion status (TaskState.SUCCESS or TaskState.FAILURE).
    """
    raise Error('AbstractMethod')

  def _Run(self):
    """Wraps Run() to update and validate the task's status.

    Uncaught exceptions are fatal.

    Returns:
      Task completion status.
    """
    try:
      self._start_time = datetime.datetime.now()
      try:
        self._state = self.Run()
      finally:
        self._end_time = datetime.datetime.now()

      if not self.completed:
        logging.error(
            '%r returned invalid task completion code: %r',
            type(self).Run, self._state)
        # A task status that is neither SUCCESS nor FAILURE is a programming
        # error: program should not continue in such a scenario.
        base.Exit()
      return self._state
    except:
      logging.error('Unhandled exception from Task.Run().')
      traceback.print_exc()
      # It is arguable whether an uncaught exception should cause the program
      # to stop. We current assume an uncaught exception is a programming error.
      # This could be made configurable via a flag.
      base.Exit()

  def _TaskSuccess(self, task):
    """Processes the success of a task."""
    assert (task.task_id in self._runs_after)
    assert (task.task_id in self._pending_deps)
    self._pending_deps.remove(task.task_id)

  def _TaskFailure(self, task):
    """Processes the failure of a task."""
    assert (task.task_id in self._runs_after)
    assert (task.task_id in self._pending_deps)
    self._pending_deps.remove(task.task_id)
    self._state = TaskState.FAILURE

  def _SetCompletionState(self, state):
    """Forcibly sets the completion state of this task.

    Args:
      New state of the task.
    """
    self._state = state
    assert self.completed

  @property
  def start_time(self):
    """Returns: the start time of the task run. or None if not started yet."""
    return self._start_time

  @property
  def end_time(self):
    """Returns: the end time of the task run, or None if not completed yet."""
    return self._end_time


# ------------------------------------------------------------------------------


class Worker(object):
  """Worker processing tasks from a queue in a separate thread.

  The thread exits when the queue is empty.
  """

  def __init__(self, worker_id, task_queue):
    """Initializes a worker.

    Args:
      worker_id: ID of the worker.
      task_queue: Queue of tasks to pick from.
    """
    self._worker_id = worker_id
    self._task_queue = task_queue
    self._thread = threading.Thread(target=self._Run)
    self._thread.start()

  def Join(self):
    """Waits until the worker exits."""
    self._thread.join()

  def _Run(self):
    """Worker loop."""
    while True:
      task = self._task_queue.Pick()
      if task is None:
        logging.debug('Shutting down worker %s.', self)
        return

      logging.info('Worker %s running task %s', self._worker_id, task.task_id)

      # Task exceptions should not kill the worker:
      try:
        task._Run()
        task.workflow._ReportTaskComplete(task)
      except:
        logging.error('Unhandled exception from Task.Run(): task not completed')
        # TODO: retry task?
        traceback.print_exc()
        base.Exit()


  def __str__(self):
    return 'Worker(%s)' % self._worker_id


# ------------------------------------------------------------------------------


# Representation of a dependency:
#   Dependency(
#       before = ID of task running before,
#       after =  ID of task running after,
#   )
Dependency = collections.namedtuple('Dependency', ('before', 'after'))


class Workflow(object):
  """Represents a graph of tasks with dependencies."""

  def __init__(self, name=None):
    """Initializes a new empty workflow."""
    if name is None:
      name = 'Workflow-%s' % id(self)
    self._name = name

    self._lock = threading.Lock()
    self._done = threading.Event()

    # Map: task ID -> Task
    # Becomes immutable after call to Build()
    self._tasks = dict()

    # Dependencies, as a set of Dependency objects:
    self._deps = set()

    # No new task may be added once the worker pool starts:
    self._started = False

    # A task belongs to exactly one of the following buckets:
    #  - running: task is currently running;
    #  - runnable: task may run, but no worker is available;
    #  - pending: task is blocked until all its dependencies are satisfied;
    #  - success or failure: task has completed.
    self._pending = set()
    self._runnable = set()
    self._running = set()
    self._success = set()
    self._failure = set()

    # Queue of runnable tasks to pick from:
    # This queue is updated to stay consistent with self._runnable:
    self._runnable_queue = queue.Queue()

  @property
  def tasks(self):
    """Returns: the map: task ID -> Task."""
    return self._tasks

  @property
  def deps(self):
    """Returns: the set of dependencies, as Dependency directed edges."""
    return self._deps

  @property
  def started(self):
    """Returns: whether the workflow is started."""
    return self._started

  @property
  def failed_tasks(self):
    """Set of tasks that failed, directly or transitively."""
    with self._lock:
      return frozenset(self._failure)

  @property
  def successful_tasks(self):
    """Set of tasks that completed successfully."""
    with self._lock:
      return frozenset(self._success)

  def GetTask(self, task_id):
    """Gets a task by ID.

    Args:
      task_id: ID of the task.
    Returns:
      The task with the specified ID.
    """
    return self._tasks[task_id]

  def _AddTask(self, task):
    """Adds a new task to this workflow.

    Used by Task.__init__() to register new task objects.

    Args:
      task: New Task object to add.
    """
    assert not self._started
    assert (task.task_id not in self._tasks), \
        ('Duplicate task ID %r' % task.task_id)
    self._tasks[task.task_id] = task

  def AddDep(self, before, after):
    """Adds a dependency between two tasks.

    Args:
      before: Task or ID of the task that must run before the other.
      after: Task or ID of the task that must run after the other.
    """
    before_id = GetTaskID(before)
    after_id = GetTaskID(after)
    dep = Dependency(before=before_id, after=after_id)
    self._AddDep(dep)

  def _AddDep(self, dep):
    """Registers a Dependency.

    Args:
      dep: Dependency tuple.
    """
    if dep not in self._deps:
      self._deps.add(dep)
      before = self._tasks.get(dep.before)
      if before is not None:
        before._runs_before.add(dep.after)
      after = self._tasks.get(dep.after)
      if after is not None:
        after._runs_after.add(dep.before)

  def Build(self):
    """Completes the worflow definition phase."""
    self._tasks = base.ImmutableDict(self._tasks)
    self._deps = frozenset(self._deps)

    # Freeze descriptors:
    for task in self._tasks.values():
      task._Build()

    # Minimal validation:
    for task in self._tasks.values():
      for dep_id in task.runs_after:
        assert (dep_id in self._tasks), \
            ('Task %r has dependency on unknown task %r'
             % (task.task_id, dep_id))

    self._CheckCircularDeps()

  def _CheckCircularDeps(self):
    """Checks for circular dependencies."""
    # Set of task IDs that are completed:
    completed = set()

    # Set of tasks that are left:
    pending = set(self._tasks.values())

    while (len(pending) > 0):
      runnable = set()
      for task in pending:
        if completed.issuperset(task.runs_after):
          runnable.add(task)

      if len(runnable) == 0:
        raise CircularDependencyError()

      pending.difference_update(runnable)
      completed.update(map(lambda task: task.task_id, runnable))

  def Process(
      self,
      nworkers=1,
      monitor_thread=True,
      sync=True,
  ):
    """Processes the tasks from the pool.

    Args:
      nworkers: Number of workers to process tasks.
      monitor_thread: Whether to start a monitor thread.
      sync: Whether to wait for the workflow to complete.
    Returns:
      When synchronous, whether the workflow is successful.
      None otherwise.
    """
    assert not self._started
    self._started = True

    # Initializes runnable/pending task sets:
    for task in self._tasks.values():
      if task.is_runnable:
        self._runnable_queue.put(task)
        self._runnable.add(task)
      else:
        self._pending.add(task)

    # Log initial state of tasks:
    self._Dump()

    # Short-circuit if workflow is empty:
    self._NotifyIfDone()
    if ((len(self._runnable) == 0) and (len(self._pending) == 0)):
      return

    # Starts workers:
    self._workers = list()
    for iworker in range(nworkers):
      worker_id = '%s-#%d' % (self._name, iworker)
      self._workers.append(Worker(worker_id=worker_id, task_queue=self))

    if monitor_thread:
      self._monitor = threading.Thread(target=self._Monitor)
      self._monitor.start()
    else:
      self._monitor = None

    if sync:
      return self.Wait()
    else:
      return None

  def Wait(self):
    """Waits for all the tasks to be processed.

    Returns:
      Whether the workflow is successful.
    """
    self._done.wait()

    # Notify all workers to exit:
    for _ in self._workers:
      self._runnable_queue.put(None)

    for worker in self._workers:
      worker.Join()

    # Wait for monitor thread to exit:
    if self._monitor is not None:
      self._monitor.join()

    return (len(self.failed_tasks) == 0)

  def _Monitor(self):
    """Monitoring thread to dump the state of the worker pool periodically."""
    while not self._done.wait(timeout=5.0):
      with self._lock:
        logging.debug(
            'Running: %s',
            ','.join(map(lambda task: task.task_id, self._running)))
    logging.debug('Monitor thread exiting')

  def Pick(self):
    """Waits for and picks a runnable task.

    Returns:
      A runnable task if any, or None.
    """
    task = self._runnable_queue.get()
    if task is None:
      # Signal the worker should exit
      return None

    with self._lock:
      self._runnable.remove(task)
      self._running.add(task)
      return task

  def _ReportTaskComplete(self, task):
    if task.state == TaskState.SUCCESS:
      self._TaskSuccess(task)
    elif task.state == TaskState.FAILURE:
      self._TaskFailure(task)
    else:
      raise Error('Invalid task completion status: %r' % task.state)

  def _TaskSuccess(self, task):
    """Processes the success of a task.

    Args:
      task: ID of the task that completed successfully.
    """
    logging.debug('Task %r completed with success.', task.task_id)
    with self._lock:
      self._success.add(task)
      self._running.remove(task)

      # Identify tasks that were pending and now become runnable:
      new_runnable = set()
      for pending_id in task.runs_before:
        pending = self._tasks[pending_id]
        pending._TaskSuccess(task)
        if pending.is_runnable:
          new_runnable.add(pending)

      # Update pending and runnable sets accordingly:
      self._pending.difference_update(new_runnable)
      self._runnable.update(new_runnable)
      for runnable_task in new_runnable:
        self._runnable_queue.put(runnable_task)

      self._Dump()

      self._NotifyIfDone()

  def _TaskFailure(self, task):
    """Processes the failure of a task.

    Args:
      task: ID of the task that completed as a failure.
    """
    logging.debug('Task %r completed with failure.', task.task_id)

    def _FailRec(task, cause):
      """Recursively fails transitive dependencies.

      Args:
        task: Transitive dependency that fails.
        cause: Task that causes the dependency to fail.
      """
      task._TaskFailure(cause)
      self._pending.discard(task)
      self._failure.add(task)
      for task_id in task.runs_before:
        _FailRec(task=self._tasks[task_id], cause=task)

    with self._lock:
      self._running.remove(task)
      self._failure.add(task)
      for task_id in task.runs_before:
        _FailRec(task=self._tasks[task_id], cause=task)

      self._Dump()

      self._NotifyIfDone()

  def _NotifyIfDone(self):
    """Tests whether there is more work to do.

    Assumes external synchronization.
    """
    if ((len(self._pending) > 0)
        and ((len(self._running) + len(self._runnable)) == 0)):
      raise CircularDependencyError()

    if len(self._pending) > 0: return
    if len(self._runnable) > 0: return
    if len(self._running) > 0: return
    self._done.set()

  # Template to dump this workflow as a Graphiv/Dot definition:
  _DOT_TEMPLATE = base.StripMargin("""\
  |digraph Workflow {
  |%(nodes)s
  |%(deps)s
  |}""")

  def DumpAsDot(self):
    """Dumps this workflow as a Graphviz/Dot definition.

    Returns:
      A Graphviz/Dot definition for this workflow.
    """
    def MakeNode(task):
      return ('  %s;' % base.MakeIdent(task.task_id))

    def MakeDep(dep):
      return ('  %s -> %s;' %
              (base.MakeIdent(dep.after), base.MakeIdent(dep.before)))

    nodes = sorted(map(MakeNode, self._tasks.values()))
    deps = sorted(map(MakeDep, self._deps))
    return self._DOT_TEMPLATE % dict(
      nodes='\n'.join(nodes),
      deps='\n'.join(deps),
    )

  def DumpRunStateAsDot(self):
    """Dumps this workflow as a Graphviz/Dot definition.

    Returns:
      A Graphviz/Dot definition for this workflow.
    """
    def MakeNode(task):
      task_id = task.task_id
      if task.state == TaskState.FAILURE:
        color = 'red'
      elif task.state == TaskState.SUCCESS:
        color = 'blue'
      elif task in self._running:
        color = 'green'
      else:
        color = 'black'
      return ('  %s [color="%s"];' % (base.MakeIdent(task_id), color))

    def MakeDep(dep):
      return ('  %s -> %s;' %
              (base.MakeIdent(dep.after), base.MakeIdent(dep.before)))

    nodes = sorted(map(MakeNode, self._tasks.values()))
    deps = sorted(map(MakeDep, self._deps))
    return self._DOT_TEMPLATE % dict(
      nodes='\n'.join(nodes),
      deps='\n'.join(deps),
    )

  def DumpStateAsTable(self):
    """Dumps the running state of this workflow as an HTML table.

    Returns:
      The running state of this workflow as an HTML table.
    """
    with self._lock:
      successes = frozenset(self._success)
      failures = frozenset(self._failure)
      pending = frozenset(self._pending)
      running = frozenset(self._running)
      runnable = frozenset(self._runnable)

    def FormatTask(task):
      if task.start_time is None:
        return task.task_id
      elif task.end_time is None:
        return ('%s (start time: %s - elapsed: %s)'
                % (task.task_id,
                   base.Timestamp(task.start_time.timestamp()),
                   datetime.datetime.now() - task.start_time))
      else:
        return ('%s (start time: %s - end time: %s - duration: %s)'
                % (task.task_id,
                   base.Timestamp(task.start_time.timestamp()),
                   base.Timestamp(task.end_time.timestamp()),
                   task.end_time - task.start_time))

    successes = frozenset(map(FormatTask, successes))
    failures = frozenset(map(FormatTask, failures))
    pending = frozenset(map(FormatTask, pending))
    running = frozenset(map(FormatTask, running))
    runnable = frozenset(map(FormatTask, runnable))

    return base.StripMargin("""\
    |Running: %(nrunning)s
    |Runnable: %(nrunnable)s
    |Pending: %(npending)s
    |Successful: %(nsuccesses)s
    |Failed: %(nfailures)s
    |%(ruler)s
    |Running tasks:
    |%(running)s
    |%(ruler)s
    |Runnable tasks:
    |%(runnable)s
    |%(ruler)s
    |Pending tasks:
    |%(pending)s
    |%(ruler)s
    |Successful tasks:
    |%(successes)s
    |%(ruler)s
    |Failed tasks:
    |%(failures)s
    """) % dict(
        ruler = '-' * 80,
        nrunning = len(running),
        nrunnable = len(runnable),
        npending = len(pending),
        nsuccesses = len(successes),
        nfailures = len(failures),
        running = '\n'.join(map(lambda s: ' - %s' % s, sorted(running))),
        runnable = '\n'.join(map(lambda s: ' - %s' % s, sorted(runnable))),
        pending = '\n'.join(map(lambda s: ' - %s' % s, sorted(pending))),
        successes = '\n'.join(map(lambda s: ' - %s' % s, sorted(successes))),
        failures = '\n'.join(map(lambda s: ' - %s' % s, sorted(failures))),
    )

  def _Dump(self):
    if (logging.getLogger().level > LogLevel.DEBUG_VERBOSE): return
    logging.debug(
        'Runnable:%s',
        ''.join(map(lambda task: '\n\t%s' % task, self._runnable)))
    logging.debug(
        'Pending:%s',
        ''.join(map(lambda task: '\n\t%s' % task, self._pending)))
    logging.debug(
        'Running:%s',
        ''.join(map(lambda task: '\n\t%s' % task, self._running)))

  def DumpAsSVG(self):
    dot_source = self.DumpRunStateAsDot()
    with tempfile.NamedTemporaryFile(suffix='.dot') as dot_file:
      with tempfile.NamedTemporaryFile(suffix='.svg') as svg_file:
        dot_file.write(dot_source.encode())
        dot_file.flush()
        cmd = command.Command(
            args=['dot', '-Tsvg', '-o%s' % svg_file.name, dot_file.name],
            exit_code=0,
        )
        return svg_file.read().decode()

  def Prune(self, tasks):
    """Prunes the workflow according to a sub-set of required tasks.

    Args:
      tasks: Collection of tasks to keep.
          Tasks that are not in this set or not required transitively
          through upstream dependencies of this set are discarded.
    """
    assert not self._started

    # Exhaustive list of tasks to keep:
    tasks = GetUpstreamTasks(flow=self, tasks=tasks)
    keep_ids = frozenset(map(lambda task: task.task_id, tasks))

    remove_ids = set(self._tasks.keys())
    remove_ids.difference_update(keep_ids)

    for task_id in remove_ids:
      del self._tasks[task_id]

    # Filter dependencies:
    remove_deps = tuple(filter(
        lambda dep: (dep.before in remove_ids) or (dep.after in remove_ids),
        self._deps))
    self._deps.difference_update(remove_deps)


# ------------------------------------------------------------------------------


class LocalFSPersistentTask(Task):
  """A task that produces a file on the local file system.

  The task is not run if the file artifact already exists.
  """

  def __init__(self, output_file_path, **kwargs):
    super(LocalFSPersistentTask, self).__init__(**kwargs)
    self._output_file_path = output_file_path

  @property
  def output_file_path(self):
    return self._output_file_path

  def _Run(self):
    if os.path.exists(self._output_file_path):
      self._SetCompletionState(TaskState.SUCCESS)
      return TaskState.SUCCESS

    status = super(LocalFSPersistentTask, self)._Run()
    assert self.completed
    if status == TaskState.SUCCESS:
      base.Touch(self._output_file_path)
    return status


# ------------------------------------------------------------------------------


class IOTask(Task):
  """Base class for tasks with inputs and outputs."""

  def __init__(
      self,
      write_output_trace=True,
      ignore_saved_output_trace=False,
      **kwargs
  ):
    """Initializes a new IOTask instance.

    Notes:
    By default, trace files are written in the current working directory.
    Sub-classes may want to override _GetTraceFilePath() to customize this.

    Args:
      write_output_trace: Whether to write an output trace file.
          True by default.
      ignore_saved_output_trace: When set, ignore saved output trace files.
          This causes the task to always run, even when a successful previous
          run exists.
      **kwargs: Other arguments proxied to Task.__init__().
    """
    super(IOTask, self).__init__(**kwargs)

    self._write_output_trace = write_output_trace
    self._ignore_saved_output_trace = ignore_saved_output_trace

    self._input = Undefined
    self._output = Undefined

    # Map: input name -> task ID whose output will be passed as input
    self._input_map = dict()

  @property
  def input(self):
    """Returns: this task's input.

    Undefined until the task run begins.
    """
    return self._input

  @property
  def output(self):
    """Returns: this task's output.

    Undefined until after successful task run completion.
    """
    return self._output

  def BindInputToTaskOutput(self, input_name, task):
    """Binds an input of this task to the output of another task.

    Implies that this task runs after the given dependency.

    Args:
      input_name: Name of the input to bind.
      task: Task or ID of the task to bind the output of.
    """
    assert (input_name != 'output')  # Reserved for output
    assert (input_name not in self._input_map)
    task = GetTaskID(task)
    self._input_map[input_name] = task
    self.RunsAfter(task)

  def GetTaskRunID(self):
    """Uniquely identifies a task run based on the task run-time inputs.

    Task run IDs are used to create trace files for task runs.

    By default, task is unique based on its sole ID,
    ie. run-time inputs (self.input.*) are ignored.
    """
    return self.task_id

  @abc.abstractmethod
  def RunWithIO(self, output, **inputs):
    """Placeholder for users to implement the task's logic.

    Args:
      output: Output record for the task to populate.
          On successful completion of the run, a trace file is written
          with this output.
      **inputs: The requested inputs, bound to the dependencies outputs.
    Returns:
      A task run must return either TaskState.SUCCESS or TaskState.FAILURE.
    Raises:
      Tasks must catch exceptions and convert them explicitly into failures.
      Uncaught exceptions will cause the entire workflow to stop.
    """
    raise Exception('Abstract method')

  def Run(self):
    """Wires tasks outputs and inputs.

    Sub-classes should NOT override this method, but should instead implement
    RunWithIO(output, **inputs).

    Returns:
      The task run completion state.
    """
    # Load task inputs:
    self._input = record.Record()
    input_map = dict()
    for input_name, dep_id in self._input_map.items():
      dep = self.workflow.tasks[dep_id]
      self._input[input_name] = dep.output
      input_map[input_name] = dep.output

    # Run task, if necessary:
    task_run_id = self.GetTaskRunID()
    logging.info('Processing task run ID: %r', task_run_id)

    output = self._ReadTaskRunTrace(task_run_id)
    if output is None:
      output = record.Record()
      task_state = self.RunWithIO(output=output, **input_map)

      # Store task output:
      if task_state == TaskState.SUCCESS:
        self._WriteTaskRunTrace(task_run_id, output)
    else:
      logging.info('Trace found for task run ID: %r', task_run_id)
      task_state = TaskState.SUCCESS

    self._output = output

    return task_state

  def _ReadTaskRunTrace(self, task_run_id):
    """Looks for an existing trace for a given task run.

    Args:
      task_run_id: ID of the task run to search for.
    Returns:
      The task output record persisted for the specified task run,
      or None if no trace is found for the specified task run.
    """
    if self._ignore_saved_output_trace:
      return None

    trace_file_path = self._GetTraceFilePath(task_run_id)
    if os.path.exists(trace_file_path):
      return record.LoadFromFile(trace_file_path)
    else:
      return None

  def _WriteTaskRunTrace(self, task_run_id, output):
    """Persists a successful task run.

    Args:
      task_run_id: ID of the task run.
      output: Task output record.
    """
    if self._write_output_trace:
      trace_file_path = self._GetTraceFilePath(task_run_id)
      logging.debug(
          'Writing trace for task run ID: %r in path %r',
          task_run_id, trace_file_path)
      output.WriteToFile(file_path=trace_file_path)

  def _GetTraceFilePath(self, task_run_id):
    """Returns: path of a trace file for the given task run ID.

    Args:
      task_run_id: ID of the task run.
    Returns:
      Path of a trace file for the given task run ID.
    """
    return task_run_id


# ------------------------------------------------------------------------------


def _MakeWorkflowMonitoringHandlerClass(monitor):
  class HTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
      flow = monitor.workflow
      if flow is None:
        self.send_response(404)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write('No workflow assigned'.encode())
      elif monitor.mode == 'svg':
        self.send_response(200)
        self.send_header('Content-type', 'image/svg+xml')
        self.end_headers()
        self.wfile.write(flow.DumpAsSVG().encode())
      elif monitor.mode == 'dot':
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(flow.DumpRunStateAsDot().encode())
      elif monitor.mode == 'table':
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(flow.DumpStateAsTable().encode())
      else:
        self.send_response(404)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(('Invalid rendering mode: %r' % monitor.mode).encode())

      self.wfile.flush()

  return HTTPRequestHandler


class WorkflowHTTPMonitor(base.MultiThreadedHTTPServer):
  """Simple HTTP server to monitor a workflow."""

  def __init__(
      self,
      interface='0.0.0.0',
      port=8000,
      workflow=None,
      mode='svg',
  ):
    """Creates a new HTTP endpoint to monitor a workflow.

    Args:
      interface: TCP interface to listen on.
      port: TCP port to listen on.
      workflow: Optional workflow to monitor.
          Can be set or updated later with SetWorkflow().
      mode: Rendering mode (either 'dot' or 'svg').
    """
    super(WorkflowHTTPMonitor, self).__init__(
        server_address=(interface, port),
        RequestHandlerClass=_MakeWorkflowMonitoringHandlerClass(self),
    )
    self._interface = interface
    self._thread = threading.Thread(target=self._ServeThread)
    self._workflow = workflow
    assert (mode in ('svg', 'dot', 'table'))
    self._mode = mode

  @property
  def workflow(self):
    return self._workflow

  @property
  def mode(self):
    return self._mode

  def SetWorkflow(self, workflow):
    self._workflow = workflow

  def Start(self):
    self._thread.start()
    logging.info(
        'Workflow monitor started on http://%s:%s',
        self._interface, self.server_port)

  def Stop(self):
    self.shutdown()
    self._thread.join()
    self.server_close()

  def _ServeThread(self):
    self.serve_forever()


# ------------------------------------------------------------------------------


def DiffWorkflow(flow1, flow2):
  """Visualize the differences between two workflows.

  Requires graphviz's frontend "xdot" program to be installed.

  Args:
    flow1, flow2: visualize the differences between these workflows.
  """
  nodes = frozenset.union(
      frozenset(flow1.tasks.keys()),
      frozenset(flow2.tasks.keys()))
  deps = frozenset.union(flow1.deps, flow2.deps)

  def MakeNode(task_id):
    if task_id not in flow1.tasks:
      color = 'blue'
    elif task_id not in flow2.tasks:
      color = 'red'
    else:
      color = 'black'
    return '  %s [color="%s"];' % (base.MakeIdent(task_id), color)

  def MakeDep(dep):
    if dep not in flow1.deps:
      color = 'blue'
    elif dep not in flow2.deps:
      color = 'red'
    else:
      color = 'black'
    return '  %s -> %s [color="%s"];' \
        % (base.MakeIdent(dep.after), base.MakeIdent(dep.before), color)

  _DOT_TEMPLATE = base.StripMargin("""\
  |digraph Workflow {
  |%(nodes)s
  |%(deps)s
  |}""")


  nodes = sorted(map(MakeNode, nodes))
  deps = sorted(map(MakeDep, deps))
  dot_source = _DOT_TEMPLATE % dict(
    nodes='\n'.join(nodes),
    deps='\n'.join(deps),
  )

  with tempfile.NamedTemporaryFile(prefix='wfdiff.', suffix='.dot') as f:
    f.write(dot_source.encode())
    f.flush()
    os.system('xdot %s' % f.name)


# ------------------------------------------------------------------------------


def GetUpstreamTasks(flow, tasks):
  """Computes the tasks needed by a collection of tasks.

  Args:
    flow: Workflow to process.
    tasks: Collection of tasks to list the upstream dependencies.
  Returns:
    The transitive dependencies according to the runs_after relationship.
  """
  tasks = set(tasks)
  task_ids = set(map(lambda t: t.task_id, tasks))

  while True:
    upstream_ids = set(itertools.chain(*map(lambda t: t.runs_after, tasks)))
    upstream_ids.difference_update(task_ids)
    if len(upstream_ids) == 0:
      break
    task_ids.update(upstream_ids)
    tasks.update(map(lambda task_id: flow.GetTask(task_id), upstream_ids))

  return tasks


def GetDownstreamTasks(flow, tasks):
  """Computes the tasks that depend on a collection of tasks.

  Args:
    flow: Workflow to process.
    tasks: Collection of tasks to list the downstream dependencies.
  Returns:
    The transitive dependencies according to the runs_before relationship.
  """
  tasks = set(tasks)
  task_ids = set(map(lambda t: t.task_id, tasks))

  while True:
    downstream_ids = set(itertools.chain(*map(lambda t: t.runs_before, tasks)))
    downstream_ids.difference_update(task_ids)
    if len(downstream_ids) == 0:
      break
    task_ids.update(downstream_ids)
    tasks.update(map(lambda task_id: flow.GetTask(task_id), downstream_ids))

  return tasks


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  raise Exception('Not a standalone module')
