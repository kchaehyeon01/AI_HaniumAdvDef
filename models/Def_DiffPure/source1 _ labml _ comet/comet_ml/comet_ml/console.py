# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2015-2021 Comet ML INC
#  This file can not be copied and/or distributed without the express
#  permission of Comet ML Inc.
# *******************************************************
#
"""
Author: Gideon Mendels

This module contains the components for console interaction like the std
wrapping

"""
import logging
import multiprocessing
import multiprocessing.util  # Force registering the atexit callback early
import os
import os.path
import select
import signal
import sys
import threading

from six.moves import queue

from .logging_messages import NATIVE_STD_WRAPPER_NOT_AVAILABLE, UNKOWN_STD_WRAPPER_SPEC
from .messages import StandardOutputMessage

# -------------------------------------------------
# These are triggers for keras and tensorflow.keras
# (respectively) to use their nice progress meters:
try:
    import ipykernel  # noqa
except ImportError:
    pass
try:
    import posix  # noqa
except ImportError:
    pass
# -------------------------------------------------

LOGGER = logging.getLogger(__name__)


def stream_handler_set_stream(stream_handler_instance, stream):
    """
    Backport of StreamHandler.setStream() for Python 2
    """
    if stream is stream_handler_instance.stream:
        result = None
    else:
        result = stream_handler_instance.stream
        stream_handler_instance.acquire()
        try:
            stream_handler_instance.flush()
            stream_handler_instance.stream = stream
        finally:
            stream_handler_instance.release()
    return result


class FakeStd(object):
    """A fake Std file-like that sends every line to a handler."""

    def __init__(self, handler, original):
        self.__wrapped__ = original
        self.handler = handler

    def write(self, line):
        """
        Overrides the default IO write(). Writes to console + queue.
        :param line: String printed to std, probably with print()
        """
        self.__wrapped__.write(line)
        try:
            if line:
                self.handler(line)
        except Exception:
            # Silent the exception to avoid infinite recursion
            # LOGGER.debug("Error in write", exc_info=True)
            pass

    def writelines(self, sequence_of_strings):
        """
        Overrides the default IO writelines() for Python 2. Writes to console +
        queue.
        :param sequence_of_strings: String sequence to write.
        """
        self.__wrapped__.writelines(sequence_of_strings)
        try:
            if sequence_of_strings:
                for string in sequence_of_strings:
                    self.handler(string)
        except Exception:
            # Silent the exception to avoid infinite recursion
            # LOGGER.debug("Error in writelines", exc_info=True)
            pass

    def isatty(self):
        return False

    def close(self):
        self.flush()

    def __getattribute__(self, attr):
        # Some std wrappers don't expose writelines, so we need to do extra work to detect such cases and not expose it either
        if attr == "writelines":
            self_wrapped = object.__getattribute__(self, "__wrapped__")
            try:
                _ = getattr(self_wrapped, "writelines")
                # Wrapped defines it, returns overridden method
                return object.__getattribute__(self, "writelines")
            except AttributeError:
                raise

        return object.__getattribute__(self, attr)

    def __getattr__(self, attr):
        # We cannot use wrapt in Python2 around sys.stdout/sys.stderr because
        # they are files and we cannot set custom attributes to files in Python
        # 2
        return getattr(self.__wrapped__, attr)


class BaseStdWrapper(object):
    def __init__(
        self, stdout=False, stdout_handler=None, stderr=False, stderr_handler=None
    ):
        self.stdout = stdout
        self.stdout_handler = stdout_handler
        self.stderr = stderr
        self.stderr_handler = stderr_handler

        self._stdout = None
        self._stderr = None

        self._old_stdout = None
        self._old_stderr = None

        self.hooked_handlers = []

    def __enter__(self):
        if self.stdout and self.stdout_handler:
            self._stdout = FakeStd(self.stdout_handler, sys.stdout)
            self._old_stdout = sys.stdout
            sys.stdout = self._stdout

        if self.stderr and self.stderr_handler:
            self._stderr = FakeStd(self.stderr_handler, sys.stderr)
            self._old_stderr = sys.stderr
            sys.stderr = self._stderr

        # Check logging handlers for StreamHandlers so we can replace existing Streams
        loggers = [logging.root] + list(logging.Logger.manager.loggerDict.values())

        for logger in loggers:
            if not hasattr(logger, "handlers"):
                continue

            for handler in logger.handlers:
                if isinstance(handler, logging.StreamHandler):
                    current_stream = handler.stream

                    if current_stream == self._old_stdout:
                        new_stream = self._stdout
                    elif current_stream == self._old_stderr:
                        new_stream = self._stderr
                    else:
                        continue

                    if hasattr(handler, "setStream"):
                        handler.setStream(new_stream)
                    else:
                        stream_handler_set_stream(handler, new_stream)

                    self.hooked_handlers.append((logger, handler, current_stream))

    def __exit__(self, exception_type, exception_value, traceback):
        if self.stdout and self._old_stdout:
            sys.stdout = self._old_stdout
            self._old_stdout = None
            self._stdout = None

        if self.stderr and self._old_stderr:
            sys.stderr = self._old_stderr
            self._old_stderr = None
            self._stderr = None

        for _, handler, old_stream in self.hooked_handlers:
            try:
                if hasattr(handler, "setStream"):
                    handler.setStream(old_stream)
                else:
                    stream_handler_set_stream(handler, old_stream)
            except Exception:
                LOGGER.debug("Error cleaning up old streamer", exc_info=True)

        self.hooked_handlers = []


def start_tee_process(*args, **kwargs):
    process = multiprocessing.Process(target=_tee_process, args=args, kwargs=kwargs)
    # Python 2 don't accept daemon as an __init__ argument
    process.daemon = True
    process.start()
    return process


def _tee_process(pipe_out, control_pipe, real_out_fd, name, queue):
    """A loop that reads a pipe, write the content of the pipe into a given
    file descriptor and put it into a queue.
    Stops when there is some data in the control pipe.
    """
    # Ignore SIGINT signal, the tee process will be killed by the main process
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    i = 0
    pipes = [pipe_out, control_pipe]
    draining = False

    while pipes:
        try:
            r, _, _ = select.select(pipes, [], [], 0.1)

            if r:
                # Activate draining mode
                if control_pipe in r:
                    draining = True
                    pipes.remove(control_pipe)
                    os.close(control_pipe)
                    LOGGER.debug("Start draining tee process for %r", name)
                    continue

                # Read data
                data = os.read(pipe_out, 1024)

                # Write the data in original fd
                os.write(real_out_fd, data)
                i += 1

                queue.put((name, data))

            else:
                if draining:
                    LOGGER.debug("Draining tee process for %r (%r)", name, r)
                    break
        except Exception:
            LOGGER.debug("LOOP ERROR", exc_info=True)

    LOGGER.debug("Tee process for %r finished", name)


class Unbuffered(object):
    """A custom sys.stdout and sys.stderr that force flush after every
    writing.
    """

    def __init__(self, stream):
        self.__wrapped__ = stream

    def write(self, data):
        self.__wrapped__.write(data)
        self.__wrapped__.flush()

    def writelines(self, datas):
        self.__wrapped__.writelines(datas)
        self.__wrapped__.flush()

    def __getattr__(self, attr):
        # We cannot use wrapt in Python2 around sys.stdout/sys.stderr because
        # they are files and we cannot set custom attributes to files in Python
        # 2
        return getattr(self.__wrapped__, attr)


try:
    import fcntl

    from wurlitzer import _default_encoding, c_stderr_p, c_stdout_p, libc

    def _setup_fd(fd):
        """Common set-up code for initializing a (pipe) file descriptor"""

        # Make the file nonblocking (but don't lose its previous flags)
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    class PipeMultiprocessingStdWrapper(object):
        """This wrapper is very similar to the wurlitzer based-one that was
        used previously. The main difference with the wurlitzer one is that
        this logger uses subprocess in order to avoid having pipe consuming
        threads being blocked by the GIL held by the main thread running some
        C code (like caffe).

        Here is a high-level description of what it does to wrap std*:
        - For each std* file descriptor, create an Unix pipe.
        - Save the file descriptor around.
        - Replace the std file descriptor with the write part of the pipe.
        - Create a subprocess with the read part of the pipe and the original
          file descriptor. The process will read the pipe and will write the
          result on the original file descriptor.
        - The process will also send every piece of data to a multiprocessing
          Queue for sending them to the backend.
        - A thread is started in the main process to read the queue and
          enqueue messages in the Streamer.
        - We monkey-patch the high-level sys.stderr and sys.stdout to force a
          flush in order to avoid latency in output messages and reduce
          interleaving between stdout and stderr.

        Here is a high-level description of what it does to restore std*:
        - We force flush from the main thread.
        - We restore the original file descriptors.
        - We informs the processes and threads to finish consuming their pipe
          / queue and exit.
        - We wait for exit of processes and thread 5 seconds each.

        There is still a bit of interleaving between stdout and stderr in the
        caffe iris_tuto at the end.
        """

        def __init__(
            self, stdout=False, stdout_handler=None, stderr=False, stderr_handler=None
        ):
            self.stdout = stdout
            self.stdout_handler = stdout_handler
            self.stderr = stderr
            self.stderr_handler = stderr_handler

            self.stdout_control_out, self.stdout_control_in = os.pipe()
            self.stderr_control_out, self.stderr_control_in = os.pipe()

            self.stderr_process = None
            self.stdout_process = None

            self.queue = None

            self.consumer_thread = None

            self._save_fds = {}
            self._real_fds = {}
            self._pipe_out_fds = []

            self.original_stdout = None
            self.original_stderr = None

            self.draining = False

        def _prepare_fd_and_process(self, name, control_pipe):
            real_fd = getattr(sys, "__%s__" % name).fileno()
            save_fd = os.dup(real_fd)
            self._save_fds[name] = save_fd

            pipe_out, pipe_in = os.pipe()
            # Keep track of the pipe passed to the underlaying process, as we
            # are in a multiprocessing context we need to close them as well
            self._pipe_out_fds.append(pipe_out)
            os.dup2(pipe_in, real_fd)
            os.close(pipe_in)
            self._real_fds[name] = real_fd

            _setup_fd(pipe_out)

            process = start_tee_process(
                pipe_out, control_pipe, save_fd, name, self.queue
            )

            return process

        def __enter__(self):
            # Create the queue in __enter__ and destroy it in __exit__ to avoid https://bugs.python.org/issue33081
            self.queue = multiprocessing.Queue()

            if self.stdout:
                self.stdout_process = self._prepare_fd_and_process(
                    "stdout", self.stdout_control_out
                )
                # Patch Python stdout to force buffer flush
                self.original_stdout = sys.stdout
                sys.stdout = Unbuffered(sys.stdout)

            if self.stderr:
                self.stderr_process = self._prepare_fd_and_process(
                    "stderr", self.stderr_control_out
                )
                # Patch Python stderr to force buffer flush
                self.original_stderr = sys.stderr
                sys.stderr = Unbuffered(sys.stderr)

            # Setup and start the thread reading the queue
            self.consumer_thread = threading.Thread(target=self.consume_queue)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

        @staticmethod
        def _flush():
            libc.fflush(c_stdout_p)
            libc.fflush(c_stderr_p)

        def consume_queue(self):
            while True:
                try:
                    data = self.queue.get(timeout=0.2)
                except queue.Empty:
                    self._flush()

                    if self.draining is True:
                        LOGGER.debug("Consumer queue exiting because draining")
                        break

                    continue

                (name, output) = data
                LOGGER.debug("Got output for %s: %r", name, output)
                output = output.decode(_default_encoding, "replace")
                try:
                    if output and name == "stdout":
                        self.stdout_handler(output)
                    elif output and name == "stderr":
                        self.stderr_handler(output)
                except Exception:
                    # Avoid raising exceptions
                    pass

        def __exit__(self, a=None, b=None, c=None):
            LOGGER.debug("Cleaning PipeMultiprocessingStdWrapper")

            self._flush()

            # Restore existing the original fds
            for name, real_fd in self._real_fds.items():
                save_fd = self._save_fds[name]
                os.dup2(save_fd, real_fd)
                os.close(save_fd)

            # Restore existing std* objects
            if self.original_stdout is not None:
                sys.stdout = self.original_stdout

            if self.original_stderr is not None:
                sys.stderr = self.original_stderr

            # Start the draining of the processes and consumer thread
            os.write(self.stdout_control_in, b"STOP")
            os.write(self.stderr_control_in, b"STOP")
            self.draining = True

            if self.stdout_process:
                self.stdout_process.join(5)
                if self.stdout_process.is_alive():
                    LOGGER.debug("Terminating stdout process")
                    self.stdout_process.terminate()
                # Remove the process object to ensure all resources are cleaned
                self.stdout_process = None

            if self.stderr_process:
                self.stderr_process.join(5)
                if self.stderr_process.is_alive():
                    LOGGER.debug("Terminating stderr process")
                    self.stderr_process.terminate()
                # Remove the process object to ensure all resources are cleaned
                self.stderr_process = None

            if self.consumer_thread:
                self.consumer_thread.join(5)
                if self.consumer_thread.is_alive():
                    LOGGER.warning("Consumer thread did not finish")

            os.close(self.stdout_control_out)
            os.close(self.stdout_control_in)
            os.close(self.stderr_control_out)
            os.close(self.stderr_control_in)

            for fd in self._pipe_out_fds:
                os.close(fd)

            self.queue.close()
            self.queue.join_thread()

            # Create the queue in __enter__ and destroy it in __exit__ to avoid https://bugs.python.org/issue33081
            self.queue = None

    HAS_WURLITZER = True

except ImportError:
    HAS_WURLITZER = False


class StdLogger(object):
    def __init__(self, streamer, wrapper_class):
        self.streamer = streamer
        self.experiment = None
        self.wrapper = wrapper_class(
            stdout=True,
            stdout_handler=self.stdout_handler,
            stderr=True,
            stderr_handler=self.stderr_handler,
        )
        self.wrapper.__enter__()
        self.wrapped = True

    def clean(self):
        if self.wrapped is True:
            # Restore sys.std*
            self.wrapper.__exit__(None, None, None)
            self.wrapped = False

    def set_experiment(self, experiment):
        self.experiment = experiment

    def stdout_handler(self, data):
        try:
            self.handler(data, "stdout")
        except Exception:
            LOGGER.debug("Error saving stdout message", exc_info=True)

    def stderr_handler(self, data):
        try:
            self.handler(data, "stderr")
        except Exception:
            LOGGER.debug("Error saving stderr message", exc_info=True)

    def handler(self, data, std_name):
        if not self.experiment:
            return

        if std_name == "stdout":
            stderr = False
        elif std_name == "stderr":
            stderr = True
        else:
            raise NotImplementedError()

        message = StandardOutputMessage.create(
            context=self.experiment.context,
            use_http_messages=self.streamer.use_http_messages,
            output=data,
            stderr=stderr,
        )

        self.streamer.put_message_in_q(message)


def get_std_logger(wrapper_spec, streamer):
    """A factory that pass the right wrapper class to the StdLogger"""
    if wrapper_spec is None or wrapper_spec is False:
        return None

    elif wrapper_spec == "simple":
        return StdLogger(streamer, BaseStdWrapper)

    elif wrapper_spec == "native":
        if HAS_WURLITZER is True:
            return StdLogger(streamer, PipeMultiprocessingStdWrapper)

        else:
            LOGGER.warning(NATIVE_STD_WRAPPER_NOT_AVAILABLE)

    else:
        LOGGER.warning(UNKOWN_STD_WRAPPER_SPEC, wrapper_spec)

    return StdLogger(streamer, BaseStdWrapper)
