#coding: utf-8
import unittest
import logbeaver
import pyramid.paster
from wsgiref.simple_server import make_server
from pyramid.config import Configurator
import os
import logging
import threading
import urllib2
import time
import wsgiref
import sys
import warnings
import collections
import traceback
import sys
from contextlib import contextmanager
import tempfile
import sqlite3
from logbeaver import queproc
from logbeaver import handler


#http://stackoverflow.com/questions/4675728/redirect-stdout-to-a-file-in-python/22434262#22434262
def fileno(file_or_fd):
    fd = getattr(file_or_fd, 'fileno', lambda: file_or_fd)()
    if not isinstance(fd, int):
        raise ValueError("Expected a file (`.fileno()`) or a file descriptor")
    return fd

@contextmanager
def stdout_redirected(to=os.devnull, stdout=None):
    if stdout is None:
       stdout = sys.stdout

    stdout_fd = fileno(stdout)
    # copy stdout_fd before it is overwritten
    with os.fdopen(os.dup(stdout_fd), 'wb') as copied:
        stdout.flush()  # flush library buffers that dup2 knows nothing about
        try:
            os.dup2(fileno(to), stdout_fd)  # $ exec >&to
        except ValueError:  # filename
            with open(to, 'wb') as to_file:
                os.dup2(to_file.fileno(), stdout_fd)  # $ exec > to
        try:
            yield stdout # allow code to be run with the redirected stdout
        finally:
            stdout.flush()
            os.dup2(copied.fileno(), stdout_fd)  # $ exec >&copied

@contextmanager
def capture_stderr ():
	result = [None]
	r, w = os.pipe()
	try:
		with stdout_redirected(to = w, stdout = sys.stderr):
			yield result
	finally:
		os.close(w)
		rf = os.fdopen(r)
		result[0] = rf.read().strip()
		# print result[0]
		rf.close()