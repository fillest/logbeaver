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


def patch_everything ():
	logging.captureWarnings(True)

	sys.excepthook = log_exc

	#http://bugs.python.org/issue1230540
	init_old = threading.Thread.__init__
	def init(self, *args, **kwargs):
		init_old(self, *args, **kwargs)
		run_old = self.run
		def run_with_except_hook(*args, **kw):
			try:
				run_old(*args, **kw)
			except (KeyboardInterrupt, SystemExit):
				raise
			except:
				#http://hg.python.org/cpython/file/ee879c0ffa11/Lib/threading.py#l817
				if sys:
					sys.excepthook(*sys.exc_info())
				else:
					raise
		self.run = run_with_except_hook
	threading.Thread.__init__ = init

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

class NonRequestLoggingWSGIRequestHandler (wsgiref.simple_server.WSGIRequestHandler):
	def log_request (self, *args, **kwargs):
		pass

class TestException (Exception):
	pass

#https://github.com/Pylons/pyramid_exclog/blob/master/pyramid_exclog/__init__.py
class Middleware (object):
	def __init__(self, application):
		self.app = application

	def __call__(self, environ, start_response):
		try:
			return self.app(environ, start_response)
		# except (pyramid.httpexceptions.WSGIHTTPException,):
			# raise
		except:
			log_exc(*sys.exc_info())

			body = 'Internal Server Error'
			start_response('500 Internal Server Error', [
				('Content-Type', 'text/plain'), #?text/html
				('Content-Length', str(len(body)))
			])
			return [body]

def log_exc (exc_type, exc_value, exc_tb):
	try:
		#traceback is not inclede to message so we have to include it by hand (http://hg.python.org/cpython/file/ee879c0ffa11/Lib/logging/__init__.py#l475)
		text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))#.strip()
	finally:
		del exc_type, exc_value, exc_tb
	logging.error(text)

class Test1 (unittest.TestCase):
	def test_1 (self):
		cfg = 'test.ini'
		assert os.path.exists(cfg)
		pyramid.paster.setup_logging(cfg)
		# logging.info("test")

		successes = collections.deque()
		add_success = lambda i: successes.append(i)

		config = Configurator()

		# config.include('pyramid_exclog')
		
		config.add_route('test', '/test')
		def hello_world (request):
			# from pyramid.httpexceptions import exception_response; raise exception_response(404)
			# from pyramid.httpexceptions import HTTPNotFound; raise HTTPNotFound()
			raise TestException()
		config.add_view(hello_world, route_name='test', renderer = 'string')
		app = config.make_wsgi_app()
		app = Middleware(app)
		

		patch_everything()
		hook = sys.excepthook
		def for_test (*args):
			hook(*args)
			add_success(1)
		sys.excepthook = for_test
		

		with capture_stderr() as out:
			logging.warn("tessst")
		out = out[0]
		assert 'tessst' in out, out
		assert out.startswith('$${'), out
		assert out.endswith('}$$'), repr(out)
		assert out.count('$${') == 1, out
		assert out.count('}$$') == 1, out
		
		with capture_stderr() as out:
			warnings.warn("tessst")
		out = out[0]
		assert 'tessst' in out, out
		assert out.startswith('$${'), out
		assert out.endswith('}$$'), out
		assert out.count('$${') == 1, out
		assert out.count('}$$') == 1, out
		

		def raise_exc_in_thread ():
			raise TestException()
		thr = threading.Thread(target = raise_exc_in_thread)
		# thr.daemon = True
		with capture_stderr() as out:
			thr.start()
			time.sleep(0.2)
		out = out[0]
		assert 'TestException' in out, out
		assert out.startswith('$${'), out
		assert out.endswith('}$$'), out
		assert out.count('$${') == 1, out
		assert out.count('}$$') == 1, out

		server = make_server('127.0.0.1', 7325, app, handler_class = NonRequestLoggingWSGIRequestHandler)

		def req_app ():
			time.sleep(0.2)
			try:
				urllib2.urlopen("http://localhost:7325/test").read()
			except urllib2.HTTPError as e:
				if e.code != 500:
				# if e.code != 404:
					raise
				add_success(2)
		thr = threading.Thread(target = req_app)
		# thr.daemon = True
		thr.start()

		with capture_stderr() as out:
			server.handle_request()
		out = out[0]
		assert 'TestException' in out, out
		assert out.startswith('$${'), out
		assert out.endswith('}$$'), out
		assert out.count('$${') == 1, out
		assert out.count('}$$') == 1, out


		self.assertEquals(len(successes), 2, successes)

	def test_2 (self):
		fd = os.open("test1.log", os.O_RDONLY)

		start_pos = 0

		while True:
			real_pos = os.lseek(fd, start_pos, os.SEEK_SET)
			assert real_pos == start_pos, (start_pos, real_pos)

			#TODO if size is not long enough, can stuck in infinite loop
			# read_size = 1024
			read_size = 1024 + 400
			chunk = os.read(fd, read_size)
			if not chunk: #eof
				print "eof"
				#TODO
				break
				# time.sleep(1)
				# continue
			print "#######", len(chunk), chunk

			pos = 0

			print "_"*100
			while True:
				maybe_start = chunk[pos:pos+len('$${')]
				if not maybe_start:
					break
				assert maybe_start == '$${', repr(maybe_start)

				i = chunk.find('}$$', pos)
				if i == -1:
					break

				msg = chunk[pos:i]
				print "@@@@", msg#.strip()

				# pos = i + len('}$$')
				# pos = i + len('}$$\n')
				assert chunk[i:i+len('}$$\n')] == '}$$\n', repr(chunk[i:i+len('}$$\n')])
				pos = i + len('}$$\n')

				#can yield pos here

				# # assert chunk[pos:pos+len('\n$${')] == '\n$${', repr(chunk[pos:pos+len('\n$${')])
				# assert chunk[pos:pos+len('\n')] == '\n', repr(chunk[pos:pos+len('\n')])
				# # pos += len('\n$${')
				# pos += len('\n')

			# left_data = chunk[pos:]
			#if left_data
				#TODO data can stuck here forever if disk is full

			start_pos += pos
			# break
