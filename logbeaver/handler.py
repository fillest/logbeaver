import logging
import cPickle
import sys
import threading
import time
import os
import beanstalkc
from pyramid.threadlocal import get_current_request


PICKLE_PROTOCOL = 2


def now_formatted ():
	return time.strftime("%Y-%m-%d %H:%M:%S +0000", time.gmtime())

#https://hg.python.org/cpython/file/2.7/Lib/logging/handlers.py#l432 (SocketHandler)
class BeanstalkHandler (logging.Handler):
	def __init__ (self, source = 'test', tube = 'logbeaver', host = 'localhost', port = 11301,  #port is default + 1
	              connect_timeout = 3, level = logging.NOTSET):
		logging.Handler.__init__(self, level = level)

		self.source = source
		self.host = host
		self.port = port
		self.tube = tube
		self.connect_timeout = connect_timeout

		self._pid = os.getpid()
		self.conn = None

	def emit (self, record):
		d = None
		try:
			#inspired by https://hg.python.org/cpython/file/2.7/Lib/logging/handlers.py#l532 (makePickle)

			#if there's another handler like console, it can call .format and record can be already mutated
			#Also this gets traceback text into record.exc_text
			_ = self.format(record)

			d = record.__dict__.copy()
			d['_source'] = self.source
			del d['exc_info']  #unpickleable
			#.message comes from https://hg.python.org/cpython/file/4252bdba6e89/Lib/logging/__init__.py#l471 (Formatter.format)
			#and we overwrite it anyway
			d['message'] = record.getMessage()  #https://docs.python.org/2/library/logging.html#logrecord-objects
			#msg and args can be unpickleable
			if isinstance(d['msg'], basestring):
				#py.warnings logger does this and people can too
				if d['msg'] == '%s':
					del d['msg']
			else:
				del d['msg']
			del d['args']
			
			request = get_current_request()
			if request:
				d.update(get_path_params(request.environ)) 

			#reconnect on fork detection (e.g. gunicorn preload case)
			pid = os.getpid()
			if pid != self._pid:
				self._close_conn()
				self._pid = pid
			if not self.conn:
				self._connect()

			serialized = cPickle.dumps(d, PICKLE_PROTOCOL)
			
			_job_id = self.conn.put(serialized)
			if not _job_id:
				raise Exception("failed to put to beanstalkd")
		except (KeyboardInterrupt, SystemExit):
			self._close_conn()
			raise
		except:
			#so putting to beanstalk failed and it may be too expensive to continue resending so
			#we just print the record and try to reconnect next time
			self._log("Logbeaver warning: failed to put this log record to beanstalkd: %s" % (d or record.__dict__))

			#TODO what happens to the exc? -- seems its being printed like that:
			# Traceback (most recent call last):
			# ...
			#   File "/home/ubuntu/venv/local/lib/python2.7/site-packages/beanstalkc.py", line 43, in wrap
			#     raise SocketError(err)
			# SocketError: [Errno 111] Connection refused
			# Logged from file __init__.py, line 63
			self.handleError(record)

	def _close_conn (self):
		if self.conn:
			self.conn.close()
			self.conn = None

	def _connect (self):
		try:
			self.conn = beanstalkc.Connection(host = self.host, port = self.port, connect_timeout = self.connect_timeout)

			# old = self.conn._socket.gettimeout()  #normally None
			self.conn._socket.settimeout(5) #TODO not documented

			self.conn.use(self.tube)
		# except beanstalkc.SocketError as e:
		# 	if e.errno != 111: #Connection refused
		# 		raise
		except Exception as e:
			self._log("Logbeaver warning: failed to connect to beanstalkd: %s" % e)
			raise

	def _log (self, msg):
		#TODO use custom logger?
		sys.stderr.write("%s  %s\n" % (now_formatted(), msg))
		sys.stderr.flush()

	#https://docs.python.org/2/library/logging.html#logging.Handler.handleError
	def handleError(self, record):
		self._close_conn()
		#TODO SocketHandler puts it in 'else', why?
		logging.Handler.handleError(self, record)

	def close (self):
		self.acquire()
		try:
			self._close_conn()
		finally:
			self.release()

		logging.Handler.close(self)

def log_exc (exc_type, exc_value, exc_tb, extra = None):
	val = u"%s" % exc_value
	if val:
		msg = u"%s: %s" % (exc_type, exc_value)
	else:
		msg = u"%s" % exc_type
	logging.error(msg, exc_info = (exc_type, exc_value, exc_tb), extra = extra)

	del exc_type, exc_value, exc_tb

def patch_everything ():
	#warnings don't use logging by default
	logging.captureWarnings(True)

	#default sys.excepthook doesn't use logging
	sys.excepthook = log_exc

	#for extra fun, threading doesn't use sys.excepthook (http://bugs.python.org/issue1230540)
	init_old = threading.Thread.__init__
	def init(self, *args, **kwargs):
		init_old(self, *args, **kwargs)
		
		run_old = self.run
		def run_with_excepthook(*args, **kw):
			try:
				run_old(*args, **kw)
			except (KeyboardInterrupt, SystemExit):
				raise
			except:
				if sys:  #http://hg.python.org/cpython/file/ee879c0ffa11/Lib/threading.py#l817
					sys.excepthook(*sys.exc_info())
				else:
					raise
		self.run = run_with_excepthook
	threading.Thread.__init__ = init

def get_path_params (environ):
	#request location happens to be useful anyway but mainly because we can get
	#unhelpful tracebacks (e.g. a view is a generic class in a lib)
	extra = {}
	for k in ('REQUEST_METHOD', 'PATH_INFO', 'QUERY_STRING'):
		extra[k] = environ.get(k)
	return extra

#https://github.com/Pylons/pyramid_exclog/blob/master/pyramid_exclog/__init__.py
class Middleware (object):
	def __init__(self, application):
		self.app = application

	def response_with_error (self, start_response):
		body_text = 'Internal Server Error'
		start_response('500 Internal Server Error', [
			('Content-Type', 'text/plain'),
			('Content-Length', str(len(body_text)))
		])
		return [body_text]

	def __call__(self, environ, start_response):
		try:
			return self.app(environ, start_response)
		except:
			extra = get_path_params(environ)
			log_exc(*sys.exc_info(), extra = extra)

			return self.response_with_error(start_response)