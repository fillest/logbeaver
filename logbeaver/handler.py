import logging
import beanstalkc
import cPickle
import sys
import threading
import time
import os


PICKLE_PROTOCOL = 2


def now_formatted ():
	return time.strftime("%Y-%m-%d %H:%M:%SUTC", time.gmtime())

#https://docs.python.org/2/library/logging.html#handler-objects
#https://hg.python.org/cpython/file/2.7/Lib/logging/handlers.py#l432
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
			#modified copypaste from https://hg.python.org/cpython/file/2.7/Lib/logging/handlers.py#l532
			
			ei = record.exc_info
			# self.format(record)
			if ei:
				# just to get traceback text into record.exc_text ...
				_ = self.format(record)
				record.exc_info = None  # to avoid Unpickleable error
			# See issue #14436: If msg or args are objects, they may not be
			# available on the receiving end. So we convert the msg % args
			# to a string, save it as msg and zap the args.
			d = dict(record.__dict__)
			d['_source'] = self.source
			#https://docs.python.org/2/library/logging.html#logrecord-objects
			d['msg_rendered'] = record.getMessage()
			#  but 'msg' can be obj (see above) so we need to do smth with it
			#  lets just del it until getting a better idea
			del d['msg']

			#reconnect on fork detection (e.g. gunicorn preload case)
			pid = os.getpid()
			if pid != self._pid:
				self._close_conn()
				self._pid = pid
			if not self.conn:
				self._connect()

			d['args'] = None

			s = cPickle.dumps(d, PICKLE_PROTOCOL) #TODO can use marshal here
			
			if ei:
				record.exc_info = ei  #for next handler

			_job_id = self.conn.put(s)
			assert _job_id
		except (KeyboardInterrupt, SystemExit):
			self._close_conn()
			raise
		except:
			#writing to beanstalk failed so we just dump out the record and try to reconnect next time

			self._log("Failed to send this record to beanstalk: %s" % (d or record.__dict__))

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
			self._log("Failed to connect to beanstalk: %s" % e)
			raise

	def _log (self, msg):
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

#https://github.com/Pylons/pyramid_exclog/blob/master/pyramid_exclog/__init__.py
class Middleware (object):
	def __init__(self, application):
		self.app = application

	def response_with_error (self, start_response):
		body = 'Internal Server Error'
		start_response('500 Internal Server Error', [
			('Content-Type', 'text/plain'), #TODO text/html?
			('Content-Length', str(len(body)))
		])
		return [body]

	def __call__(self, environ, start_response):
		try:
			return self.app(environ, start_response)
		#TODO -- seems no need according to the test?
		# except (pyramid.httpexceptions.WSGIHTTPException,):
			# raise
		except:
			#useless tracebacks really happen (e.g. a view is a class in a lib) so we need request location
			extra = {}
			for k in ('REQUEST_METHOD', 'PATH_INFO', 'QUERY_STRING'):
				extra[k] = environ.get(k)
			log_exc(*sys.exc_info(), extra = extra)

			return self.response_with_error(start_response)