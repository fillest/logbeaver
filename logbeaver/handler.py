import logging
import beanstalkc
import cPickle
import sys
import threading


PICKLE_PROTOCOL = 2


#https://docs.python.org/2/library/logging.html#handler-objects
#https://hg.python.org/cpython/file/2.7/Lib/logging/handlers.py#l432
class BeanstalkHandler (logging.Handler):
	def __init__ (self, source = 'test', host = 'localhost', port = 11301,  #port is default + 1
	              tube = 'logbeaver', connect_timeout = 3, level = logging.NOTSET):
		logging.Handler.__init__(self, level = level)

		self.source = source
		self.host = host
		self.port = port
		self.tube = tube
		self.connect_timeout = connect_timeout

		self.conn = None

	def _connect (self):
		try:
			self.conn = beanstalkc.Connection(host = self.host, port = self.port, connect_timeout = self.connect_timeout)

			old = self.conn._socket.gettimeout()  #None
			self.conn._socket.settimeout(3) #TODO report? think about other commands
			self.conn.use(self.tube)
			self.conn._socket.settimeout(old)
		# except beanstalkc.SocketError as e:
		# 	if e.errno != 111: #Connection refused
		# 		raise
		except Exception as e:
			#TODO where are we? separate thread?
			sys.stderr.write("Failed to connect to beanstalk: %s\n" % e) #TODO wtf doesnt get printed in test?
			sys.stderr.flush()
			raise #TODO it does nothing

	def emit (self, record):
		d = None
		try:
			#patched copypaste https://hg.python.org/cpython/file/2.7/Lib/logging/handlers.py#l532
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

			if not self.conn:
				self._connect()

			# print d['args']
			d['args'] = None
			s = cPickle.dumps(d, PICKLE_PROTOCOL)
			if ei:
				record.exc_info = ei  #for next handler

			#TODO try catch
			_job_id = self.conn.put(s)
			assert _job_id
		except (KeyboardInterrupt, SystemExit):
			raise
		except:
			#writing to beanstalk failed so we just dump out the record and try reconnect next time
			#TODO try resend? think if it can explode under load
			sys.stderr.write("Failed to send to beanstalk, record: %s\n" % (d or record.__dict__))
			sys.stderr.flush()

			#TODO what happens to the exc? -- seems its being printed like that:
			# Traceback (most recent call last):
			#   File "/home/ubuntu/venv/local/lib/python2.7/site-packages/logbeaver/handler.py", line 42, in emit
			#     self._connect()
			#   File "/home/ubuntu/venv/local/lib/python2.7/site-packages/logbeaver/handler.py", line 26, in _connect
			#     self.conn = beanstalkc.Connection(host = self.host, port = self.port, connect_timeout = self.connect_timeout)
			#   File "/home/ubuntu/venv/local/lib/python2.7/site-packages/beanstalkc.py", line 59, in __init__
			#     self.connect()
			#   File "/home/ubuntu/venv/local/lib/python2.7/site-packages/beanstalkc.py", line 65, in connect
			#     SocketError.wrap(self._socket.connect, (self.host, self.port))
			#   File "/home/ubuntu/venv/local/lib/python2.7/site-packages/beanstalkc.py", line 43, in wrap
			#     raise SocketError(err)
			# SocketError: [Errno 111] Connection refused
			# Logged from file __init__.py, line 63
			self.handleError(record)

	#https://docs.python.org/2/library/logging.html#logging.Handler.handleError
	def handleError(self, record):
		if self.conn:
			self.conn.close()
			self.conn = None
		#TODO SocketHandler puts it in 'else', why?
		logging.Handler.handleError(self, record)

	def close (self):
		self.acquire()
		try:
			if self.conn:
				self.conn.close()
				self.conn = None
		finally:
			self.release()

		logging.Handler.close(self)

def log_exc (exc_type, exc_value, exc_tb):
	# try:
	# 	#traceback is not included to message so we have to include it by hand (http://hg.python.org/cpython/file/ee879c0ffa11/Lib/logging/__init__.py#l475)
	# 	text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))#.strip()
	# finally:
	# 	del exc_type, exc_value, exc_tb
	# logging.error(text)

	val = u"%s" % exc_value
	if val:
		msg = u"%s: %s" % (exc_type, exc_value)
	else:
		msg = u"%s" % exc_type
	logging.error(msg, exc_info = (exc_type, exc_value, exc_tb))

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

	def __call__(self, environ, start_response):
		try:
			return self.app(environ, start_response)
		#TODO
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