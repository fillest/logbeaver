import logging
import beanstalkc
import cPickle
import sys
import threading


#https://docs.python.org/2/library/logging.html#handler-objects
#https://hg.python.org/cpython/file/2.7/Lib/logging/handlers.py#l432
class BeanstalkHandler (logging.Handler):
	def __init__ (self, host = 'localhost', port = 11300, tube = 'test_tube', level=logging.NOTSET):
		logging.Handler.__init__(self, level)
		self.host = host
		self.port = port
		self.tube = tube
		self.conn = None

	def emit (self, record):
		try:
			if not self.conn:
				# print "!!! connnn", self.host, self.port
				try:
					self.conn = beanstalkc.Connection(host = self.host, port = self.port, connect_timeout = 5)
					self.conn.use(self.tube)
				except Exception as e:
					#TODO
					sys.stderr.write("failed to connect to beanstalk: %s\n" % e) #TODO wtf doesnt get printed in test?
					sys.stderr.flush()
					raise #TODO it does nothing

			# print "***", record
			# print record.__dict__

			#patched copypaste https://hg.python.org/cpython/file/2.7/Lib/logging/handlers.py#l532
			ei = record.exc_info
			# self.format(record)
			if ei:
				# just to get traceback text into record.exc_text ...
				dummy = self.format(record)
				record.exc_info = None  # to avoid Unpickleable error
			# See issue #14436: If msg or args are objects, they may not be
			# available on the receiving end. So we convert the msg % args
			# to a string, save it as msg and zap the args.
			d = dict(record.__dict__)
			d['msg_rendered'] = record.getMessage()
			# print d['args']
			d['args'] = None
			# print d
			s = cPickle.dumps(d, 2)
			if ei:
				record.exc_info = ei  # for next handler

			#TODO try catch
			_job_id = self.conn.put(s)
			assert _job_id
			# print self.conn.stats()
		except (KeyboardInterrupt, SystemExit):
			raise
		except:
			#            self.handleError(record)
			raise

	def close (self):
		# print "!!! closing"
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

	logging.error('%s: %s' % (exc_type, exc_value), exc_info = (exc_type, exc_value, exc_tb))
	del exc_type, exc_value, exc_tb

def patch_everything ():
	#warnings doesn't use logging by default
	logging.captureWarnings(True)

	#default sys.excepthook doesn't use logging
	sys.excepthook = log_exc

	#for extra fun, threading doesn't use sys.excepthook (http://bugs.python.org/issue1230540)
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
				if sys:  #http://hg.python.org/cpython/file/ee879c0ffa11/Lib/threading.py#l817
					sys.excepthook(*sys.exc_info())
				else:
					raise
		self.run = run_with_except_hook
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