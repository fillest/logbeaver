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
import que
import handler

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

class Test1 (unittest.TestCase):
	def test_logging (self):
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
		app = handler.Middleware(app)
		

		handler.patch_everything()
		hook = sys.excepthook
		def for_test (*args):
			hook(*args)
			add_success(1)
		sys.excepthook = for_test
		

		p = que.QueueProcessor()
		g = p.run_loop(timeout = 3)

		p.clear_queue()

		# print p.conn.stats()

		# with capture_stderr() as out:
		# 	logging.warn("tessst")
		logging.warn("test1")
		# print out

		warnings.warn("test2")


		r = g.next()
		# print r
		self.assertEquals(r['msg'], "test1")
		g.next()

		r = g.next()
		# print r
		self.assertEquals(r['msg'], "%s")
		assert "test2" in r['msg_rendered'], r
		g.next()

		# print p.conn.stats()

		self.assertEquals(p.clear_queue(), 0)


		def raise_exc_in_thread ():
			raise TestException()
		thr = threading.Thread(target = raise_exc_in_thread)
		# thr.daemon = True
		thr.start()
		time.sleep(0.2)

		r = g.next()
		# print r
		# self.assertEquals(r['msg'], "%s")
		assert "TestException" in r['exc_text'], r
		g.next()


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

		server.handle_request()

		r = g.next()
		# print r
		# self.assertEquals(r['msg'], "%s")
		assert "TestException" in r['exc_text'], r
		assert "test.TestException" in r['msg_rendered'], r
		g.next()


		try:
			1 / 0
		except:
			logging.exception('test')

		r = g.next()
		# print r
		self.assertEquals(r['msg_rendered'], "test")
		assert "ZeroDivisionError" in r['exc_text'], r
		g.next()


		self.assertEquals(len(successes), 2, successes)


		return


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

	def test_tail (self):
		return

		fd, path = tempfile.mkstemp()
		# print path
		try:
			def write_to_log ():
				for i in range(2):
					time.sleep(0.3)
					with open(path, 'ab') as f:
						f.write("$${2014-08-01 02:35:25,978 WARNI test.py:test_1:154  tessst %s}$$\n" % i)
					# print "wrote"
			thr = threading.Thread(target = write_to_log)
			thr.daemon = True
			thr.start()

			start_pos = 0
			for i, (next_pos, msg) in enumerate(tail_msgs(fd, start_pos, retry_interval = 0.1)):
				# print msg
				assert 'tessst' in msg  #TODO test whole match
				
				if i == 2 - 1:
					break
		finally:
			os.remove(path)




		# from raven import Client
		# import datetime
		# client = Client('sync+http://9d76d6872ce04d8bb7b11b396f30721d:48605f7b5803443fb228237cea378fc7@localhost:9000/1?timeout=1000')
		# data = {}
		# time_spent = 0
		# extra = {}
		# stack = None
		# tags = {}
		# date = datetime.datetime.utcnow()
		mesg = """    response = view_callable(context, request)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/pyramid/config/views.py", line 237, in _secured_view
    return view(context, request)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/pyramid/config/views.py", line 347, in rendered_view
    result = view(context, request)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/pyramid/config/views.py", line 493, in _requestonly_view
    response = view(request)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/arss_reader/views/__init__.py", line 235, in entries_set_is_read
    UserSource.modify_d_read_entry_num(user.id, entry.source_id, 1 if is_read else -1)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/arss_reader/db/models.py", line 60, in modify_d_read_entry_num
    .update({cls.d_read_entry_num: cls.d_read_entry_num + mod}, synchronize_session = False))
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/query.py", line 2505, in update
    update_op.exec_()
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/persistence.py", line 811, in exec_
    self._do_pre()
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/persistence.py", line 833, in _do_pre
    session._autoflush()
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/session.py", line 1138, in _autoflush
    self.flush()
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/session.py", line 1817, in flush
    self._flush(objects)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/session.py", line 1935, in _flush
    transaction.rollback(_capture_exception=True)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/util/langhelpers.py", line 58, in __exit__
    compat.reraise(exc_type, exc_value, exc_tb)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/session.py", line 1899, in _flush
    flush_context.execute()
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/unitofwork.py", line 372, in execute
    rec.execute(self)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/unitofwork.py", line 481, in execute
    self.dependency_processor.process_saves(uow, states)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/dependency.py", line 1083, in process_saves
    secondary_update, secondary_delete)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/orm/dependency.py", line 1126, in _run_crud
    connection.execute(statement, secondary_insert)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/engine/base.py", line 662, in execute
    params)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/engine/base.py", line 761, in _execute_clauseelement
    compiled_sql, distilled_params
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/engine/base.py", line 874, in _execute_context
    context)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/engine/base.py", line 1024, in _handle_dbapi_exception
    exc_info
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/util/compat.py", line 163, in raise_from_cause
    reraise(type(exception), exception, tb=exc_tb)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/engine/base.py", line 867, in _execute_context
    context)
  File "/opt/arss_reader/venv/lib/python2.6/site-packages/sqlalchemy/engine/default.py", line 324, in do_execute
    cursor.execute(statement, parameters)
IntegrityError: (IntegrityError) duplicate key value violates unique constraint "users__entries_pkey"
 'INSERT INTO users__entries (user_id, entry_id) VALUES (%(user_id)s, %(entry_id)s)' {'entry_id': 3495177, 'user_id': 2}
"""
		# # mesg = "test"
		# msg = client.build_msg('raven.events.Message', data, date, time_spent, extra, stack, tags=tags, message = mesg,
		# 	level=logging.INFO)
		# client.send(**msg)


		import json
		import requests
		import socket
		url = 'http://localhost:6543/create'
		headers = {'content-type': 'application/json'}
		payload = [{
			'source': 'test',
			'time': time.time(),
			'title': 'exception',
			'content': mesg,
			'level': 'error',
			'host': socket.getfqdn(),
		}]
		r = requests.post(url, data=json.dumps(payload), timeout=5, headers=headers)
		#requests.exceptions.ConnectionError
		#requests.exceptions.HTTPError
		assert r.status_code == requests.codes.ok
		self.assertEquals(r.text, 'ok')

		# fd = os.open("test1.log", os.O_RDONLY)
		# # fd = os.open("test1_cut.log", os.O_RDONLY)

		# start_pos = 0
		# for next_pos, msg in fetch_msgs(fd, start_pos):
		# 	print "@@@@", next_pos, msg
		# 	if next_pos is None:
		# 		break

		# import beanstalkc
		# port = 11300
		# beanstalk = beanstalkc.Connection(host='localhost', port=port)
		# beanstalk.use('test_tube')
		# print beanstalk.stats()

		# beanstalk.close()

	def test_save_pos (self):
		return


		fd, path = tempfile.mkstemp()
		try:
			# def write_to_log ():
			# 	for i in range(2):
			# 		time.sleep(0.3)
			# 		with open(path, 'ab') as f:
			# 			f.write("$${2014-08-01 02:35:25,978 WARNI test.py:test_1:154  tessst %s}$$\n" % i)
			# 		# print "wrote"
			# thr = threading.Thread(target = write_to_log)
			# thr.daemon = True
			# thr.start()

			db_name = "test.sqlite"
			if os.path.exists(db_name):
				os.remove(db_name)

			conn = sqlite3.connect(db_name)

			
			init_db(conn)

			conn = sqlite3.connect(db_name)


			start_pos = fetch_pos(path, conn)

			def write_to_log (ids):
				for i in ids:
					time.sleep(0.3)
					with open(path, 'ab') as f:
						f.write("$${2014-08-01 02:35:25,978 WARNI test.py:test_1:154  tessst %s}$$\n" % i)
					# print "wrote"
			thr = threading.Thread(target = write_to_log, args = [[0, 1]])
			thr.daemon = True
			thr.start()

			msgs = []

			for i, (next_pos, msg) in enumerate(tail_msgs(fd, start_pos, retry_interval = 0.1)):
				# print msg
				# assert ('tessst %s' % i) in msg
				msgs.append(msg)

				with conn:
					conn.execute("update file_positions set position = ? where file_path = ?", (next_pos, path))
				
				if i == 2 - 1:
					break


			start_pos = fetch_pos(path, conn)

			thr = threading.Thread(target = write_to_log, args = [[2, 3]])
			thr.daemon = True
			thr.start()

			for i, (next_pos, msg) in enumerate(tail_msgs(fd, start_pos, retry_interval = 0.1)):
				# print msg
				# assert ('tessst %s' % i) in msg
				msgs.append(msg)

				with conn:
					conn.execute("update file_positions set position = ? where file_path = ?", (next_pos, path))
				
				if i == 2 - 1:
					break

			for i in [0, 1, 2, 3]:
				assert ('tessst %s' % i) in msgs[i], msgs[i]
		finally:
			os.remove(path)

def init_db (conn):
	#TODO how big can integer be? think about several gb files
	conn.execute("""create table if not exists file_positions (
		file_path		text NOT NULL primary key,
		position 		integer NOT NULL
	);
	""")
	conn.commit()
	conn.close()

def fetch_pos (path, conn):
	res = conn.execute("""
		select position
		from file_positions
		where file_path = ?
	""", [path]).fetchone()

	if res is None:
		with conn:
			conn.execute("insert into file_positions (file_path, position) values (?, ?)", (path, 0))
		return 0
	else:
		return res[0]

def tail_msgs (fd, start_pos, retry_interval = 0.5):
	while True:
		for next_pos, msg in fetch_msgs(fd, start_pos):
			if next_pos is None:
				time.sleep(retry_interval)
			else:
				start_pos = next_pos
				yield next_pos, msg

def fetch_msgs (fd, start_pos):
	msg_read_num_last = 0

	while True:
		real_pos = os.lseek(fd, start_pos, os.SEEK_SET)
		assert real_pos == start_pos, (start_pos, real_pos)

		#TODO if size is not long enough, can stuck in infinite loop
		# read_size = 1024
		read_size = 1024 + 400
		chunk = os.read(fd, read_size)
		if not chunk: #eof
			# print "eof"
			yield None, None
			return
		# print "#######", len(chunk), chunk

		pos = 0
		msg_parsed_num = 0

		# print "_"*100
		while True:
			maybe_start = chunk[pos:pos+len('$${')]
			if not maybe_start:
				break
			#TODO parse the stuff here, it can be caused by print
			assert maybe_start == '$${', repr(maybe_start)

			i = chunk.find('}$$', pos)
			if i == -1:
				break

			msg_parsed_num += 1

			msg = chunk[pos:i]
			# print "@@@@", msg#.strip()

			# pos = i + len('}$$')
			# pos = i + len('}$$\n')
			assert chunk[i:i+len('}$$\n')] == '}$$\n', repr(chunk[i:i+len('}$$\n')])
			pos = i + len('}$$\n')

			yield start_pos + pos, msg#.strip()

			# # assert chunk[pos:pos+len('\n$${')] == '\n$${', repr(chunk[pos:pos+len('\n$${')])
			# assert chunk[pos:pos+len('\n')] == '\n', repr(chunk[pos:pos+len('\n')])
			# # pos += len('\n$${')
			# pos += len('\n')

		# left_data = chunk[pos:]
		#if left_data
			#TODO data can stuck here forever if disk is full; also files can be deleted after this so
			#it has to be restarted by hand (bad)

		if (not msg_parsed_num) and not msg_read_num_last:
			# print "data is still incomplete"
			yield None, None
			return

		msg_read_num_last = msg_parsed_num

		start_pos += pos
