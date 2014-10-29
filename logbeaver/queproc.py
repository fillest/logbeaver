import beanstalkc
import cPickle
import json
import requests
import socket
import time
import calendar
import argparse
import logging
import os
import sys


class QueueProcessor (object):
	def __init__ (self, upstream_uri = 'http://localhost:6543', bean_tube = 'logbeaver',
			bean_host = 'localhost', bean_port = 11301, bean_conn_timeout = 10, send_timeout = 60,
			bean_reconnect_interval = 5, bean_reconnect_limit = 0):
		self.logger = logging.getLogger(__name__)  #local to avoid disabling by pyramid.paster.setup_logging

		self.send_timeout = send_timeout
		self.upstream_uri = upstream_uri
		self.bean_tube = bean_tube
		self.bean_reconnect_interval = bean_reconnect_interval
		self.bean_reconnect_limit = bean_reconnect_limit

		self._connect(bean_host, bean_port, bean_conn_timeout)

	def _connect (self, bean_host, bean_port, bean_conn_timeout):
		t_start = time.time()
		while True:
			try:
				self.conn = beanstalkc.Connection(host = bean_host, port = bean_port, connect_timeout = bean_conn_timeout)
				break
			except beanstalkc.BeanstalkcException as e:
				self.logger.warning("Failed to connect to beanstalkd (%s:%s), will retry: %s" % (bean_host, bean_port, e))
				if self.bean_reconnect_limit:
					if time.time() - t_start >= self.bean_reconnect_limit:
						self.logger.error("Reconnect time limit reached")
						raise
				time.sleep(self.bean_reconnect_interval)

		# old = self.conn._socket.gettimeout()  #normally None
		# self.conn._socket.settimeout(15) #TODO not documented
		self.conn.watch(self.bean_tube)
		if self.bean_tube != 'default':
			self.conn.ignore('default')

	def clear_queue (self):
		num = 0
		while True:
			job = self.conn.reserve(timeout = 0)
			if not job:
				return num
			job.delete()
			num += 1

	def inspect (self):
		self.conn.use(self.bean_tube)

		self.logger.info("Tubes: %s" % (self.conn.tubes(),))
		self.logger.info("Stats: %s" % (self.conn.stats(),))
		job = self.conn.peek_ready()
		if job:
			self.logger.info("Next 'ready' job #%s data:\n%s" % (job.jid, cPickle.loads(job.body)))
		else:
			self.logger.info("Queue is empty")

	def delete (self, jid):
		# self.conn.use(self.bean_tube)

		job = self.conn.peek(jid)
		if not job:
			self.logger.error("Job not found: %s" % jid)
			sys.exit(1)

		job.delete()

		self.logger.info("Done")

	def run_loop (self, timeout = None, reraise_on_error = False):
		try:
			while True:
				job = self.conn.reserve(timeout = timeout)
				body = job.body
				data = None
				try:
					data = cPickle.loads(body)
					#there should be no raw binary data in msg, because json, postgres and other stuff expect unicode and
					#it's unreadable anyway so we enforce unicode decoding. Run your binary data through base64 for example
					#TODO document it
					data['msg_rendered'] = data['msg_rendered'].decode('utf-8', 'replace')

					if not self._send(data, reraise_on_error):
						job.release()
						time.sleep(3)
						#TODO touch and try send again? + limit for tests
						continue
					else:
						#"Once we are done with processing a job, we have to mark it as done, otherwise jobs are
						#re-queued by beanstalkd after a "time to run" (120 seconds, per default) is surpassed."
						job.delete()
				except:
					job.release()

					self.logger.error("Error (see traceback below) while processing item:\n%s" % data)
					raise

				yield data
		finally:
			self.conn.close()

	def _send (self, data, reraise_on_error):
		events = [{
			'source': data['_source'],
			#TODO use logger time + google "python logging utc" + or hard-set utc for app?
			'time': calendar.timegm(time.gmtime()), #TODO drops milliseconds, does pg support them anyway?
			'host': socket.getfqdn(), #TODO cache?
			'data': data,
		}]

		body = json.dumps(events)
		url = self.upstream_uri + '/events/create'
		headers = {'content-type': 'application/json'}

		try:
			resp = requests.post(url, data = body, timeout = self.send_timeout, headers = headers)
		#http://docs.python-requests.org/en/latest/api/#exceptions
		#https://github.com/kennethreitz/requests/blob/master/requests/exceptions.py
		except requests.exceptions.RequestException as e:
			self.logger.warning("%s: %s" % (type(e), e))
			if reraise_on_error:
				self.logger.warning("reraising")
				raise
			else:
				return False

		if resp.status_code != requests.codes.ok:
			self.logger.warning("unexpected response status: %s" % resp.status_code)
			return False
		if resp.text != 'ok':
			self.logger.warning("unexpected response text:%s" % resp.text)
			return False

		return True

def _parse_args ():
	parser = argparse.ArgumentParser()
	parser.add_argument('upstream_uri', help = "e.g. http://localhost:6543")
	parser.add_argument('--inspect', action = 'store_true', help = "inspect queue and finish")
	parser.add_argument('--delete', type = int, help = "delete job by id and finish")
	return parser.parse_args()

def main ():
	args = _parse_args()

	_set_utc_timezone()

	logging.basicConfig(level = logging.INFO, format = '%(asctime)s %(levelname)-5s %(filename)s:%(funcName)s:%(lineno)d  %(message)s')
	#TODO review
	logging.getLogger("requests").setLevel(logging.WARNING) #http://stackoverflow.com/questions/11029717/how-do-i-disable-log-messages-from-the-requests-library
	logging.info("Starting, upstream: %s" % args.upstream_uri)

	q = QueueProcessor(upstream_uri = args.upstream_uri)
	if args.inspect:
		q.inspect()
	elif args.delete:
		q.delete(args.delete)
	else:
		for _e in q.run_loop():
			pass

def _set_utc_timezone ():
	os.environ['TZ'] = 'UTC'
	time.tzset()


if __name__ == '__main__':
	main()