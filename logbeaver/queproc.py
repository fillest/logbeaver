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


log = logging.getLogger(__name__)


class QueueProcessor (object):
	def __init__ (self, upstream_uri = 'http://localhost:6543', bean_tube = 'logbeaver',
			bean_host = 'localhost', bean_port = 11301, bean_conn_timeout = 10, send_timeout = 60):
		self.send_timeout = send_timeout
		self.upstream_uri = upstream_uri

		self._connect(bean_host, bean_port, bean_conn_timeout, bean_tube)

	def _connect (self, bean_host, bean_port, bean_conn_timeout, bean_tube):
		self.conn = beanstalkc.Connection(host = bean_host, port = bean_port, connect_timeout = bean_conn_timeout)

		old = self.conn._socket.gettimeout()  #normally None
		self.conn._socket.settimeout(3) #TODO report? think about other commands
		self.conn.watch(bean_tube)
		self.conn._socket.settimeout(old)

	def clear_queue (self):
		num = 0
		while True:
			job = self.conn.reserve(timeout = 0)
			if not job:
				return num
			job.delete()
			num += 1

	def run_loop (self, timeout = None):
		try:
			while True:
				job = self.conn.reserve(timeout = timeout)
				assert job
				# print job, job.jid
				try:
					data = cPickle.loads(job.body)

					#TODO how to bulk-get-send? - bury?
					if not self._send(data):
						job.release()
						time.sleep(3)
						#TODO touch and try send again?
						continue
				except:
					job.release()
					raise
				else:
					#"Once we are done with processing a job, we have to mark it as done, otherwise jobs are
					#re-queued by beanstalkd after a "time to run" (120 seconds, per default) is surpassed."
					job.delete()

				yield data
		finally:
			self.conn.close()

	def _send (self, data):
		events = [{
			'source': data['source'],
			#TODO use logger time + google "python logging utc" + or hard-set utc for app?
			'time': calendar.timegm(time.gmtime()), #TODO drops milliseconds, does pg support them anyway?
			'host': socket.getfqdn(), #TODO cache?
			'data': data,
		}]
		body = json.dumps(events)
		url = self.upstream_uri + '/create' #TODO rename to events/create?
		#TODO compression?
		resp = requests.post(url, data = body, timeout = self.send_timeout, headers = {'content-type': 'application/json'})
		#requests.exceptions.ConnectionError
		#requests.exceptions.HTTPError
		if resp.status_code != requests.codes.ok:
			log.warning("fail %s" % resp.status_code)
			return False
		if resp.text != 'ok':
			log.warning("fail %s" % resp.text)
			return False

		return True

def _parse_args ():
	parser = argparse.ArgumentParser()
	parser.add_argument('upstream_uri', help = "http://localhost:6543")
	# parser.add_argument('-b', '--executable-path', default = 'build/dev/rtmp_load', help = "rtmp_load executable path")
	# parser.add_argument('-sh', '--stat-host', default = '10.40.25.155:7778', help = "stat server host[:port]")
	return parser.parse_args()

def main ():
	args = _parse_args()

	_set_utc_timezone()

	logging.basicConfig(level = logging.INFO, format = '%(asctime)s %(levelname)-5s %(filename)s:%(funcName)s:%(lineno)d  %(message)s')

	q = QueueProcessor(source = args.source, upstream_uri = args.upstream_uri)
	for _e in q.run_loop():
		pass

def _set_utc_timezone ():
	os.environ['TZ'] = 'UTC'
	time.tzset()


if __name__ == '__main__':
	main()