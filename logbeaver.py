#!/usr/bin/env python
#coding: utf-8
from sys import stdin
import re
import socket
from urlparse import urlsplit, parse_qs
import argparse
import logging
import threading
import Queue


def main ():
	parser = argparse.ArgumentParser()
	parser.add_argument('--host', default = '127.0.0.1')
	parser.add_argument('--port', default = 8125, type = int)
	args = parser.parse_args()

	logging.basicConfig(level = logging.INFO, format = '%(asctime)s %(levelname)-5s %(filename)s:%(funcName)s:%(lineno)d  %(message)s')
	log = logging.getLogger(__name__)

	local_fqdn = socket.getfqdn()
	log.info('Starting. Using fqdn: %s' % local_fqdn)

	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

	send_que = Queue.Queue()
	def send_loop ():
		while True:
			data = send_que.get()
			sock.sendto(data, (args.host, args.port))
	thr = threading.Thread(target = send_loop)
	thr.daemon = True #TODO "If you want your threads to stop gracefully, make them non-daemonic and use a suitable signalling mechanism such as an Event."
	thr.start()

	# log_format for_logstash '[$time_local] $request_time $upstream_response_time $upstream_cache_status $status'
	#                                 ' "$request" $remote_addr "$http_referer" "$http_user_agent"';
	line_regexp = re.compile(r'\[.+\] ([^ ]+) ([^ ]+) [^ ]+ ([^ ]+) "([^ ]+) ([^ ]+)')

	dispatch_re = re.compile(r'/dispatch/[A-Z\d]+')
	stat_re = re.compile(r'/stat/?$')

	while True:
		line = stdin.readline()
		if not line:
			break

		try:
			match = line_regexp.match(line)
			request_time, upstream_response_time, status, verb, url = match.groups()

			assert request_time != '-', line
			batch_data = [
				"logstash.%s.nginx_access.response.%s:1|c" % (local_fqdn, status),
				"logstash.%s.nginx_access.request_time:%s|ms" % (local_fqdn, request_time),
			]

			if upstream_response_time != '-':
				batch_data.append("logstash.%s.nginx_access.upstream_response_time:%s|ms" % (local_fqdn, upstream_response_time))

			url_parts = urlsplit(url)
			keep_empty = True
			strict_parse = False  #TODO with strict parsing fails on /com/site/ACF042EDF74FB312392E4BB059A9E8C0AF0EAE9AA28ED6D7?0.7257665740326047
			query = parse_qs(url_parts.query, keep_empty, strict_parse) if url_parts.query else None

			match = dispatch_re.match(url_parts.path)
			if match:
				if query:
					#TODO refactor
					v = query.get('version')
					if v: v = v[0]
					v = v or 'not_set'
					if v: int(v) #TODO proper checks/escape
					
					d = query.get('dispatcher')
					if d: d = d[0]
					d = d or 'not_set'
					if d: int(d) #TODO
				else:
					v = d = 'not_set'

				batch_data.append("logstash.%s.nginx_access.dispatch_ver.dispatcher.%s:1|c" % (local_fqdn, d))
				batch_data.append("logstash.%s.nginx_access.dispatch_ver.version.%s:1|c" % (local_fqdn, v))

			else:
				match = stat_re.match(url_parts.path)
				if match:
					if upstream_response_time != '-':
						batch_data.append("logstash.%s.nginx_access_bapi_stat.response.%s:1|c" % (local_fqdn, status))
						batch_data.append("logstash.%s.nginx_access_bapi_stat.upstream_response_time:%s|ms" % (local_fqdn, upstream_response_time))
					else:
						log.warning("upstream_response_time == '-': %s" % repr(line))


			data = "\n".join(batch_data)
			# print len(data) #TODO https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets
			
			send_que.put(data)
		except:
			log.error("error while processing line: %s" % repr(line))
			raise


if __name__ == '__main__':
	main()
