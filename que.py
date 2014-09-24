import beanstalkc
import cPickle

class QueueProcessor (object):
	def __init__ (self):
		port = 11300
		self.conn = beanstalkc.Connection(host='localhost', port=port)

		old = self.conn._socket.gettimeout()  #None
		self.conn._socket.settimeout(3)
		self.conn.watch('test_tube')
		self.conn._socket.settimeout(old) #TODO report? think about other commands

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
				yield cPickle.loads(job.body)

				#send to mill

				#job.release() #If we are not interested in a job anymore (e.g. after we failed to process it), we can simply release the job back to the tube 
				
				#"Once we are done with processing a job, we have to mark it as done, otherwise jobs are
				#re-queued by beanstalkd after a "time to run" (120 seconds, per default) is surpassed."
				job.delete()

				yield
		finally:
			self.conn.close()