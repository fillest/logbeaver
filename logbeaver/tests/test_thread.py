import logbeaver
import logbeaver.handler
import threading
import time


def rty (_):
	pass
def qwe ():
	# rty()
	raise Exception("test")

def main ():
	thr = threading.Thread(target = qwe)
	thr.daemon = True
	thr.start()

	time.sleep(0.5)

	logbeaver.handler.patch_everything()

	thr = threading.Thread(target = qwe, args = [123])
	# thr = threading.Thread(target = qwe, args = [])
	thr.daemon = True
	thr.start()

	time.sleep(0.5)


main()