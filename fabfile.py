from fabric.api import *
import time
import glob
from contextlib import contextmanager


@task
def install_bean ():
	#it's named not just 'beanstalkd' because beanstalkd is ran with full fsync, it's global and can conflict with generic setup
	sudo('adduser --system --no-create-home --disabled-login --disabled-password --group logbeaver_beanstalkd || true')

	try:
		with cd('/tmp'):
			run('wget https://github.com/kr/beanstalkd/archive/v1.10.tar.gz')
			run('tar -zxf v1.10.tar.gz')
			sudo('mv beanstalkd-1.10 /opt/logbeaver_beanstalkd')
	finally:
		run('rm /tmp/v1.10.tar.gz')
	sudo('mkdir /opt/logbeaver_beanstalkd/wal')
	sudo('chown -R logbeaver_beanstalkd:logbeaver_beanstalkd /opt/logbeaver_beanstalkd')
	with cd('/opt/logbeaver_beanstalkd'):
		sudo('make -j 4', user = 'logbeaver_beanstalkd')

	put('beanstalkd.supervisor.conf', '/etc/supervisor/conf.d/logbeaver_beanstalkd.conf', use_sudo = True)
	sudo('supervisorctl update')
	# sudo('supervisorctl start')
	time.sleep(10)
	with settings(warn_only = True):
		res = sudo('supervisorctl status logbeaver_beanstalkd | grep RUNNING')
		if res.failed:
			sudo('supervisorctl tail logbeaver_beanstalkd')
			abort('failed to start logbeaver_beanstalkd')

@task
def delete_bean ():
	with settings(warn_only = True):
		_res = sudo('supervisorctl stop logbeaver_beanstalkd')
	sudo('rm -r /opt/logbeaver_beanstalkd')
	sudo('rm /etc/supervisor/conf.d/logbeaver_beanstalkd.conf')
	sudo('supervisorctl update')


@contextmanager
def _upload_sdist ():
	[fpath] = glob.glob('dist/*.gz')
	[remote] = put(fpath, '/tmp/')
	try:
		yield remote
	finally:
		sudo('rm ' + remote)

@task
def upload_queproc ():
	sdist()
	with _upload_sdist() as remote:
		#TODO improve
		sudo('rm -rf /opt/logbeaver_queproc/src/*')
		sudo('mkdir -p /opt/logbeaver_queproc/src', user = 'logbeaver_queproc') #TODO ? tar doesn't create the dir
		sudo('tar xzf %s -C /opt/logbeaver_queproc/src --strip-components=1' % remote, user = 'logbeaver_queproc')

@task
def upgrade_queproc ():
	with cd('/opt/logbeaver_queproc/src'):
		sudo('../venv/bin/pip install -r logbeaver/requirements_queproc.txt')
		sudo('../venv/bin/pip install -e .')

@task
def deploy_queproc ():
	sudo('supervisorctl stop logbeaver_queproc')
	execute(upload_queproc)
	execute(upgrade_queproc)
	sudo('supervisorctl update')
	sudo('supervisorctl start logbeaver_queproc')
	execute(check_running_queproc)

@task
def check_running_queproc ():
	with settings(warn_only = True):
		res = sudo('supervisorctl status logbeaver_queproc | grep RUNNING')
		if res.failed:
			sudo('supervisorctl tail logbeaver_queproc')
			abort('failed to start logbeaver_queproc')

@task
def install_queproc ():
	sudo('adduser --system --no-create-home --disabled-login --disabled-password --group logbeaver_queproc || true')

	sudo('mkdir -p /opt/logbeaver_queproc/src')
	execute(sdist)
	with _upload_sdist() as remote:
		sudo('tar xzf %s -C /opt/logbeaver_queproc/src --strip-components=1' % remote)

	sudo('virtualenv /opt/logbeaver_queproc/venv')
	execute(upgrade_queproc)

	sudo('chown -R logbeaver_queproc:logbeaver_queproc /opt/logbeaver_queproc')

	sudo('ln -s /opt/logbeaver_queproc/src/logbeaver_queproc.supervisor.conf /etc/supervisor/conf.d/logbeaver_queproc.conf')

	sudo('supervisorctl update')
	time.sleep(10)
	execute(check_running_queproc)

@task
def delete_queproc ():
	with settings(warn_only = True):
		_res = sudo('supervisorctl stop logbeaver_queproc')
	sudo('rm -rf /opt/logbeaver_queproc')
	sudo('rm -f /etc/supervisor/conf.d/logbeaver_queproc.conf')
	sudo('supervisorctl update')

@task
def sdist ():
	local('rm -rf dist/')
	local('python setup.py --quiet sdist')

@hosts(env.get('package_host') or [])
@task()
def package ():
	assert env.get('package_host')
	execute(sdist)
	put('dist/logbeaver-*.tar.gz', '~/packages')