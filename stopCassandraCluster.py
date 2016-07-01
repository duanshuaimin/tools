from paramiko.client import SSHClient
from paramiko.client import AutoAddPolicy
client=SSHClient()
autoAddpolicy=AutoAddPolicy()
client.set_missing_host_key_policy(autoAddpolicy)
client.load_system_host_keys()
hosts=['192.168.100.2','192.168.100.3','192.168.100.4','192.168.100.6']
for host in hosts:
	client.connect(host,port=22,username='root',password='Luotuocao2008',look_for_keys=False, allow_agent=False)
	stdin, stdout, stderr = client.exec_command('ps aux|grep cassandra|awk "{print $2}"|xargs kill -9')
