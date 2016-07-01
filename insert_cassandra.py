from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement
from cassandra.policies import ConsistencyLevel
from random import Random
import time
import thread

cluster = Cluster(
    ['192.168.100.2', '192.168.100.3', '192.168.100.4'],
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='US_EAST'),
    port=9042)


def random_str(randomlength=8):
	str = ''
	chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
	length = len(chars) - 1
	random = Random()
	for i in range(randomlength):
		str+=chars[random.randint(0, length)]
	return str


session = cluster.connect('my_keyspace')
insert_user = session.prepare("INSERT INTO user (first_name, last_name) VALUES (?, ?)")

def write(n=1000000):
	batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
	for i in range(1,n) :

		last_name=random_str(25)
		first_name=random_str(25)
		batch.add(insert_user, (first_name, last_name))
		if (i%1000==0):
			session.execute(batch)
			batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
			print i
			print '\n'
start=time.time()
for j in range(1,10):
	thread.start_new_thread(write())
	
end=time.time()
duration=end-start

print duration
