from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement
from cassandra.policies import ConsistencyLevel
from random import Random
import time
import thread
import random

cluster = Cluster(
    ['192.168.100.2', '192.168.100.3', '192.168.100.4','192.168.100.6'],
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
insert_num = session.prepare("INSERT INTO num (num1, num2) VALUES (?, ?)")
data=[]
def write(n=1000000):
	batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
	for i in range(1,10000000) :

		num1=random.randint(1,100000)
		num2=random.randint(1,100000)
		batch.add(insert_num, (num1, num2))
		if (i%5000==0):
			session.execute(batch)
			batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
			print i
			print '\n'
start=time.time()
write()
end=time.time()
duration=end-start

print duration
