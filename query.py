from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import time
cluster=Cluster(['192.168.100.2','192.168.100.3','192.168.100.4','192.168.100.6'],load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='US_EAST'),port=9042)

session=cluster.connect('my_keyspace')
start_time=time.time()
rows=session.execute('select * from num',timeout=1000000)
end_time=time.time()
duration=end_time-start_time
print duration
num=0;
for (n1,n2) in rows:
	print n1,n2
	num+=1
end_time2=time.time()
print end_time2-start_time
print num
