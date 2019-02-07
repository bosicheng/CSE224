# Bosi Cheng
# A53271697
# Zan Deng
# A53285138

import os
import sys
import rpyc
import random
import logging
import threading
'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''


class RaftNode(rpyc.Service):
	"""
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
	"""
	def __init__(self, config, index):
		# heartbeat flag
		self.hb_flag=False
		# timer flag
		self.flag=False
		# which term
		self.term=0
		# current node number
		self.cur_num=0
		# vote to which node
		self.vote=""
		# self state
		self.state="follower"
		# the index of this node
		self.index="node"+str(index)
		# Debug Logging
		self.log=self.log_setting()
		self.lock=threading.Lock()
		# the dict of the connection of the nodes
		self.conns={}
		# the dict of the connection information of the nodes
		self.config_dict=self.read_info(config)
		self.start()

	# setting the log
	# Debug Logging
	def log_setting(self):
		handler=logging.StreamHandler()
		log=logging.Logger(self.index)
		log.addHandler(handler)
		return log

	# read the log
	def read_record(self):
		record = {}
		# check the persistent txt in /tmp
		if not os.path.isfile("/tmp/cbsdz10_"+str(self.index)+"_persistent.txt"):
			return None
		# open the txt and read the record
		f=open("/tmp/cbsdz15_"+str(self.index)+"_persistent.txt","r")
		for line in f.readlines():
			if len(line.strip().split(":")) == 1:
				record[line.strip().split(":")[0]] = ""
			else:
				record[line.strip().split(":")[0]] = line.strip().split(":")[1].strip()
		return record

	def read_info(self, config):
		config_dict = {}
		f=open(config)
		for line in f.readlines():
			if line.startswith("N"):
				self.sum = int(line.strip().split(": ")[1])
			else:
				host,port = line.strip().split(": ")[1].split(":")
				config_dict[line.strip().split(": ")[0]] = (host, port)
		return config_dict

	def start(self):
		record = self.read_record()
		if record is None:
			self.set_election_timer()
		else:
			self.state = record['state']
			self.cur_num = int(record['cur_num'])
			self.term = int(record['term'])
			self.vote = record['vote']
			if record['state'] == "leader":
				self.set_hb()
			else:
				self.set_election_timer()

	# set a election_timer with threading.Timer
	def set_election_timer(self):
		self.timer = threading.Timer(random.randint(1.7 * 10000, 2.6 * 10000) * 1.0 / 10000, self.become_candidate)
		self.flag=True
		self.timer.start()

	# setting a node to "follower"
	def become_follower(self, term, if_persist):
		with self.lock:
			self.state = "follower"
			self.vote = ""
			self.term = term
			self.cur_num = 0
			if if_persist:
				self.persist()		
		# if we have an election_timer we reset it
		if self.flag:
			self.timer.cancel()
			self.flag=False
		self.set_election_timer()
		# if the node was "leader" and sending heartbeat we cancel it 
		if self.hb_flag:
			self.heartbeat_timer.cancel()
			self.hb_flag=False

	# setting a node to "candidate"
	def become_candidate(self):
		self.log.info( "in term"+str(self.term)+" "+str(self.index)+" becomes candidate")
		with self.lock:
			# lock the thread and add the term by 1
			self.term += 1
			self.cur_num = 1
			self.state = "candidate"
			self.vote = self.index
			self.persist()
		# if we have an election_timer we reset it
		if self.flag:
			self.timer.cancel()
			self.flag=False
		self.set_election_timer()
		# connect all the rest of nodes and asking for votes
		for node in self.config_dict.keys():
			if node == self.index:
				continue
			if node not in self.conns.keys():
				self.conns[node] = rpyc.connect(self.config_dict[node][0], self.config_dict[node][1])
			threading.Thread(target=self.helper, args=[node]).start()

	def helper(self, node):
		self.log.info("asking vote from "+str(node))
		term, vote_granted = self.conns[node].root.request_vote(self.term, self.index)
		if term < self.term:
			return
		elif term > self.term:
			self.become_follower(term,True)
		else:
			if vote_granted:
				with self.lock:
					self.cur_num += 1
					self.persist()
				# setting the node to "leader"
				if self.state != "leader" and self.cur_num >= int(self.sum / 2 + 1):
					self.log.info(str(self.index)+" becomes leader")
				if self.flag:
					self.timer.cancel()
					self.flag=False
				with self.lock:
					self.state = "leader"
					self.persist()
				# sending heartbeats
				self.set_hb()

	def set_hb(self):
		for node in self.config_dict:
			if node == self.index:
				continue
			if node not in self.conns:
				self.conns[node] = rpyc.connect(self.config_dict[node][0], self.config_dict[node][1])
			threading.Thread(target=self.set_hb_helper, args=[node]).start()
		self.heartbeat_timer = threading.Timer(0.7, self.set_hb)
		self.heartbeat_timer.start()
		self.hb_flag=True

	def set_hb_helper(self, node):
		term, _ = self.conns[node].root.append_entries(self.term)
		if term > self.term:
			self.become_follower(term,True)

	# check the term and send appendEntries
	def exposed_append_entries(self, term):
		if term < self.term:
			return self.term
		elif term > self.term:
			self.become_follower(term,True)
			return self.term
		if self.state == "follower":
			if self.flag:
				self.timer.cancel()
				self.flag=False
			self.set_election_timer()
		else:
			self.become_follower(term,True)
		return self.term

	# returns its term and whether it votes for the node
	def exposed_request_vote(self, term, index):
		if self.state == "leader":
			return self.term, False
		if term < self.term or (term == self.term and self.vote != index):
			return self.term, False
		if term == self.term and self.vote == index:
			return self.term, True
		if term > self.term:
			self.become_follower(term, False)
		with self.lock:
			self.vote = index
			self.persist()
		return self.term, True

	'''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, addi
        ng the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''
	def exposed_is_leader(self):
		if self.state == "leader":
			return True
		else:
			return False

	# Persistent storage
	def persist(self):
		note="node:"+self.index+"\nstate:"+self.state+"\nvote:"+self.state+"\nterm:"+str(self.term)+"\ncur_num:"+str(self.cur_num)
		f=open("/tmp/cbsdz11_"+str(self.index)+"_persistent.txt", 'w')
		f.write(note)
		f.flush()
		os.fsync(f.fileno())

if __name__ == '__main__':
	from rpyc.utils.server import ThreadedServer
	server = ThreadedServer(RaftNode(sys.argv[1], sys.argv[2]), port = int(sys.argv[3]))
	server.start()