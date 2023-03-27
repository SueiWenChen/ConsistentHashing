# !pip install pymemcache
# !pip install faker
import random, faker
from pymemcache.client.base import Client
from pymemcache.client.murmur3 import murmur3_32
import os

class Ring:
    class Node:
        def __init__(self, name, ip, port):
            self.name = name
            self.ip = ip
            self.port = port
            self.client = Client((ip, port))
            self.keys = set() # keys of data stored in this node
        def __str__(self):
            return f'({self.name},{self.ip},{self.port})'
        def __repr__(self):
            return f'{self.name}, {self.keys}'
    
    def __init__(self, N):
        assert isinstance(N, int) and N>=1
        self.size = N # a ring of 0, 1, ..., N-1
        self.cluster = [] # sorted list of (location, node) tuple
        self.ports_in_use = set()
    def __repr__(self):
        return f'Ring of size {self.size} with nodes:'+'\n'+ ',\n'.join([str(n) for n in self.cluster])
    
    def get_hash(self, key):
        return murmur3_32(str(key)) % self.size
    
    def find_next_node(self, loc):
        # given a location on the ring, find the first node encountered counterclockwise
        # 0<=loc<=ring_size-1
        # returns the index of the next node in self.cluster
        # NOTE: it returns len(self.cluster) instead of 0 if loc is the largest on the ring
        assert len(self.cluster)>0, 'No node has been added to the ring'
        tempidx = 0 # index in the array representing the cluster
        while loc > self.cluster[tempidx][0]:
            tempidx += 1
            if tempidx==len(self.cluster): # reach the end of the cluster
                return tempidx # return the first node
        return tempidx
    
    def find_node(self, name):
        # return the index of a node in self.cluster with a certain name
        for idx, (loc, node) in enumerate(self.cluster):
            if node.name == name:
                return idx
        assert False, f'Error: no node with name {name}'

    def add_node(self, name, ip, port):
        assert port not in self.ports_in_use, f'Port {port} already in use'
        assert name not in {n[1].name for n in self.cluster}, f'Name "{name}" already exists'
        node = self.Node(name, ip, port) # node initialization
        self.ports_in_use.add(port)
        # choose somewhere on the ring to place the node
        loc = random.randint(0, self.size-1)
        occupied = {n[0] for n in self.cluster}
        while loc in occupied:
            loc = random.randint(0, self.size-1)
        # if no node has been added, just add the node to the ring at loc
        if len(self.cluster)==0:
            self.cluster.append((loc, node))
        # else, identify the next node and perform rehashing, replication factor=2
        else:
            idx = self.find_next_node(loc) # index of the next node in self.cluster
            self.cluster = self.cluster[:idx] + [(loc, node)] + self.cluster[idx:]
            # notations
            num_nodes = len(self.cluster) # number of nodes after addition
            h = lambda k: self.get_hash(k) # hash function
            l_n, n = loc, node # new node being added
            l_P, P = self.cluster[(idx-1)%num_nodes] # node prior to n
            l_N, N = self.cluster[(idx+1)%num_nodes] # next node from n
            _,  NN = self.cluster[(idx+2)%num_nodes] # next node from N
            # if the added node is the second one, copy all the data from the first node
            if num_nodes==2:
                n.keys |= P.keys
                for k in n.keys:
                    v = P.client.get(k)
                    n.client.set(k,v)
            # if the added node is the third one (P --> n --> N --> P):
            elif num_nodes==3:
                # 1) keys in (l_N,l_P] should be deleted from N and added to n
                N_to_P = {k for k in P.keys if h(k)<=l_P or h(k)>l_N} if l_P<l_N else \
                         {k for k in P.keys if l_N<h(k)<=l_P}
                for k in N_to_P:
                    N.client.delete(k)
                    v = P.client.get(k)
                    n.client.set(k,v)
                N.keys -= N_to_P
                n.keys |= N_to_P
                # 2) keys in (l_P,l_n] should be deleted from P and added to n
                P_to_n = {k for k in N.keys if h(k)<=l_n or h(k)>l_P} if l_n<l_P else \
                         {k for k in N.keys if l_P<h(k)<=l_n}
                for k in P_to_n:
                    P.client.delete(k)
                    v = N.client.get(k)
                    n.client.set(k,v)
                P.keys -= P_to_n
                n.keys |= P_to_n
            # if there are at least 3 nodes before adding the new one  (P-->n-->N-->NN)
            else:
                # 1) for data in both P and N, delete them from N and add them to n
                keysPN = P.keys.intersection(N.keys)
                for k in keysPN:
                    N.client.delete(k)
                    v = P.client.get(k)
                    n.client.set(k,v)
                N.keys -= keysPN
                n.keys |= keysPN
                # 2) for keys in (l_P, l_n], delete them from NN and add them to n
                P_to_n = {k for k in N.keys if h(k)<=l_n or h(k)>l_P} if l_n<l_P else \
                         {k for k in N.keys if l_P<h(k)<=l_n}
                for k in P_to_n:
                    NN.client.delete(k)
                    v = N.client.get(k)
                    n.client.set(k,v)
                NN.keys -= P_to_n
                n.keys  |= P_to_n
        print(f'Node {name} added')
        return

    def remove_node(self, name):
        idx = self.find_node(name)
        n = self.cluster[idx][1] # the node to be removed
        # data rehashing before deleting the node, replication_factor=2 
        num_nodes = len(self.cluster) # before removal
        P = self.cluster[(idx-1)%num_nodes][1] # node prior to n
        N = self.cluster[(idx+1)%num_nodes][1] # next node from n
        NN = self.cluster[(idx+2)%num_nodes][1] # next node from N
        
        if num_nodes>=3: # otherwise no need to do anything
            keysPn = P.keys.intersection(n.keys)
            keysNn = N.keys.intersection(n.keys)
            # 1) data in both P and n should be added to N and deleted from n
            for k in keysPn:
                v = P.client.get(k)
                N.client.set(k,v)
            N.keys |= keysPn
            # 2) data in both N and n should be added to NN and deleted from n
            for k in keysNn:
                v = N.client.get(k)
                NN.client.set(k,v)
            NN.keys |= keysNn
            
        n.client.close() # close the client
        self.cluster = self.cluster[:idx] + self.cluster[idx+1:] # remove from cluster
        self.ports_in_use.remove(n.port) # free the port

        print(f'Node {name} removed')
        return

    def dht_get(self, key):
        key = str(key)
        hash_key = self.get_hash(key) 
        idx = self.find_next_node(hash_key) 
        # node1, node2: where the key-value pair was stored
        node1 = self.cluster[idx%len(self.cluster)][1]
        node2 = self.cluster[(idx+1)%len(self.cluster)][1]
        # if key not in node1.keys:
        #    raise Exception(f'Key "{key}" not found')
        try:
            value = node1.client.get(key)
        except: # in case when node1 is down
            value = node2.client.get(key)
        return value

    def dht_set(self, key, value):
        # replication factor is set to 2
        key, value = str(key),str(value)
        hash_key = self.get_hash(key) 
        idx = self.find_next_node(hash_key) 
        # node1, node2: where to store the key-value pair
        node1 = self.cluster[idx%len(self.cluster)][1]
        node2 = self.cluster[(idx+1)%len(self.cluster)][1] 
        node1.client.set(key, value)
        node2.client.set(key, value)
        node1.keys.add(key)
        node2.keys.add(key)
        return node1, node2

    # Helper functions
    def read_list_func(self, rlist):
        # Read the values of the sampled keys from your DHT
        for key in rlist:
            value = self.dht_get(key)
            print(f"Key: {key}, Value: {value}")

    def add_fake_data(self, key_from_inclusive, key_to_exlusive):
        # N = how many pairs to add
        keyrange = range(key_from_inclusive, key_to_exlusive)
        print(f"Loading {len(keyrange)} fake data points")
        fake = faker.Faker()
        for key in keyrange:
            value = fake.company()
            self.dht_set(key, value)

    def test(self):
        # test if all keys are stored in the correct two consecutive nodes with the same value
        num_nodes = len(self.cluster)
        if num_nodes>1:
            node_locations = [n[0] for n in self.cluster] # node locations on the ring
            all_keys = [n[1].keys for n in self.cluster]
            # dict from key to the index of the node where it resides
            keyToNodeIdx = {kk:[] for kk in [k for ks in all_keys for k in ks]}
            for idx, k_list in enumerate(all_keys):
                for k in k_list:
                    keyToNodeIdx[k].append(idx)
            for k, indices in keyToNodeIdx.items():
                assert len(indices)==2, f'More than two nodes with key {k}'
                # make sure i0 and i1 are adjacent
                i0,i1 = indices[0], indices[1]
                if i0*i1: # if both are nonzero
                    assert (i0-i1)**2==1
                else:
                    assert i0+i1==1 or i0+i1==len(self.cluster)-1
                # make sure the key is hashed to the right location
                if num_nodes>2:
                    keyhash = self.get_hash(k)
                    if {i0,i1}=={0,num_nodes-1}: # if crossing 0 on the ring
                        assert self.cluster[-2][0]<keyhash<=self.cluster[-1][0], f'Key {k}:{keyhash} misplaced'
                    elif i0*i1==0: # {i0,i1}=={0,1}
                        assert keyhash<=self.cluster[0][0] or keyhash>self.cluster[-1][0] , f'Key {k}:{keyhash} misplaced'
                    else:
                        i = min(i0,i1)
                        assert self.cluster[i-1][0]<keyhash<=self.cluster[i][0], f'Key {k}:{keyhash} misplaced'
                # make sure the values are the same
                v0 = self.cluster[i0][1].client.get(k)
                v1 = self.cluster[i1][1].client.get(k)
                assert v0==v1, f'Inconsistent values for key {k}'
        print('Test passed ...')

def system_test():
    print("Initializing the DHT Cluster:")
    ring = Ring(100)
    # add m1
    ring.add_node('m1','localhost', 11211)
    ring.add_fake_data(0,10)
    ring.test()
    # add m2
    ring.add_node('m2','localhost', 11212)
    ring.add_fake_data(10,20)
    ring.test()
    # add m3
    ring.add_node('m3','localhost', 11213)
    ring.add_fake_data(20,30)
    ring.test()
    # add m4
    ring.add_node('m4','localhost', 11214)
    ring.add_fake_data(30,40)
    ring.test()
    # remove m1
    ring.remove_node('m1')
    ring.test()
    # add m5
    ring.add_node('m5','localhost', 11215)
    ring.add_fake_data(40,50)
    ring.test()
    # remove m2
    ring.remove_node('m2')
    ring.test()  
    # remove m3
    ring.remove_node('m3')
    ring.test()
    # add back m1
    ring.add_node('m1','localhost', 11211)
    ring.add_fake_data(50,100)
    ring.test()
    # add back m2
    ring.add_node('m2','localhost', 11212)
    ring.add_fake_data(100,150)
    ring.test()    
    # remove m4
    ring.remove_node('m4')
    ring.test()
    # remove m5
    ring.remove_node('m5')
    ring.test()
    # remove m1
    ring.remove_node('m1')
    ring.test()
    # remove m2
    ring.remove_node('m2')
    ring.test()

def main():
    print('My Memcached DHT.')
    instructions = '''Commands (variables in angle brackets are assumed to be strings, no need to put quotations)\n1. add_node<node_name,ip,port>\n2. remove_node<node_name>\n3. get<key>\n4. put<key,value>\n5. display\n6. system_test\n7. clear\n8. menu\n9. quit'''
    while True:
        ring_size = input("Please specify the ring size: ")
        try:
            ring = Ring(int(ring_size))
            break
        except:
            pass
    print(instructions)
    while True:
        command = input('command: ').lower()
        if command=='quit':
            break
        elif command=='system_test':
            print('Running system test ...')
            system_test()
            continue
        elif command=='clear':
            os.system('clear')
            continue
        elif command=='menu':
            print(instructions)
            continue
        elif command=='display':
            print(ring)
            continue            
       	try:
            cmd = command[:command.index("<")]
            var = command[command.index('<')+1:command.index('>')]
            if cmd=='add_node':
                name, ip, port = var.split(',')
                ring.add_node(name, ip, port)
            elif cmd=='remove_node':
                ring.remove_node(var)
            elif cmd=='get':
                value = ring.dht_get(var)
                print(f'The value for key {var} is {value}')
            elif cmd=='put':
                key, value = var.split(',')
                n1, n2 = ring.dht_set(key, value)
                print(f'Pair ({var}) set in nodes {n1.name} and {n2.name}.')
            else:
                print('Command not found')
        except AssertionError as msg:
            print(msg)
        except:
            print('Invalid input')

if __name__ == "__main__":
	main()