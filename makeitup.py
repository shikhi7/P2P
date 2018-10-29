import sys
import random
import socket
import sha
import hashlib
from threading import Lock
from threading import Thread
from random import randint

# fingerLock = Lock()

DEF_IP = "127.0.0.1"
DEF_PORT = 60221
# HASH_BITS = 160
HASH_BITS = 10

class Node:
    def __init__(self, ip, port, nxt = None, prev = None):
        self.ip = ip
        self.port = port
        self.ID = self.getKey(ip, port)
        self.fingerTable = [nxt]
        self.prev = prev
        self.data = dict()

    def getKey(self, ip, port):
        # msg = str(ip) + str(port)
        # return long(hashlib.sha1(msg).hexdigest(), 16)
        return randint(0, 1024)

    def updateFingerTable(self, dht):
        del self.fingerTable[1:]
        for i in range(1, dht.k):
            self.fingerTable.append(dht.findNode(dht.startNode, self.ID + 2 ** i))

class OverlayDHT:
    def __init__(self, k):
        self.k = k
        self.size = 2 ** k
        self.startNode = Node(DEF_IP, DEF_PORT)
        self.startNode.fingerTable[0] = self.startNode
        self.startNode.prev = self.startNode
        self.startNode.updateFingerTable(self)

    def getHashId(self, key):
        return key % self.size

    def distance(self, n1, n2):
        if n1 == n2:
            return 0
        if n1 < n2:
            return n2 - n1
        return self.size - n1 + n2

    def findNode(self, start, key):
        hashId = self.getHashId(key)
        curr = start
        numJumps = 0
        while True:
            if curr.ID == hashId:
                print("number of jumps: ", numJumps)
                return curr
            if self.distance(curr.ID, hashId) <= self.distance(curr.fingerTable[0].ID, hashId):
                print("number of jumps: ", numJumps)
                return curr.fingerTable[0]
            tabSize = len(curr.fingerTable)
            i = 0;
            nextNode = curr.fingerTable[-1]
            while i < tabSize - 1:
                if self.distance(curr.fingerTable[i].ID, hashId) < self.distance(curr.fingerTable[i + 1].ID, hashId):
                    nextNode = curr.fingerTable[i]
                i = i + 1
            curr = nextNode
            numJumps += 1

    def join(self, newNode):
        # Find the node before which the new node should be inserted
        origNode = self.findNode(self.startNode, newNode.ID)

        # If there is a node with the same id, decline the join request for now
        if origNode.ID == newNode.ID:
            print("There is already a node with the same id!")
            return

        # Copy the key-value pairs that will belong to the new node after
        # the node is inserted in the system
        for key in origNode.data:
            hashId = self.getHashId(key)
            if self.distance(hashId, newNode.ID) < self.distance(hashId, origNode.ID):
                newNode.data[key] = origNode.data[key]

        # Update the prev and next pointers
        prevNode = origNode.prev
        newNode.fingerTable[0] = origNode
        newNode.prev = prevNode
        origNode.prev = newNode
        prevNode.fingerTable[0] = newNode

        # Set up finger table of the new node
        newNode.updateFingerTable(self)

        # Delete keys that have been moved to new node
        for key in list(origNode.data.keys()):
            hashId = self.getHashId(key)
            if self.distance(hashId, newNode.ID) < self.distance(hashId, origNode.ID):
                del origNode.data[key]

    # def leave(self, node):
    #     # Copy all its key-value pairs to its successor in the system
    #     for k, v in node.data.items():
    #         node.fingerTable[0].data[k] = v
    #     # If this node is the only node in the system.
    #     if node.fingerTable[0] == node:
    #         self._startNode = None
    #     else:
    #         node.prev.fingerTable[0] = node.fingerTable[0]
    #         node.fingerTable[0] = prev = node.prev
    #         # If this deleted node was an entry point to the system, we
    #         # need to choose another entry point. Simply choose its successor
    #         if self._startNode == node:
    #             self._startNode = node.fingerTable[0]

    def updateAllFingerTables(self):
        self.startNode.updateFingerTable(self)
        curr = self.startNode.fingerTable[0]
        while curr != self.startNode:
            curr.updateFingerTable(self)
            curr = curr.fingerTable[0]

    # Look up a key in the DHT
    def lookup(self, start, key):
        nodeForKey = self.findNode(start, key)
        if key in nodeForKey.data:
            # print("The key is in node: ", nodeForKey.ID)
            return nodeForKey.data[key]
        return None

    # Store a key-value pair in the DHT
    def store(self, start, key, value):
        nodeForKey = self.findNode(start, key)
        nodeForKey.data[key] = value

if __name__ == "__main__":
    myIP = socket.gethostbyname(socket.gethostname())
    myPort = 0
    if (len(sys.argv) < 2):
        print("Too few arguments")
        sys.exit()
    elif (len(sys.argv) > 2):
        print("Too many arguments")
        sys.exit()
    else:
        myPort = int(sys.argv[1])

    dht = OverlayDHT(HASH_BITS)
    # myNode = Node(myIP, myPort)
    # dht.join(myNode)
    for i in range(120):
        dht.join(Node(myIP, myPort))

    dht.updateAllFingerTables();

    for i in range(5, 1024, 10):
        dht.store(dht.startNode, i, "hello" + str(i))

    for i in range(5, 200, 10):
        print(dht.lookup(dht.startNode, i))
