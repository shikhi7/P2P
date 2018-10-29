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
        self.finger = [nxt]
        self.prev = prev
        self.data = dict()

    def getKey(self, ip, port):
        # msg = str(ip) + str(port)
        # return long(hashlib.sha1(msg).hexdigest(), 16)
        return randint(0, 1024)

    def updateFinger(self, dht):
        del self.finger[1:]
        for i in range(1, dht.k):
            self.finger.append(dht.findNode(dht.startNode, self.ID + 2 ** i))

class OverlayDHT:
    def __init__(self, k):
        self.k = k
        self.size = 2 ** k
        self.startNode = Node(DEF_IP, DEF_PORT)
        self.startNode.finger[0] = self.startNode
        self.startNode.prev = self.startNode
        self.startNode.updateFinger(self)

    def distance(self, n1, n2):
        if n1 == n2:
            return 0
        elif n1 < n2:
            return n2 - n1
        else:
            return self.size - n1 + n2

    def printHops(self, hopCount, hopIDs, key):
        order = "->".join(hopIDs)
        print("Hops taken to search " + str(key) + " are " + str(hopCount) + " and in the order " + order)

    def findNode(self, start, key):
        properKey = key % self.size
        curr = start
        hopCount = 0
        hopIDs = [str(start.ID)]

        while True:
            if curr.ID == properKey:
                self.printHops(hopCount, hopIDs, properKey)
                return curr
            if self.distance(curr.ID, properKey) <= self.distance(curr.finger[0].ID, properKey):
                self.printHops(hopCount, hopIDs, properKey)
                return curr.finger[0]

            nextNode = curr.finger[-1]
            for i in range(0, len(curr.finger)-1):
                if self.distance(curr.finger[i].ID, properKey) < self.distance(curr.finger[i+1].ID, properKey):
                    nextNode = curr.finger[i]
                    break

            curr = nextNode
            hopIDs.append(str(curr.ID))
            hopCount+=1

    def joinNetwork(self, newNode):
        nextToNewNode = self.findNode(self.startNode, newNode.ID)

        if nextToNewNode.ID == newNode.ID:
            print("Duplicate ID found!")
            return

        for key in nextToNewNode.data:
            properKey = key % self.size
            if self.distance(properKey, newNode.ID) < self.distance(properKey, nextToNewNode.ID):
                newNode.data[key] = nextToNewNode.data[key]

        for key in list(nextToNewNode.data.keys()):
            if self.distance(properKey, newNode.ID) < self.distance(properKey, nextToNewNode.ID):
                del nextToNewNode.data[key]

        prevNode = nextToNewNode.prev
        newNode.prev = prevNode
        newNode.finger[0] = nextToNewNode
        nextToNewNode.prev = newNode
        prevNode.finger[0] = newNode

        newNode.updateFinger(self)

    def leaveNetwork(self, node):
        succNode = node.finger[0]
        for k, v in node.data.items():
            succNode.data[k] = v
        if node.finger[0] == node:
            self.startNode = None
        else:
            node.prev.finger[0] = node.finger[0]
            node.finger[0] = prev = node.prev
            if self.startNode == node:
                self.startNode = node.finger[0]

    def updateAllFingerTables(self):
        self.startNode.updateFinger(self)
        curr = self.startNode.finger[0]
        while curr != self.startNode:
            curr.updateFinger(self)
            curr = curr.finger[0]

    def getContent(self, start, key):
        lookupNode = self.findNode(start, key)
        if key in lookupNode.data:
            return lookupNode.data[key]
        else:
            return None

    def putContent(self, start, key, value):
        storeNode = self.findNode(start, key)
        storeNode.data[key] = value

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
        dht.joinNetwork(Node(myIP, myPort))

    dht.updateAllFingerTables();

    for i in range(5, 1024, 10):
        dht.putContent(dht.startNode, i, "content" + str(i))

    for i in range(5, 800, 40):
        print(dht.getContent(dht.startNode, i))
