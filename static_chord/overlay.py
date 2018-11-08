import sys
import socket
from hashlib import sha1
from random import randint
import threading

HASH_BITS = 160
LOGICAL_SIZE = 2**HASH_BITS
nodeIP = "127.0.0.1"
nodePort = 4343
sep = "-"*30 + "\n"
sep2 = "-"*30

def getKey(ip, port):
    raw = str(ip) + str(port)
    key = int(sha1(raw.encode('utf-8')).hexdigest(), 16)
    return key

class Node(threading.Thread):
    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port

        self.id = getKey(ip, port)
        self.predecessor = None
        self.fingerTable = {}
        self.dataTable = {}

        self.sock = None

    def updateFingerTable(self):
        for i in range(HASH_BITS):
            self.fingerTable[i] = [self.id, self.ip, self.port]
            f = False
            for j in allNodes:
                if j[0] >= (self.id + 2**i) % LOGICAL_SIZE:
                    self.fingerTable[i] = j
                    f = True
                    break

            if not f:
                self.fingerTable[i] = allNodes[0]

    def between(self, n1, n2, n3):
        ## if n1 is in between n2 and n3
        if n2 < n3:
            return (n2 < n1 < n3)
        else:
            return (n1 < n3)

    def findNode(self, key):
        key = key % LOGICAL_SIZE

        if self.id == key:
            self.printHops(False)
            return

        nextHop = [self.id, self.ip, self.port]
        for i in range(HASH_BITS):
            if self.between(key, self.id, self.fingerTable[i][0]):
                break
            else:
                nextHop = self.fingerTable[i]

        if nextHop[0] == self.id:
            self.printHops(False)
            return
        else:
            self.printHops()
            next_ip = nextHop[1]
            next_port = nextHop[2]
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((next_ip, next_port))
            newsock.sendall(b'findNode ' + str(key).encode())
            newsock.close()

    def getMsgKey(self, msg):
        key = int(sha1(msg.encode('utf-8')).hexdigest(), 16)
        return key

    def keyPresent(self, key):
        return (key in self.dataTable)

    def getContent(self, key):
        key = key % LOGICAL_SIZE

        if self.id == key:
            self.printHops(False)
            ## if this key is present in the table
            if self.keyPresent(key):
                print("Message: " + self.dataTable[key])
            else:
                print("This key does not have an entry")
            print(sep)
            return

        nextHop = [self.id, self.ip, self.port]
        for i in range(HASH_BITS):
            if self.between(key, self.id, self.fingerTable[i][0]):
                break
            else:
                nextHop = self.fingerTable[i]

        if nextHop[0] == self.id:
            self.printHops(False)
            if self.keyPresent(key):
                print("Message: " + self.dataTable[key])
            else:
                print("This key does not have an entry")
            print(sep)
            return
        else:
            self.printHops()
            next_ip = nextHop[1]
            next_port = nextHop[2]
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((next_ip, next_port))
            newsock.sendall(b'getContent ' + str(key).encode())
            newsock.close()

    def putContent(self, msg):
        key = self.getMsgKey(msg)
        key = key % LOGICAL_SIZE

        if self.id == key:
            self.dataTable[key] = msg
            self.printHops(False)
            return

        nextHop = [self.id, self.ip, self.port]
        for i in range(HASH_BITS):
            if self.between(key, self.id, self.fingerTable[i][0]):
                break
            else:
                nextHop = self.fingerTable[i]

        if nextHop[0] == self.id:
            self.dataTable[key] = msg
            self.printHops(False)
            return
        else:
            self.printHops()
            next_ip = nextHop[1]
            next_port = nextHop[2]
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((next_ip, next_port))
            newsock.sendall(b'putContent ' + msg.encode())
            newsock.close()

    def printHops(self, dash=True):
        if dash:
            print (str(self.id)+ "->"),
        else:
            print (str(self.id))
            print (sep)

    def printNodes(self, startNode):
        print(self.id)
        if self.fingerTable[0][0] != startNode:
            succ_ip = self.fingerTable[0][1]
            succ_port = self.fingerTable[0][2]
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((succ_ip, succ_port))
            newsock.sendall(b'printNodes ' + str(startNode).encode())
            newsock.close()
        else:
            print(sep)

    def printFingerTable(self):
        for i in range(HASH_BITS):
            print(self.fingerTable[i])
        print(sep)

    def printMyDataContents(self):
        print("Contents of node: " + str(self))
        print(sep2)
        for k,v in self.dataTable.items():
            print(str(k) + ":" + v)
        print(sep)

    def run(self):
        self.updateFingerTable()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.bind((self.ip, self.port))
        except:
            print("Port bind error")
            exit(1)

        while True:
            cmd, addr = self.sock.recvfrom(1024)
            if cmd.startswith(b'printNodes'):
                startNode = self.id
                if cmd != b'printNodes':
                    startNode = int(cmd.split()[1])
                self.printNodes(startNode)

            elif cmd.startswith(b'findNode'):
                key = int(cmd.split()[1])
                self.findNode(key)

            elif cmd.startswith(b'putContent'):
                msg = ' '.join(cmd.split()[1:])
                self.putContent(msg)

            elif cmd.startswith(b'getContent'):
                key = int(cmd.split()[1])
                self.getContent(key)

            elif cmd == b'allContents':
                self.printMyDataContents()

            elif cmd == b'fingerTable':
                self.printFingerTable()

            elif cmd == 'exit':
                self.sock.sendto('exit', (self.fingerTable[0][1], self.fingerTable[0][2]))
                break
        
        self.sock.close()


    def __str__(self):
        return str([self.id, self.ip, self.port])


if __name__ == "__main__":
    Nnodes = int(input("Number of nodes: "))
    allNodes = []
    print("Nodes in the network:")
    for i in range(Nnodes):
        allNodes.append([getKey(nodeIP, nodePort+i), nodeIP, nodePort+i])
        print(allNodes[i])
    print(sep)

    allNodes.sort()
    nodeThreads = []

    for i in range(Nnodes):
        nodeThread = Node(allNodes[i][1], allNodes[i][2])
        nodeThread.start()
        nodeThreads.append(nodeThread)

    for node in nodeThreads:
        node.join()
