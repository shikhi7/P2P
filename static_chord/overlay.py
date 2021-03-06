import sys
import socket
from hashlib import sha1
from random import randint
import threading

HASH_BITS = 160
LOGICAL_SIZE = 2**HASH_BITS
nodeIP = "127.0.0.1"
# nodeIP = "192.168.137.87"
nodePort = 4505
sep = "-"*30 + "\n"
sep2 = "-"*30
recvBytes = 2048

def getKey(ip, port):
    raw = str(ip) + str(port)
    key = int(sha1(raw.encode('utf-8')).hexdigest(), 16)
    return key

class Node(threading.Thread):
    def __init__(self, ip, port, dynamicNode=False):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.id = getKey(ip, port)
        self.info = [self.id, self.ip, self.port]

        self.successor = self.info
        self.predecessor = self.info
        # self.fingerTable = {}
        self.deBruijnNode = self.info
        self.dataTable = {}

        self.sock = None
        self.dynamicNode = dynamicNode

    # def updateFingerTable(self):
    #     for i in range(HASH_BITS):
    #         self.fingerTable[i] = [self.id, self.ip, self.port]
    #         f = False
    #         for j in allNodes:
    #             if j[0] >= (self.id + 2**i) % LOGICAL_SIZE:
    #                 self.fingerTable[i] = j
    #                 f = True
    #                 break
    #
    #         if not f:
    #             self.fingerTable[i] = allNodes[0]

    def updateNeighbours(self):
        myIndex = -1
        for idx, i in enumerate(allNodes):
            if (self.id == i[0]):
                myIndex = idx
                break

        allNodesSize = len(allNodes)
        self.predecessor = allNodes[(myIndex-1)%allNodesSize]
        self.successor = allNodes[(myIndex+1)%allNodesSize]

        deBIdx = -1
        for idx, i in enumerate(allNodes):
            if i[0] > (2*(self.id)%LOGICAL_SIZE):
                deBIdx = idx
                break
        self.deBruijnNode = allNodes[(deBIdx-1)%allNodesSize]

    def between(self, n1, n2, n3):
        ## if n1 is in between n2 and n3
        if n2 < n3:
            return (n2 < n1 < n3)
        else:
            return (n1 < n3 or n1 > n2)

    def endInclusive(self, n1, n2, n3):
        if n1 == n3:
            return True
        else:
            return self.between(n1, n2, n3)

    def startInclusive(self, n1, n2, n3):
        if n1 == n2:
            return True
        else:
            return self.between(n1, n2, n3)

    def msb(self, num):
        fbit = 2**(HASH_BITS - 1)
        if num & fbit == fbit:
            return 1
        else:
            return 0

    def findNodeKoorde(self, key, keyShift, i, startNodeAdd):
        key = key % LOGICAL_SIZE
        startNodeIP, startNodePort = startNodeAdd.split(':')

        print("In findNode() method of node: " + str(self) + " for the key: " + str(key))
        print(sep)

        if self.endInclusive(key, self.id, self.successor[0]):
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((startNodeIP, int(startNodePort)))
            resultString = ' '.join(map(str, self.successor)) + " " + ' '.join(map(str, self.info))
            newsock.sendall(b'foundNode ' + resultString.encode())
            newsock.close()
            # self.printHops(False)
            return

        elif self.endInclusive( i, self.id, self.successor[0]):
            # print("yo boy")
            next_ip = self.deBruijnNode[1]
            next_port = self.deBruijnNode[2]
            i = (i<<1)%LOGICAL_SIZE + self.msb(keyShift)
            keyShift = (keyShift<<1)%LOGICAL_SIZE

            while( self.endInclusive(i, self.id, self.successor[0]) and self.id == self.deBruijnNode[0]):
                i = (i<<1)%LOGICAL_SIZE + self.msb(keyShift)
                keyShift = (keyShift<<1)%LOGICAL_SIZE

            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((next_ip, next_port))
            newsock.sendall(b'findNode ' + str(key).encode() + ' ' + str(keyShift).encode() + ' ' + str(i).encode() + ' ' + startNodeAdd)
            newsock.close()

        else:
            next_ip = self.successor[1]
            next_port = self.successor[2]
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((next_ip, next_port))
            newsock.sendall(b'findNode ' + str(key).encode() + ' ' + str(keyShift).encode() + ' ' + str(i).encode() + ' ' + startNodeAdd)
            newsock.close()

    def findBestImag(self, key):
        tmp = self.id
        imaginaryNodes = []
        lenOfImList = HASH_BITS - 1

        for i in range(lenOfImList):
            nxt = ((tmp<<1)%LOGICAL_SIZE) + self.msb(key)
            imaginaryNodes.append(nxt)
            tmp = nxt
            key = (key<<1)%LOGICAL_SIZE

        imaginaryNodes.reverse()
        j=0
        while(j<(HASH_BITS-2) and not self.endInclusive(imaginaryNodes[j], self.id, self.successor[0])):
            j+=1
        return imaginaryNodes[j]

    # def findNodeChord(self, key, startNodeAdd):
    #     key = key % LOGICAL_SIZE
    #
    #     startNodeIP, startNodePort = startNodeAdd.split(':')
    #
    #     if self.id == key:
    #         newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #         newsock.connect((startNodeIP, int(startNodePort)))
    #         newsock.sendall(b'foundNode ' + str(self).encode())
    #         newsock.close()
    #         self.printHops(False)
    #         return
    #
    #     nextHop = self.info
    #     for i in range(HASH_BITS):
    #         if self.between(key, self.id, self.fingerTable[i][0]):
    #             break
    #         else:
    #             nextHop = self.fingerTable[i]
    #
    #     if nextHop[0] == self.id:
    #         # succ_ip = self.fingerTable[0][1]
    #         # succ_port = self.fingerTable[0][2]
    #         newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #         newsock.connect((startNodeIP, int(startNodePort)))
    #         newsock.sendall(b'foundNode ' + str(self.fingerTable[0]).encode())
    #         newsock.close()
    #         self.printHops(False)
    #         return
    #     else:
    #         self.printHops()
    #         next_ip = nextHop[1]
    #         next_port = nextHop[2]
    #         newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #         newsock.connect((next_ip, next_port))
    #         newsock.sendall(b'findNode ' + str(key).encode() + ' ' + startNodeAdd)
    #         newsock.close()

    def joinNetwork(self, newNode):
        newNodeID = getKey(newNode[0], newNode[1])
        newsock_ip = nodeIP
        newsock_port = 18001

        print("Got a request to add new node: " + newNode[0] + ":" + str(newNode[1]) + ". I am node: " + str(self) )
        print(sep)

        newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        newsock.bind((newsock_ip, newsock_port))

        i = self.findBestImag(newNodeID)
        self.findNodeKoorde(newNodeID, newNodeID, i, str(newsock_ip)+':'+str(newsock_port))

        data, addr = newsock.recvfrom(recvBytes)
        data = data[len('foundNode '):].split()
        newNodeSucc = data[:3]
        newNodePred = data[3:]

        newNodeD = (2*newNodeID) % LOGICAL_SIZE
        i = self.findBestImag(newNodeD)
        self.findNodeKoorde(newNodeD, newNodeD, i, str(newsock_ip)+':'+str(newsock_port))

        data, addr = newsock.recvfrom(recvBytes)
        data = data[len('foundNode '):].split()
        newNodeDe = data[3:]
        newsock.close()

        infoNeighbours = ' '.join(map(str, newNodeSucc)) + " " + ' '.join(map(str, newNodePred)) + " " + ' '.join(map(str, newNodeDe))
        self.sock.sendto(str(infoNeighbours), (newNode[0], int(newNode[1])))

        resultString = str(newNodeID) + " " + newNode[0] + " " + str(newNode[1])
        updateMsg = "changeNode 0 " + resultString
        self.sock.sendto(updateMsg, (newNodePred[1], int(newNodePred[2])))

        updateMsg = "changeNode 1 " + resultString
        self.sock.sendto(updateMsg, (newNodeSucc[1], int(newNodeSucc[2])))

        self.updateOthers(newNode, newNodePred)
        self.invokeContentShare(newNodeSucc, newNode)

    def updateOthers(self, newNode, newNodePred):
        predID = int(newNodePred[0]) + 1
        predNodeID = (predID>>1)%LOGICAL_SIZE

        if (predNodeID << 1)%LOGICAL_SIZE == predID:
            i = self.findBestImag(predNodeID)
            newsock_ip = nodeIP
            newsock_port = 18001

            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.bind((newsock_ip, newsock_port))

            self.findNodeKoorde(predNodeID, predNodeID, i, str(newsock_ip)+':'+str(newsock_port))
            data, addr = newsock.recvfrom(recvBytes)
            data = data[len('foundNode '): ].split()

            updateNode = data[:3]
            newNodeID = getKey(newNode[0], newNode[1])
            resultString = str(newNodeID) + " " + newNode[0] + " " + str(newNode[1])

            updateMsg = "changeNode 2 " + resultString
            # print(str(updateNode))
            # print(updateMsg)
            # print(sep)
            newsock.sendto(updateMsg, (updateNode[1], int(updateNode[2])))
            newsock.close()

    ## ------------------------ content update code -------------------

    def invokeContentShare(self, succ, newNode):
        newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        contentMsg = "contentShare " + newNode[0] + " " + str(newNode[1])
        newsock.sendto(contentMsg, (succ[1], int(succ[2])))
        print('Invoking the successor of new node to share content belonging to new node')
        print(sep)
        newsock.close()

    def sendContentToNewNode(self, newNodeAddr):
        print("Sending contents belonging to new node")
        print(sep)
        newNodeID = getKey(newNodeAddr[0], newNodeAddr[1])
        contentList = []
        for k, v in self.dataTable.items():
            if not self.endInclusive(k, newNodeID, self.id):
                contentList.append([k, v])

        #  print('sending to new node')
        if len(contentList) > 0:
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for elem in contentList:
                ## assuming value is a list
                elemMsg = 'contentUpdate ' + ' '.join(map(str, elem[1]))
                newsock.sendto(elemMsg, tuple(newNodeAddr))
                del self.dataTable[elem[0]]
            newsock.close()
        #  print('sent to new node')

    def updateMyContent(self, msg):
        print('Receiving and updating my content as a new node')
        print(sep)
        key = self.getMsgKey(msg[0])
        self.dataTable[key] = msg
    ## ------------------------ content update code -------------------

    def getMsgKey(self, msg):
        key = int(sha1(msg.encode('utf-8')).hexdigest(), 16)
        return key

    def keyPresent(self, key):
        return (key in self.dataTable)

    # def putContentChord(self, msg):
    #     key = self.getMsgKey(msg)
    #     key = key % LOGICAL_SIZE
    #
    #     if self.id == key:
    #         self.dataTable[key] = msg
    #         self.printHops(False)
    #         return
    #
    #     nextHop = [self.id, self.ip, self.port]
    #     for i in range(HASH_BITS):
    #         if self.between(key, self.id, self.fingerTable[i][0]):
    #             break
    #         else:
    #             nextHop = self.fingerTable[i]
    #
    #     if nextHop[0] == self.id:
    #         self.dataTable[key] = msg
    #         self.printHops(False)
    #         return
    #     else:
    #         self.printHops()
    #         next_ip = nextHop[1]
    #         next_port = nextHop[2]
    #         newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #         newsock.connect((next_ip, next_port))
    #         newsock.sendall(b'putContent ' + msg.encode())
    #         newsock.close()

    def putContentKoorde(self, msg):
        msgList = msg.split()
        username = msgList[0]
        password = msgList[1]
        msgKey = self.getMsgKey(username)
        msgKey = msgKey % LOGICAL_SIZE

        newsock_ip = nodeIP
        newsock_port = 18111
        newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        newsock.bind((newsock_ip, newsock_port))
        i = self.findBestImag(msgKey)
        self.findNodeKoorde(msgKey, msgKey, i, str(newsock_ip)+':'+str(newsock_port))

        data, addr = newsock.recvfrom(recvBytes)
        data = data[len('foundNode '):].split()
        msgKeySucc = data[:3]
        newsock.close()

        print("About to put the msg: *"+msg+"* with msgKey "+str(msgKey)+" in node: " + str(msgKeySucc))
        print(sep)

        resultString = "putYourContent " + msg
        self.sock.sendto(resultString, (msgKeySucc[1], int(msgKeySucc[2])))

    def putInMyContent(self, msg):
        msgList = msg.split()
        username = msgList[0]
        password = msgList[1]
        msgKey = self.getMsgKey(username)
        msgKey = msgKey % LOGICAL_SIZE
        self.dataTable[msgKey] = [username, password]

    # def getContentChord(self, key):
    #     key = key % LOGICAL_SIZE
    #
    #     if self.id == key:
    #         self.printHops(False)
    #         ## if this key is present in the table
    #         if self.keyPresent(key):
    #             print("Message: " + self.dataTable[key])
    #         else:
    #             print("This key does not have an entry")
    #         print(sep)
    #         return
    #
    #     nextHop = [self.id, self.ip, self.port]
    #     for i in range(HASH_BITS):
    #         if self.between(key, self.id, self.fingerTable[i][0]):
    #             break
    #         else:
    #             nextHop = self.fingerTable[i]
    #
    #     if nextHop[0] == self.id:
    #         self.printHops(False)
    #         if self.keyPresent(key):
    #             print("Message: " + self.dataTable[key])
    #         else:
    #             print("This key does not have an entry")
    #         print(sep)
    #         return
    #     else:
    #         self.printHops()
    #         next_ip = nextHop[1]
    #         next_port = nextHop[2]
    #         newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #         newsock.connect((next_ip, next_port))
    #         newsock.sendall(b'getContent ' + str(key).encode())
    #         newsock.close()

    def getContentKoorde(self, msg):
        msgKey = self.getMsgKey(msg)
        msgKey = msgKey % LOGICAL_SIZE

        newsock_ip = nodeIP
        newsock_port = 18112
        newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        newsock.bind((newsock_ip, newsock_port))
        i = self.findBestImag(msgKey)
        self.findNodeKoorde(msgKey, msgKey, i, str(newsock_ip)+':'+str(newsock_port))

        data, addr = newsock.recvfrom(recvBytes)
        data = data[len('foundNode '):].split()
        msgKeySucc = data[:3]
        newsock.close()

        print("About to get the msg: *"+msg+"* with msgKey "+str(msgKey)+" from node: " + str(msgKeySucc))
        print(sep)

        resultString = "getYourContent " + msg
        self.sock.sendto(resultString, (msgKeySucc[1], int(msgKeySucc[2])))

    def fetchMyContent(self, msg, queryNode):
        msgKey = self.getMsgKey(msg)
        msgKey = msgKey % LOGICAL_SIZE
        queryNodeIP = queryNode[0]
        queryNodePort = queryNode[1]

        response = ""
        if msgKey in self.dataTable.keys():
            response = self.dataTable[msgKey][1]
        else:
            response = "Couldn't find this username!"

        print('About to send the response to msg:*'+msg+'* from my data to queryNode: '+ queryNodeIP + ":" + str(queryNodePort) +'. I am node: ' + str(self))
        print(sep)
        resultString = "responseToQuery " + response
        self.sock.sendto(resultString, (queryNodeIP, queryNodePort))

    def printHops(self, dash=True):
        if dash:
            print (str(self.id)+ "->"),
        else:
            print (str(self.id))
            print (sep)

    # def printNodes(self, startNode):
    #     print(self.id)
    #     if self.fingerTable[0][0] != startNode:
    #         succ_ip = self.fingerTable[0][1]
    #         succ_port = self.fingerTable[0][2]
    #         newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #         newsock.connect((succ_ip, succ_port))
    #         newsock.sendall(b'printNodes ' + str(startNode).encode())
    #         newsock.close()
    #     else:
    #         print(sep)

    def printNodesKoorde(self, startNode):
        print(self)
        if self.successor[0] != startNode:
            succ_ip = self.successor[1]
            succ_port = self.successor[2]
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((succ_ip, succ_port))
            newsock.sendall(b'printNodes ' + str(startNode).encode())
            newsock.close()
        else:
            print(sep)

    def printNeighbourInfo(self):
        print("I am " + str(self))
        print("My successor is " + str(self.successor))
        print("My predecessor is " + str(self.predecessor))
        print("My deBruijn node is " + str(self.deBruijnNode))
        print(sep)

    def printFingerTable(self):
        for i in range(HASH_BITS):
            print(self.fingerTable[i])
        print(sep)

    def printMyDataContents(self):
        print("Contents of node: " + str(self) + " are: ")
        print(sep2)
        for k,v in self.dataTable.items():
            print(str(k) + ":" + str(v))
        print(sep)

    def printAllContents(self, startNodeID=None):
        if (self.id == startNodeID):
            return
        elif (startNodeID == None):
            startNodeID = self.id
        self.printMyDataContents()
        resultString = 'allContents ' + str(startNodeID)
        self.sock.sendto(resultString, (self.successor[1], self.successor[2]))

    def run(self):
        # self.updateFingerTable()
        if not self.dynamicNode:
            self.updateNeighbours()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                self.sock.bind((self.ip, self.port))
            except:
                print("Port bind error")
                exit(1)
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                self.sock.bind((self.ip, self.port))
            except:
                print("Port bind error")
                exit(1)
            self.sock.sendto("joinNetwork", (nodeIP, nodePort))
            data, addr = self.sock.recvfrom(recvBytes)
            data = data.split()
            self.successor = [int(data[0]), data[1], int(data[2])]
            self.predecessor = [int(data[3]), data[4], int(data[5])]
            self.deBruijnNode = [int(data[6]), data[7], int(data[8])]
            print("I have joined the network! Here are my neighbours:")
            self.printNeighbourInfo()

        while True:

            cmd, addr = self.sock.recvfrom(recvBytes)

            if cmd.startswith(b'printNodes'):
                startNode = self.id
                if cmd != b'printNodes':
                    startNode = int(cmd.split()[1])
                self.printNodesKoorde(startNode)

            elif cmd.startswith(b'findNode'):
                lst = cmd.split()
                if len(lst)==2:
                    # startNode
                    key = int(lst[1])
                    i = self.findBestImag(key)
                    self.findNodeKoorde(key, key, i, str(self.ip)+':'+str(self.port))
                else:
                    ## intermediate
                    key = int(lst[1])
                    kShift = int(lst[2])
                    i = int(lst[3])
                    startNode = lst[4]
                    self.findNodeKoorde(key, kShift, i, startNode)

            elif cmd.startswith(b'foundNode'):
                lst = cmd[len('foundNode '):].split()
                targetNode = lst[:3]
                targetPred = lst[3:]
                print("I am node: " + str(self))
                print("Target node is " + str(targetNode))
                print(sep)

            elif cmd.startswith(b'putContent'):
                msg = ' '.join(cmd.split()[1:])
                print("About to invoke putContentKoorde from node: " + str(self) + " for the message: *" + msg +"*")
                print(sep)
                thread1 = threading.Thread(target = self.putContentKoorde, args = (msg, ))
                thread1.start()
                # self.putContentKoorde(msg)

            elif cmd.startswith(b'putYourContent'):
                msg = ' '.join(cmd.split()[1:])
                print('Received a request to add msg:*'+msg+'* into my data. I am node: ' + str(self))
                print(sep)
                self.putInMyContent(msg)

            elif cmd.startswith(b'getContent'):
                msg = ' '.join(cmd.split()[1:])
                print("About to invoke getContentKoorde from node: " + str(self) + " for the message: *" + msg +"*")
                print(sep)
                thread2 = threading.Thread(target = self.getContentKoorde, args = (msg, ))
                thread2.start()

            elif cmd.startswith(b'getYourContent'):
                msg = ' '.join(cmd.split()[1:])
                print('Received a request to get msg:*'+msg+'* from my data. I am node: ' + str(self))
                print(sep)
                self.fetchMyContent(msg, addr)

            elif cmd.startswith(b'responseToQuery'):
                response = ' '.join(cmd.split()[1:])
                print('Got a response *' + response + '*. I am node: ' + str(self))
                print(sep)

            elif cmd == b'myContents':
                self.printMyDataContents()

            elif cmd.startswith(b'allContents'):
                lst = cmd.split()
                if len(lst) == 1:
                    #startNode
                    self.printAllContents()
                else:
                    self.printAllContents(int(lst[1]))

            # elif cmd == b'fingerTable':
            #     self.printFingerTable()

            elif cmd == b'neighbourInfo':
                self.printNeighbourInfo()

            elif cmd == b'joinNetwork':
                thread1 = threading.Thread(target = self.joinNetwork, args = (addr, ))
                thread1.start()
                #  self.joinNetwork(addr)

            elif cmd.startswith(b'changeNode'):
                # print(cmd)
                lst = cmd.split()
                if lst[1] == '0':
                    self.successor = [int(lst[2]), lst[3], int(lst[4])]
                    print("Updating my successor to " + str(self.successor))
                    print(sep)
                elif (lst[1] == str(1)):
                    self.predecessor = [int(lst[2]), lst[3], int(lst[4])]
                    print("Updating my predecessor to " + str(self.predecessor))
                    print(sep)
                elif (lst[1] == str(2)):
                    self.deBruijnNode = [int(lst[2]), lst[3], int(lst[4])]
                    print("Updating my deBruijnNode to " + str(self.deBruijnNode))
                    print(sep)

            elif cmd.startswith(b'contentShare'):
                lst = cmd.split()[1:]
                newNode_addr = [lst[0], int(lst[1])]
                self.sendContentToNewNode(newNode_addr)

            elif cmd.startswith(b'contentUpdate'):
                lst = cmd.split()[1:]
                self.updateMyContent(lst)

            elif cmd == 'exit':
                self.sock.sendto('exit', (self.successor[1], self.successor[2]))
                break

        self.sock.close()


    def __str__(self):
        return str([self.id, self.ip, self.port])


if __name__ == "__main__":
    Nnodes = int(input("Number of nodes: "))
    allNodes = []
    for i in range(Nnodes):
        allNodes.append([getKey(nodeIP, nodePort+i), nodeIP, nodePort+i])

    allNodes.sort()

    print("Nodes in the network:")
    for i in allNodes:
        print(i)
    print(sep)

    nodeThreads = []
    for i in range(Nnodes):
        nodeThread = Node(allNodes[i][1], allNodes[i][2])
        nodeThread.start()
        nodeThreads.append(nodeThread)

    for node in nodeThreads:
        node.join()
