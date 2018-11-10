import sys
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

while True:
    nodeAddr = input("Enter node port: ")
    if nodeAddr == 'exit':
        break

    try:
        # ip, port = nodeAddr.split(':')
        port = int(nodeAddr)
        # port = int(port)
        ip = "127.0.0.1"
        # ip = "192.168.137.87"
        cmd = input("Enter Command: ")
        cmd = str.encode(cmd)
        sock.connect((ip, port))
        sock.sendall(cmd)
        print()
    except:
        break

sock.close()
