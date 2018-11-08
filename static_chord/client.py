import sys
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

while True:
    nodeAddr = input("Enter node address: ")
    if nodeAddr == 'exit':
        break

    try:
        ip, port = nodeAddr.split(':')
        port = int(port)

        cmd = input("Enter Command: ")
        cmd = str.encode(cmd)
        sock.connect((ip, port))
        sock.sendall(cmd)
        print()
    except:
        break

sock.close()
