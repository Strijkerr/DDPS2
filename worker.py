import socket
import sys

# https://www.digitalocean.com/community/tutorials/python-socket-programming-server-client
def client_program(master):
    host = master
    port = 56609

    client_socket = socket.socket()
    client_socket.connect((host, port))

    message = input(" -> ")  # take input
    while message.lower().strip() != 'bye':
        client_socket.send(message.encode())
        data = client_socket.recv(1024).decode()
        print('Received from server: ' + data)
        message = input(" -> ")  # again take input

    client_socket.close()

client_program(sys.argv[1])