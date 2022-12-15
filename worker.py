import socket
import sys
import time

def client_program(master):
    while True :
        host = master
        port = 56609
        client_socket = socket.socket()
        try :
            client_socket.connect((host, port))

            # TODO: Change this from this chat functionality into something useful
            message = input(" -> ")
            while message.lower().strip() != 'bye':
                client_socket.send(message.encode())
                data = client_socket.recv(1024).decode()
                print('Received from server: ' + data)
                message = input(" -> ")
        except :
            # If can't connect yet, wait 5 seconds and try again.
            time.sleep(5)
            continue
        client_socket.close()

client_program(sys.argv[1])