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
            message = input(" -> ")  # take input
            while message.lower().strip() != 'bye':
                client_socket.send(message.encode())
                data = client_socket.recv(1024).decode()
                print('Received from server: ' + data)
                message = input(" -> ")  # again take input
        except :
            print("FAILED. Sleep briefly & try again")
            time.sleep(10)
            continue

        client_socket.close()

client_program(sys.argv[1])