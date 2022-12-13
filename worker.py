import socket
import argparse

def command_line_arguments () :
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--master", help="E.g., node102")
    return argparser.parse_args()

# https://www.digitalocean.com/community/tutorials/python-socket-programming-server-client
def client_program(master):
    host = master
    port = 22 

    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server

    message = input(" -> ")  # take input

    while message.lower().strip() != 'bye':
        client_socket.send(message.encode())  # send message
        data = client_socket.recv(1024).decode()  # receive response

        print('Received from server: ' + data)  # show in terminal

        message = input(" -> ")  # again take input

    client_socket.close()

master = command_line_arguments()
client_program()