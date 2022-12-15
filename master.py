import socket
import sys
import pickle
import threading
from _thread import *

def returnDict (filename) :
    infile = open(filename,'rb')
    dictionary = pickle.load(infile)
    infile.close()
    return dictionary

# https://stackoverflow.com/questions/10810249/python-socket-multiple-clients
def on_new_client(conn):
    while True:
        data = conn.recv(1024).decode()
        if not data:
            break
        reply = "OK . . " + data
        conn.send(reply.decode())
    conn.close()

# https://www.digitalocean.com/community/tutorials/python-socket-programming-server-client
def server_program(client_count):
    host = socket.gethostname()
    port = 56605
    server_socket = socket.socket()
    server_socket.bind((host, port)) 
    server_socket.listen(client_count)

    while True:
        conn, address = server_socket.accept()
        print("Connection from: " + str(address))
        data = "Welcome to the Server. Type messages and press enter to send.\n"
        conn.send(data.decode())
        # data = conn.recv(1024).decode()
        # if not data:
        #     break
        # if data == 'bye' :
        #     break
        # print("from connected user: " + str(data))
        # data = "ok"
        # conn.send(data.encode())
        start_new_thread(on_new_client,(conn, ))
    conn.close()

shard_dict = returnDict(sys.argv[1])
task_dict = returnDict(sys.argv[2])
worker_dict = returnDict(sys.argv[3])

server_program(len(worker_dict))