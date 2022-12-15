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

def on_new_client(conn):
    while True :
        try:
            msg = conn.recv(1024).decode()
        except Exception as e:
            print(f"[!] Error: {e}")
            conn.remove(conn)
        else:
            conn.send(msg.encode())

def server_program(client_count):
    host = socket.gethostname()
    port = 56609
    server_socket = socket.socket()
    server_socket.bind((host, port)) 
    server_socket.listen(client_count)
    list_of_clients = []
    while True and len(list_of_clients) != client_count:
        conn, address = server_socket.accept()
        list_of_clients.append(conn)
        t = threading.Thread(target=on_new_client, args=(conn,))
        t.daemon = True
        t.start()
    # At this point the daemons for every client have been created.
    print("Test")

shard_dict = returnDict(sys.argv[1])
task_dict = returnDict(sys.argv[2])
worker_dict = returnDict(sys.argv[3])

server_program(len(worker_dict))