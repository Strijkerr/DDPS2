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
    # TODO: Change this from this chat functionality into something useful
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
    print("All clients connected. Program exit")

shard_dict = returnDict(sys.argv[1])
map_task_dict = returnDict(sys.argv[2])
reduce_task_dict = returnDict(sys.argv[3])
worker_dict = returnDict(sys.argv[4])

server_program(len(worker_dict))

print(shard_dict)
print(map_task_dict)
print(reduce_task_dict)
print(worker_dict)