import socket
import sys
import pickle

def returnDict (filename) :
    infile = open(filename,'rb')
    dictionary = pickle.load(infile)
    infile.close()
    return dictionary

    # https://www.digitalocean.com/community/tutorials/python-socket-programming-server-client
def server_program(client_count):
    host = socket.gethostname()
    server_socket = socket.socket()
    server_socket.bind((host, 0)) 
    print(server_socket.getsockname())
    # server_socket.listen(client_count)
    # conn, address = server_socket.accept()
    # print("Connection from: " + str(address))
    # while True:
    #     data = conn.recv(1024).decode()
    #     if not data:
    #         # if data is not received break
    #         break
    #     print("from connected user: " + str(data))
    #     data = input(' -> ')
    #     conn.send(data.encode())  # send data to the client

    #conn.close()  # close the connection

shard_dict = returnDict(sys.argv[1])
task_dict = returnDict(sys.argv[2])
worker_dict = returnDict(sys.argv[3])

server_program(len(worker_dict))