import socket
import sys
import time
import numpy as np
import collections
import sys
import pickle

def returnDict (filename) :
    infile = open(filename,'rb')
    dictionary = pickle.load(infile)
    infile.close()
    return dictionary

def mapper (task, worker, location) :
    shard = location
    test =  np.load(shard)
    count = collections.Counter(test)
    filename = shard.split('/')[-1].split('.')[0]
    with open(f'temp/{filename}.pickle', 'wb') as outputfile:
        pickle.dump(count, outputfile)
    return task, worker, f'/home/ddps2202/DDPS2/temp/{filename}.pickle'

def shuffle (task, workers, locations) :
    #TODO
    pass

def reduce () :
    #TODO
    pass
    #total_dict = collections.Counter()
    
    # for pickle_file in pickle_files :
    #     with open(pickle_file, 'rb') as inputfile:
    #         pickle_dict = pickle.load(inputfile)
    #         total_dict+=pickle_dict

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