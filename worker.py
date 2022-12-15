import socket
import sys
import time
import numpy as np
import collections
import sys
import pickle
import os.path

def returnDict (filename) :
    infile = open(filename,'rb')
    dictionary = pickle.load(infile)
    infile.close()
    return dictionary

def mapper (location) :
    file =  np.load(location)
    count = collections.Counter(file)
    filename = location.split('/')[-1].split('.')[0]
    with open(f'/local/ddps2202/{filename}.pickle', 'wb') as outputfile:
        pickle.dump(count, outputfile)
    return f'/local/ddps2202/{filename}.pickle'

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

def client_program(master, worker):
    host = master
    port = 56609
    client_socket = socket.socket()
    while True :
        try :
            client_socket.connect((host, port))

            # Send identity
            try : 
                client_socket.send(worker.encode())
            except Exception as e:
                    print(f"[!] Error: {e}")
            # Main while loop
            while True:
                # Get task
                try:
                    msg = client_socket.recv(1024).decode()
                except Exception as e:
                    print(f"[!] Error: {e}")
                else:
                    if (msg == 'done') :
                        break
                    # Get result of mapping operation and send result location to master node.
                    reply = mapper(msg)
                    client_socket.send(reply.encode())
            client_socket.close()
            break
        except :
            # If can't connect yet, wait 5 seconds and try again.
            time.sleep(5)
            continue      

client_program(sys.argv[1], sys.argv[2])
print("Client.py exit")