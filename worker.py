import socket
import sys
import time
import numpy as np
import collections
import sys
import pickle
import json
import paramiko
import os

def mapper (location) :
    file =  np.load(location)
    count = collections.Counter(file)
    filename = location.split('/')[-1].split('.')[0]
    with open(f'/local/ddps2202/{filename}.pickle', 'wb') as outputfile:
        pickle.dump(count, outputfile)
    return f'/local/ddps2202/{filename}.pickle'

def shuffle (host, file) :
    # Create client and connect.
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, port=22)
    sftp = ssh.open_sftp()

    # Upload file.
    sftp.get(file,file)

    # Close connections
    sftp.close()
    ssh.close()

def reduce () :
    total_dict = collections.Counter()
    folderName = '/local/ddps2202/'
    for file in os.listdir(folderName) :
        if file.endswith(".pickle"):
            #print(file)
            #print(folderName)
            sequence = pickle.load(folderName + file)
            print(len(sequence))
            # total_dict+=sequence
    # return total_dict
    # with open(f'/local/ddps2202/{filename}.pickle', 'wb') as outputfile:
    #     pickle.dump(count, outputfile)
    # return f'/local/ddps2202/{filename}.pickle'


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
            # Map task
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
            # Reduce task
            while True:
                try:
                    msg = client_socket.recv(1024).decode()
                except Exception as e:
                    print(f"[!] Error: {e}")
                else:
                    if (msg == 'done') :
                        break
                    locations = json.loads(str(msg))
                    for loc in locations.keys() :
                        if (locations[loc] != worker) :
                            shuffle(locations[loc],loc)
                    reduce()
                    #print(reply)
                    #client_socket.send(reply.encode())
                    break # Remove later
            
            client_socket.close()
            break
        except :
            # If can't connect yet, wait 5 seconds and try again.
            time.sleep(5)
            continue      

client_program(sys.argv[1], sys.argv[2])
print("Client.py exit")