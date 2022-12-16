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
import os.path

# Mapping stage: load file, count digits, then save intermediate results locally.
def mapper (location) :
    file =  np.load(location)
    count = collections.Counter(file)
    filename = location.split('/')[-1].split('.')[0]
    with open(f'/local/ddps2202/{filename}.pickle', 'wb') as outputfile:
        pickle.dump(count, outputfile)
    return f'/local/ddps2202/{filename}.pickle'

# Shuffle stage: connect to storages with intermediate results, then move results to local storage.
def shuffle (host, file) :

    # Create client and connect.
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, port=22)
    sftp = ssh.open_sftp()

    # Upload file.
    sftp.get(file,file)

    # Close connections.
    sftp.close()
    ssh.close()

# Reduce stage: loop over intermediate results and aggregate them into one.
def reduce () :
    total_dict = collections.Counter()
    folderName = '/local/ddps2202/'
    for file in os.listdir(folderName) :
        if file.endswith(".pickle"):
            with open(folderName + file, "rb") as input_file:
                count = pickle.load(input_file)
                total_dict+=count
    print(total_dict)
    # return total_dict
    # with open(f'/local/ddps2202/{filename}.pickle', 'wb') as outputfile:
    #     pickle.dump(count, outputfile)
    # return f'/local/ddps2202/{filename}.pickle'

# Client.
def client_program(master, worker):

    # Setup connection to master node.
    host = master
    port = 56609
    client_socket = socket.socket()

    # Main client loop.
    while True :

        # Try to connect to master node, if fails, wait 5 seconds and try again.
        try :
            client_socket.connect((host, port))

            # Send worker's identity to master node.
            try : 
                client_socket.send(worker.encode())
            except Exception as e:
                    print(f"[!] Error: {e}")

            # Mapping stage loop.
            while True:

                # Get mapping task.
                try:
                    msg = client_socket.recv(1024).decode()
                except Exception as e:
                    print(f"[!] Error: {e}")
                else:
                
                    # Exit mapping stage if master node sends 'done' signal.
                    if (msg == 'done') :
                        break
                    
                    # Get result of mapping operation and send result location to master node.
                    reply = mapper(msg)
                    client_socket.send(reply.encode())
            
            # Shuffle and reduce stage loop.
            while True:

                # Get reduce task.
                try:
                    msg = client_socket.recv(1024).decode()
                except Exception as e:
                    print(f"[!] Error: {e}")
                else:

                    # Exit shuffle/reduce stage if master node sends 'done' signal.
                    if (msg == 'done') :
                        break

                    # Get dictionary with intermediate result locations.
                    locations = json.loads(str(msg))
                    for loc in locations.keys() :

                        # If intermediate results are also on the reduce worker, do not download.
                        if (locations[loc] != worker) :
                            shuffle(locations[loc],loc)
                    
                    # Reduce results
                    reduce()
                    # reply = reduce()
                    # print(reply)
                    #client_socket.send(reply.encode())
                    break # Remove later
            
            # Exit client.
            client_socket.close()
            break
        except :

            # If can't connect yet, wait 5 seconds and try again.
            time.sleep(5)
            continue     
    
    print("Client.py exit") 

# Start client.
client_program(sys.argv[1], sys.argv[2])