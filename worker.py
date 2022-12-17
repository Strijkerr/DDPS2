import socket
import sys
import time
import numpy as np
import collections
import sys
import pickle
import json
import os
sys.stderr = open(os.devnull, "w") # To remove paramiko warning
import paramiko
sys.stderr = sys.__stderr__
import os.path

# Mapping stage: load file, count digits, then save intermediate results locally.
def mapper (location, index) :
    file =  np.load(location)
    count = collections.Counter(file)
    filename = location.split('/')[-1].split('.')[0]
    with open(f'/local/ddps2202/{filename}_{index}.pickle', 'wb') as outputfile:
        pickle.dump(count, outputfile)
    return f'/local/ddps2202/{filename}_{index}.pickle'

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
def reduce (index) :
    total_dict = collections.Counter()
    folderName = '/local/ddps2202/'
    for file in os.listdir(folderName) :
        if file.endswith(f"{index}.pickle"):
            with open(folderName + file, "rb") as input_file:
                count = pickle.load(input_file)
                total_dict+=count
    fileName = f'/home/ddps2202/DDPS2/output/reduce_output_{index}.pickle'
    with open(fileName, 'wb') as outputfile:
        pickle.dump(total_dict, outputfile)
    return fileName

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
                    shard_location = json.loads(str(msg))
                    index = shard_location['partition']
                    reply = mapper(shard_location['location'],index)
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
                    index = locations['partition']
                    for loc in locations['locations'].keys() :

                        # If intermediate results are also on the reduce worker, do not download.
                        if (locations['locations'][loc] != worker) :
                            shuffle(locations['locations'][loc],loc)
                    
                    # Reduce results, and send result location to master.
                    reply = reduce(index)
                    client_socket.send(reply.encode())
            
            # Exit client.
            client_socket.close()
            break
        except :

            # If can't connect yet, wait 5 seconds and try again.
            time.sleep(5)
            continue     

# Start client.
#print(f"Starting client for: {sys.argv[2]}")
client_program(sys.argv[1], sys.argv[2])