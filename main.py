import os
import numpy as np
import argparse
import socket
import subprocess
import shutil
import paramiko
import collections
import json

# Parsing command line arguments
def command_line_arguments () :
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--nodes", default= 'node102,node103,node104', type=str, help="E.g., node102,node103,node104") # Remove default afterwards
    argparser.add_argument("--input", default= 'sequence.npy', type=str, help="E.g., sequence.npy") # Remove default afterwards
    argparser.add_argument("--partitions", default= '1', type=int, help="E.g., 2")
    argparser.add_argument("--splits", default= '5', type=int, help="E.g., 5")
    argparser.add_argument("--copies", default= '1', type=int, help="E.g., 0")
    return argparser.parse_args()

# Test connection of nodes
def check_ssh(server_ip, port=22):
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.connect((server_ip, port))
    except Exception:
        return False
    else:
        test_socket.close()
    return True

# Turn command line arguments to list of nodes, then test connections to nodes.
def check_node_input (arguments) :
    if arguments is None:
        raise ValueError('No nodes specified.')
    nodes = arguments.strip().split(',')
    for node in nodes :
        if not check_ssh(node) :
            raise ValueError(f"Can't connect to: {node}.")
    if (len(nodes[1:]) == 0) :
        raise ValueError('Not enough nodes.')
    return nodes[0], nodes[1:]

# Split input file into different parts
def splitInput (filename, splits) :
    path = os.getcwd()
    sequence = np.load(os.path.join(path, filename))

    # Save splits
    for i, split in enumerate(np.split(sequence,splits)) :
        np.save(f"temp/shard{i}", split)
    return True

def createTempDir (dirName) :
    # Create temp directory, empty if exists
    if os.path.exists(dirName):
        shutil.rmtree(dirName)
    os.makedirs(dirName)
    return dirName

# Copy shards from front-end to local storage of each node.
def copyShards (host, file, delete=True) : # Delete `delete' after we are done debugging.

    # Create client and connect.
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, port=22)
    sftp = ssh.open_sftp()
    
    # Test if remote_path exists.
    folder_remote = '/local/ddps2202/'
    try:
        sftp.chdir(folder_remote) 
        filesInRemoteArtifacts = sftp.listdir(path=folder_remote)
        # Empty temp directory beforehand,
        for file in filesInRemoteArtifacts:
            sftp.remove(folder_remote+file)
    # Create directory if it doesn't yet exist
    except:
        sftp.mkdir(folder_remote) 
        sftp.chdir(folder_remote)

    # Upload file.
    file_remote = folder_remote + file
    file_local = '/home/ddps2202/DDPS2/temp/' + file
    sftp.put(file_local,file_remote)

    ################################ This removes shit again as we do not want to fill the local storage of every node.  ##############################
    if delete :
        sftp.chdir(folder_remote) 
        filesInRemoteArtifacts = sftp.listdir(path=folder_remote)
        # Empty temp directory beforehand,
        for file in filesInRemoteArtifacts:
            sftp.remove(folder_remote+file)
    ############################################################
    # Close connections
    sftp.close()
    ssh.close()

    return file_local, host

# Get command line arguments
args = command_line_arguments()

# Get master & workers hostnames
master, workers = check_node_input(args.nodes)

# Limit copies < workers, don't want to store more than 1 copy per worker.
if (args.copies+1 > len(workers)) :
    args.copies = len(workers)
else :
    args.copies +=1

# Create temporary directory.
tempDir = createTempDir('temp')

# Split input data
file_splits = splitInput (args.input, args.splits)

# Dict with file locations
files = os.listdir(tempDir)
dictionary = collections.defaultdict(lambda: collections.defaultdict(dict))

# Copy split input files over cluster computers.
for index, file in enumerate(files):
    for copy in range(args.copies) :
        location, host = copyShards (workers[(index + copy) % len(workers)], file)
        dictionary[file][f"Copy{copy}"]['host'] = host
        dictionary[file][f"Copy{copy}"]['location'] = location

print("(Complete) Data has been split and distributed over cluster.")

json_dictionary = json.dumps(dictionary)
# print(json_dictionary)
# test_dictionary = json.loads(json_dictionary)
# print(test_dictionary)

# Fork process
pid = os.fork()

# The parent process (master node)
if pid > 0 :
    process = subprocess.Popen(f"ssh {master} python3 ~/DDPS2/helloworld.py {[json_dictionary]}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stder = process.communicate() # Blocking
    print("Stdout:",stdout.decode('ASCII'))
    print("Stderr:",stder)
# # The created child process (worker nodes)
# else :
#     for worker in workers:
#         pid = os.fork()
#         if pid:
#             print(f"Child: {pid}")
#             process = subprocess.Popen(f"ssh {worker} python3 ~/DDPS2/helloworld.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#         else:
#             os._exit(0)