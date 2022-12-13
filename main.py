import os
import numpy as np
import argparse
import socket
import subprocess
import shutil
import paramiko

# Parsing command line arguments
def command_line_arguments () :
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--nodes", default= 'node102,node103,node104', help="E.g., node102,node103,node104") # Remove default afterwards
    argparser.add_argument("--input_file", default= 'sequence.npy', help="E.g., sequence.npy") # Remove default afterwards
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
    # for node in nodes :
    #     if not check_ssh(node) :
    #         raise ValueError(f"Can't connect to: {node}.")
    return nodes[0], nodes[1:]

# Split input file into different parts
def splitInput (filename, worker_count) :
    path = os.getcwd()
    if worker_count == 0 :
        raise ValueError('Not enough nodes.')
    sequence = np.load(os.path.join(path, filename))
    return np.split(sequence,worker_count)

# Copy shards from front-end to local storage of each node.
def copyShards (host, file) :

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

    # Close connections
    sftp.close()
    ssh.close()

args = command_line_arguments()
master, workers = check_node_input(args.nodes)
worker_count = len(workers)
file_splits = splitInput (args.input_file, worker_count)

# Create temp directory, empty if exists
dirName = 'temp'
if os.path.exists(dirName):
    shutil.rmtree(dirName)
os.makedirs(dirName)

# Save splits
for i, split in enumerate(file_splits) :
    np.save(f"temp/shard{i}", split)

# Copy files over cluster computers
for worker, file in zip(workers,os.listdir(dirName)):
    copyShards (worker, file)

pid = os.fork()

# The parent process (master node)
if pid > 0 :
    print(f"Parent: {pid}")
    process = subprocess.Popen(f"ssh {master} python3 ~/DDPS2/helloworld.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stder = process.communicate() # Blocking
    print(pid,stdout)
# The created child process (worker nodes)
else :
    for worker in workers:
        pid = os.fork()
        if pid:
            print(f"Child: {pid}")
            process = subprocess.Popen(f"ssh {worker} python3 ~/DDPS2/helloworld.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            os._exit(0)