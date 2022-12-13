import os
import numpy as np
import argparse
import socket
import subprocess
import shutil
import paramiko
import collections

# Parsing command line arguments
def command_line_arguments () :
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--nodes", default= 'node102,node103,node104', help="E.g., node102,node103,node104") # Remove default afterwards
    argparser.add_argument("--input_file", default= 'sequence.npy', help="E.g., sequence.npy") # Remove default afterwards
    argparser.add_argument("--partitions", default= '1', help="E.g., 2")
    argparser.add_argument("--splits", default= '5', help="E.g., 5") # Remove default afterwards
    argparser.add_argument("--data_copies", default= '2', help="E.g., 2")
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

    # ##############################################################
    # sftp.chdir(folder_remote) 
    # filesInRemoteArtifacts = sftp.listdir(path=folder_remote)
    # # Empty temp directory beforehand,
    # for file in filesInRemoteArtifacts:
    #     sftp.remove(folder_remote+file)
    #############################################################
    # Close connections
    sftp.close()
    ssh.close()

    return file_local, host

# Get command line arguments
args = command_line_arguments()

# Get master & workers hostnames
master, workers = check_node_input(args.nodes)

# Create temporary directory.
tempDir = createTempDir('temp')

# Split input data
file_splits = splitInput (args.input_file, int(args.splits))

# Dict with file locations
files = os.listdir(tempDir)
dictionary = collections.defaultdict(lambda: collections.defaultdict(dict))

# Copy split input files over cluster computers.
for index, file in enumerate(files):
    
    for copy in range(int(args.data_copies)) :
        print(workers[(index + copy) % len(workers)], file)
        location, host = copyShards (workers[(index + copy) % len(workers)], file)
        dictionary[file][f"Copy{copy}"]['host'] = host
        dictionary[file][f"Copy{copy}"]['location'] = location

print(dictionary)

# # Fork process
# pid = os.fork()

# # The parent process (master node)
# if pid > 0 :
#     print(f"Parent: {pid}")
#     process = subprocess.Popen(f"ssh {master} python3 ~/DDPS2/helloworld.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     stdout, stder = process.communicate() # Blocking
#     print(pid,stdout)
# # The created child process (worker nodes)
# else :
#     for worker in workers:
#         pid = os.fork()
#         if pid:
#             print(f"Child: {pid}")
#             process = subprocess.Popen(f"ssh {worker} python3 ~/DDPS2/helloworld.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#         else:
#             os._exit(0)