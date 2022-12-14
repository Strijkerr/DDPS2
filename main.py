import os
import numpy as np
import argparse
import socket
import subprocess
import shutil
import paramiko
import collections
import json
import pickle

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

def deleteTempDir (dirName) :
    # Remove temp dir
    if os.path.exists(dirName):
        shutil.rmtree(dirName)
    return True

# Copy shards from front-end to local storage of each node.
def copyFiles (host, file) : # Delete `delete' after we are done debugging.

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
        for f in filesInRemoteArtifacts:
            sftp.remove(folder_remote+f)
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

    return file_local, host

def removeTempRemote (hosts) :
    # Loop over hosts
    for host in hosts :
        # Create client and connect.
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(
                    paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, port=22)
        sftp = ssh.open_sftp()
        
        # Test if remote_path exists, then empty and remove folder.
        folder_remote = '/local/ddps2202/'
        try:
            sftp.chdir(folder_remote) 
            filesInRemoteArtifacts = sftp.listdir(path=folder_remote)
            # Empty temp directory beforehand,
            for file in filesInRemoteArtifacts:
                sftp.remove(folder_remote+file)
            sftp.rmdir(folder_remote)
        except:
            print(f"Folder: {folder_remote} doesn't exist on host: {host}.")
        
        # Close connections
        sftp.close()
        ssh.close()

    return True

# Get command line arguments
args = command_line_arguments()

# Get master & workers hostnames
master, workers = check_node_input(args.nodes)

# Limit copies < workers, don't want to store more than 1 identical shard per worker.
if (args.copies+1 > len(workers)) :
    args.copies = len(workers)
else :
    args.copies +=1

# Create temporary directory locally on frontend.
tempDir = createTempDir('temp')

# Split input data and store locally on frontend.
file_splits = splitInput (args.input, args.splits)

print("\n(Complete) Input has been split and stored temporarily on disk.")

# Create nested default dict, and get filenames from the temporary directory.
files = os.listdir(tempDir)
dictionary = collections.defaultdict(lambda: collections.defaultdict(dict))

# Copy and distribute split input files over computers in cluster.
for index, file in enumerate(files):
    for copy in range(args.copies) :
        location, host = copyFiles (workers[(index + copy) % len(workers)], file)
        dictionary[file][f"Copy{copy}"]['host'] = host
        dictionary[file][f"Copy{copy}"]['location'] = location

print("(Complete) Split data has been distributed over cluster.")

# Save the shard locations to disk, then copy over to master node storage.
with open(tempDir + '/shard_dict.pickle', 'wb') as handle:
    pickle.dump(json.loads(json.dumps(dictionary)), handle, protocol=pickle.HIGHEST_PROTOCOL)
location1, host = copyFiles (master, 'shard_dict.pickle')

# Dictionary with map tasks
task_dict = dict.fromkeys(dictionary.keys(),None)
for i in task_dict.keys() :
    task_dict[i] = {'status': None, 'worker': None}

# Extend dictionary with reduce tasks
for p in range(args.partitions) :
    task_dict[f"Reduce{p}"] = {'status': None, 'worker': None, 'result_location': None}
with open(tempDir + '/task_dict.pickle', 'wb') as handle:
    pickle.dump(json.loads(json.dumps(task_dict)), handle, protocol=pickle.HIGHEST_PROTOCOL)
location2, host = copyFiles (master, 'task_dict.pickle')

# Dict with workers
worker_dict = dict.fromkeys(workers,None)
for d in worker_dict.keys() :
    worker_dict[d] = {'status': None}
with open(tempDir + '/worker_dict.pickle', 'wb') as handle:
    pickle.dump(json.loads(json.dumps(worker_dict)), handle, protocol=pickle.HIGHEST_PROTOCOL)
location3, host = copyFiles (master, 'worker_dict.pickle')

# Fork process
pid = os.fork()

# The parent process (master node)
if pid > 0 :
    process = subprocess.Popen(f"ssh {master} python3 ~/DDPS2/master.py {location1} {location2} {location3}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stder = process.communicate() # Blocking
    print("Stdout:",stdout.decode('ASCII'))
    print("Stderr:",stder)
    # Clean up all temporary files (locally and remote) after we are done.
    deleteTempDir (tempDir)
    removeTempRemote (workers)
    removeTempRemote ([master])
# # The created child process (worker nodes)
# else :
#     for worker in workers:
#         pid = os.fork()
#         if pid:
#             print(f"Child: {pid}")
#             process = subprocess.Popen(f"ssh {worker} python3 ~/DDPS2/helloworld.py {master}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#         else:
#             os._exit(0)