import os
import numpy as np
import argparse
import socket
import subprocess

# Parsing command line arguments
def command_line_arguments () :
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--nodes", help="E.g., node102,node103,node104")
    argparser.add_argument("--input_file", default= 'sequence.npy', help="E.g., sequence.npy")
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
        raise ValueError('No workers specified.')
    nodes = arguments.strip().split(',')
    for node in nodes :
        if not check_ssh(node) :
            raise ValueError(f"Can't connect to: {node}.")
    return nodes[0], nodes[1:]

# Split input file into different parts
def splitInput (filename, workers) :
    path = os.getcwd()
    worker_count = len(workers)
    if worker_count == 0 :
        raise ValueError('Not enough workers.')
    sequence = np.load(os.path.join(path, filename))
    return np.split(sequence,worker_count)

args = command_line_arguments()
master, workers = check_node_input(args.nodes)
file_splits = splitInput (args.input_file, workers)

pid = os.fork()

# The parent process 
if pid > 0 :
    stdout, stderr = subprocess.Popen(f"ssh {master} python3 ~/DDPS2/helloworld.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    output = stdout.read()
    print(output)
# The created child process
# else :
#     client = paramiko.SSHClient()
#     client.load_system_host_keys()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     client.connect(workers[0],22) # only 1 worker atm
    
#     #stdin, stdout, stderr = client.exec_command(f"python3 ~/DDPS2/worker.py --master {master}") 
#     stdin, stdout, stderr = client.exec_command(f"python3 ~/DDPS2/helloworld.py") 
#     output = stdout.read()
#     print(output)
#     client.close()