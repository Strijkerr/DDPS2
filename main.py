import os
import numpy as np
import argparse
import socket

# Parsing command line arguments
def command_line_arguments () :
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--nodes", help="E.g., node102,node103,node104 ")
    argparser.add_argument("--input_file", default= 'sequence.npy', help="E.g., node102,node103,node104 ")
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
    return nodes

# Split input file into different parts
def splitInput (filename, workers) :
    path = os.getcwd()
    worker_count = len(workers)
    if worker_count == 0 :
        raise ValueError('Not enough workers.')
    sequence = np.load(os.path.join(path, filename))
    splits = np.split(sequence,worker_count)
    return splits[0], splits[1:]

args = command_line_arguments()
master, workers = check_node_input(args.nodes)
file_splits = splitInput (args.input_file, workers)


# scp /path/to/file username@a:/path/to/destination


pid = os.fork()

# pid greater than 0 represents
# the parent process 
if pid > 0 :
    print("I am parent process:")
    print("Process ID:", os.getpid())
    print("Child's process ID:", pid)
  
# pid equal to 0 represents
# the created child process
else :
    print("\nI am child process:")
    print("Process ID:", os.getpid())
    print("Parent's process ID:", os.getppid())