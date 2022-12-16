import socket
import sys
import pickle
import threading
from _thread import *
import time
import json

# Load dictionary from file into variable, and return.
def returnDict (filename) :
    infile = open(filename,'rb')
    dictionary = pickle.load(infile)
    infile.close()
    return dictionary

# Check if all map/reduce tasks have been completed.
def checkTaskComplete (dictionary) :
    for task in dictionary.keys() :
        if not (dictionary[task]['status'] == 'done') :
            return False
    return True

# Find a free map task, only assigns task to worker which has input shard in local storage.
# !!! If copies = 2, and two workers with the same copy fail, then the whole process fails
def findFreeMapTask (worker) :

    # Loop over tasks and find a task that is not assigned yet.
    for task in map_task_dict.keys() :
        if (map_task_dict[task]['status'] == None) :

            # Find task that has the input stored locally on worker, update statuses in dictionaries after.
            for copy in shard_dict[task].keys() :
                if (shard_dict[task][copy]['host'] == worker) :
                    map_task_dict[task]['status'] = 'in-progress'
                    map_task_dict[task]['worker'] = worker
                    worker_dict[worker] = 'busy'
                    location = {'location' : shard_dict[task][copy]['location'], 'partition' : map_task_dict[task]['partition']}
                    return task, location #shard_dict[task][copy]['location']

    # False if all map tasks have a status of not None (in-progress or done)
    # Also false if worker does not have any available map task data locally.
    return False, False

# Find a free reduce task, based on `first come first serve` principle, update statuses in dictionaries after.
def findFreeReduceTask (worker) :
    for task in reduce_task_dict.keys() :
        if (reduce_task_dict[task]['status'] == None) :
            reduce_task_dict[task]['status'] = 'in-progress'
            reduce_task_dict[task]['worker'] = worker
            worker_dict[worker] = 'busy'
            return task, reduce_task_dict[task]['index']

    # False if all reduce tasks have a status of not None (in-progress or done)
    return False, False

# After all map tasks are finished, get locations of intermediate results.
def getMapResultLocations (index) :
    locations = {'locations' : {}, 'partition' : index}
    for task in map_task_dict.keys() :
        if (map_task_dict[task]['partition'] == index) :
            locations['locations'][map_task_dict[task]['result_location']] = map_task_dict[task]['worker']
    return locations

# Update dictionaries after task has been completed.
def taskComplete (task, worker, result_location, dictionary) :
    worker_dict[worker] = None
    dictionary[task]['status'] = 'done'
    dictionary[task]['worker'] = worker
    dictionary[task]['result_location'] = result_location

# Client interaction function.
def on_new_client(conn):
    
    # Receive client identity.
    worker = ''
    try : 
        worker = conn.recv(1024).decode()
        print("Worker connected:",worker)
    except Exception as e:
        print(f"[!] Error: {e}")

    # Mapping stage loop.
    while not checkTaskComplete (map_task_dict) :
        task, tasklocation = findFreeMapTask(worker)
        if not task :
            break
        
        # Turn to json
        tasklocation = json.dumps(tasklocation)

        # Send mapping task to worker.
        try:
            conn.send(tasklocation.encode())
        except Exception as e:
            print(f"[!] Error: {e}")
        else:
            print(f"Mapping task sent to {worker}")
        
        # Get mapping task result location from worker. Update statuses in dictionaries after.
        try : 
            msg = conn.recv(1024).decode()
        except Exception as e:
            print(f"[!] Error: {e}")
        else :
            print(f"Mapping task completed by {worker}")
            taskComplete (task, worker, msg, map_task_dict)
    
    # Send 'done' signal, this indicates that all map tasks are completed.
    try:
        conn.send('done'.encode())
    except Exception as e:
        print(f"[!] Error: {e}")
    
    # Lazy approach to sync threads.
    while not checkTaskComplete (map_task_dict) :
        time.sleep(1)

    # Reduce task loop.
    while not checkTaskComplete (reduce_task_dict) :
        task, index = findFreeReduceTask(worker)

        # Exit if no free reduce tasks.
        if not task :
            try : 
                conn.send('done'.encode())
            except Exception as e:
                print(f"[!] Error: {e}")
            break

        # Get mapping task result locations based on the partition index.
        locations = getMapResultLocations(index)
        locations = json.dumps(locations)

        # Send reduce task.
        try:
            conn.send(locations.encode())
        except Exception as e:
            print(f"[!] Error: {e}")
        else:
            print(f"Reduce task sent to {worker}")
        
        # Get task response
        try : 
            msg = conn.recv(1024).decode()
        except Exception as e:
            print(f"[!] Error: {e}")
        else :
            print(f"Reduce completed by {worker}")
            taskComplete (task, worker, msg, reduce_task_dict)
        
    # Send 'done' signal, this indicates that all reduce tasks are completed.
    try:
        conn.send('done'.encode())
    except Exception as e:
        print(f"[!] Error: {e}")
    
    # Lazy approach to sync threads.
    while not checkTaskComplete (reduce_task_dict) :
        time.sleep(1)
        
    # End interaction with client.
    conn.close()

# Server program which creates a server socket, then it creates a thread for each connected client (worker).
def server_program(client_count):

    print('Server output log:\n')

    # Create server socket
    host = socket.gethostname()
    port = 56609
    server_socket = socket.socket()
    server_socket.bind((host, port)) 
    server_socket.listen(client_count)
    threads = []

    # Create connections until all workers have connected. Create thread for each client.
    while True and len(threads) != client_count:
        conn, address = server_socket.accept()
        t = threading.Thread(target=on_new_client, args=(conn,))
        #t.daemon = True # If thread is daemon is can still run if main thread has ended. A non-daemon task blocks the main thread from ending.
        t.start()
        threads.append(t)

    # (Sync) Wait for threads to finish.
    for t in threads :
        t.join()

    # Exit server program.
    server_socket.close()
    print("\nServer exits.")

# Get dictionaries with information about shard locations, mapping tasks, reduce tasks and workers.
shard_dict = returnDict(sys.argv[1])
map_task_dict = returnDict(sys.argv[2])
reduce_task_dict = returnDict(sys.argv[3])
worker_dict = returnDict(sys.argv[4])

# Start master node server.
server_program(len(worker_dict))