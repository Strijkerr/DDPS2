import socket
import sys
import pickle
import threading
from _thread import *

def returnDict (filename) :
    infile = open(filename,'rb')
    dictionary = pickle.load(infile)
    infile.close()
    return dictionary

def checkMapTaskComplete () :
    for task in map_task_dict.keys() :
        if not (map_task_dict[task]['status'] == 'done') :
            return False
    return True

def taskComplete (task, worker, result_location) :
    worker_dict[worker] = None
    map_task_dict[task]['status'] = 'done'
    map_task_dict[task]['worker'] = worker
    map_task_dict[task]['result_location'] = result_location

def findFreeMapTask (worker) :
    for task in map_task_dict.keys() :
        if (map_task_dict[task]['status'] == None) :
            for i in shard_dict[task].keys() :
                if (shard_dict[task][i]['host'] == worker) :
                    map_task_dict[task]['status'] = 'in-progress'
                    map_task_dict[task]['worker'] = worker
                    worker_dict[worker] = 'busy'
                    return task, shard_dict[task][i]['location']
    # False if all map tasks have a status of not None (in-progress or done)
    # Also false if worker does not have any available map task data locally.
    return False, False

def on_new_client(conn):
    worker = ''
    # Receive client identity
    try : 
        worker = conn.recv(1024).decode()
        #print("Worker connected:",worker)
    except Exception as e:
        print(f"[!] Error: {e}")

    # Main while loop
    while not checkMapTaskComplete() :
        task, tasklocation = findFreeMapTask(worker)
        if not task :
            print(f"Exit: {worker} thread.")
            break

        # Send task
        try:
            conn.send(tasklocation.encode())
        except Exception as e:
            print(f"[!] Error: {e}")
        else:
            print(f"Master.py {worker} task: {task}")
        
        # Get task response
        try : 
            msg = conn.recv(1024).decode()
        except Exception as e:
            print(f"[!] Error: {e}")
        else :
            print(f"Master.py {worker} reply: {msg}")
            taskComplete (task, worker, msg)
    
    # Send 'done' signal, this indicates all map tasks are completed.
    try:
        conn.send('done'.encode())
    except Exception as e:
        print(f"[!] Error: {e}")
    
    # Double check
    print(checkMapTaskComplete())

    # While loop for reduce tasks.
    conn.close()

def server_program(client_count):
    host = socket.gethostname()
    port = 56609
    server_socket = socket.socket()
    server_socket.bind((host, port)) 
    server_socket.listen(client_count)
    threads = []
    while True and len(threads) != client_count:
        conn, address = server_socket.accept()
        t = threading.Thread(target=on_new_client, args=(conn,))
        #t.daemon = True # If thread is daemon is can still run if main thread has ended. A non-daemon task blocks the main thread from ending.
        t.start()
        threads.append(t)

    # Wait for threads to finish (~10s)
    for t in threads :
        t.join()
    server_socket.close()
    print("Master.py exit")

shard_dict = returnDict(sys.argv[1])
map_task_dict = returnDict(sys.argv[2])
reduce_task_dict = returnDict(sys.argv[3])
worker_dict = returnDict(sys.argv[4])

server_program(len(worker_dict))