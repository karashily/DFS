import zmq
from multiprocessing import Process,Value,Lock,Manager
import datetime
import time
import json
import os
import copy


datakeepers_ips = [
    "tcp://192.168.43.23:",
    "tcp://192.168.43.197:",
    "tcp://192.168.43.105:"
]

master_ports = [
    "tcp://192.168.43.105:5500",
    "tcp://192.168.43.105:5501",
    "tcp://192.168.43.105:5502"
]

master_own_ip = "tcp://192.168.43.105:"
master_alive_port = "5400"


ports_per_datakeeper = [3,0,0]

datakeepers_ports_ips = []

unique_files = []

for j in range(3):
    for i in range(ports_per_datakeeper[j]):
        datakeepers_ports_ips.append(datakeepers_ips[j]+"551"+str(i))


def initialize_ports_table(ports_table, lock):
    for i in range(len(datakeepers_ports_ips)):
        d = {
            'ip': datakeepers_ports_ips[i],
            'free': True,
            'alive': False,
            'last_time_alive': datetime.datetime.now() - datetime.timedelta(seconds=5)
        }

        lock.acquire()
        ports_table.append(d)
        lock.release()  


def alive(files_table, ports_table, files_table_lock, ports_table_lock):
    context = zmq.Context()
    results_receiver = context.socket(zmq.SUB)
    results_receiver.bind(master_own_ip + master_alive_port)
    results_receiver.setsockopt_string(zmq.SUBSCRIBE, "")

    while True:
        d = results_receiver.recv_string()
        
        # Update ports table
        ports_table_lock.acquire()
        for i in range(len(ports_table)):
            if (ports_table[i]['ip'][:-5] == d):
                d = {
                    'ip': ports_table[i]['ip'],
                    'free': ports_table[i]['free'],
                    'alive': True,
                    'last_time_alive': datetime.datetime.now()
                }
                ports_table.append(d)
                ports_table.remove(ports_table[i])
        ports_table_lock.release()

        # Update Files table
        files_table_lock.acquire()
        for i in range(len(files_table)):
            if (files_table[i]['data_node_number'][:-5] == d):
                d = {
                    "user_id" : files_table[j]['user_id'],
                    "file_name" : files_table[j]['file_name'],
                    "data_node_number" : files_table[j]['data_node_number'],
                    "is_data_node_alive" : True
                }
                files_table.append(d)
                files_table.remove(files_table(j))
        files_table_lock.release()

        # time.sleep(1)


def undertaker(files_table, ports_table, files_table_lock, ports_table_lock):
    while True:
        recently_dead_datakeepers = []
        
        # Update ports table
        ports_table_lock.acquire()
        for i in range(len(ports_table)):
            if (((datetime.datetime.now()-ports_table[i]['last_time_alive']).total_seconds() > 2) and ports_table[i]['alive'] == True):
                recently_dead_datakeepers.append(ports_table[i]['ip'])
                d = {
                    'ip': ports_table[i]['ip'],
                    'free': ports_table[i]['free'],
                    'alive': False,
                    'last_time_alive': ports_table[i]['last_time_alive']
                }
                ports_table.append(d)
                ports_table.remove(ports_table[i])
        ports_table_lock.release()

        # Update files Table
        files_table_lock.acquire()
        for i in range(len(recently_dead_datakeepers)):
            for j in range(len(files_table)):
                if(files_table[j]['data_node_number'] == recently_dead_datakeepers[i]):
                    d = {
                        "user_id" : files_table[j]['user_id'],
                        "file_name" : files_table[j]['file_name'],
                        "data_node_number" : files_table[j]['data_node_number'],
                        "is_data_node_alive" : False
                    }
                    files_table.append(d)
                    files_table.remove(files_table(j))
        files_table_lock.release()

        # time.sleep(1)

    
def acquire_port(ports_table, ports_table_lock, port):
    # Update ports table
    ports_table_lock.acquire()
    for i in range(len(ports_table)):
        if(ports_table[i]['ip'] == port):
            d = {
                'ip': ports_table[i]['ip'],
                'free': False,
                'alive': ports_table[i]['alive'],
                'last_time_alive': ports_table[i]['last_time_alive']
            }
            ports_table.append(d)
            ports_table.remove(ports_table[i])
            break
    ports_table_lock.release()


def release_port(ports_table, ports_table_lock, port):
    # Update ports table
    ports_table_lock.acquire()
    for i in range(len(ports_table)):
        if(ports_table[i]['ip'] == port):
            d = {
                'ip': ports_table[i]['ip'],
                'free': True,
                'alive': ports_table[i]['alive'],
                'last_time_alive': ports_table[i]['last_time_alive']
            }
            ports_table.append(d)
            ports_table.remove(ports_table[i])
            break
    ports_table_lock.release()


def add_to_files_table(files_table,lock,user_id,file_name,data_node_number,is_data_node_alive):
    lock.acquire()
    d = {
        "user_id" : user_id,
        "file_name" : file_name,
        "data_node_number" : data_node_number,
        "is_data_node_alive" : is_data_node_alive
    }
    files_table.append(d)

    with open('files.txt', 'a') as fout:
        json.dump(copy.deepcopy(files_table), fout)

    

    lock.release()


def get_datakeepers_of_file(files_table,file_name):
    datakeepers = set()
    for i in range(len(files_table)):
        if ((files_table[i]['file_name'] == file_name) and (files_table[i]['is_data_node_alive'] == True)):
            datakeepers.add(files_table[i]['data_node_number'][:-5])
    datakeepers = list(datakeepers)
    return datakeepers

def replicate_file(files_table, files_table_lock,ports_table,ports_table_lock, file, num_of_replicates):
    datakeepers = get_datakeepers_of_file(files_table,file_name)
    if(len(datakeepers)>=num_of_replicates):
        return

    num_of_replicates = num_of_replicates-len(datakeepers)

    
    list_dks_without_files = list(set(datakeepers_ips)-set(datakeepers))
    i = 0
    while ((num_of_replicates > 0) and (i < len(list_dks_without_files))):
        port_dk_to_rep_file_on = get_free_port(ports_table, list_dks_without_files[i])
        i += 1
        if (port_dk_to_rep_file_on == None):
            continue


        #set port_dk_to_rep_file_on busy
        acquire_port(ports_table, ports_table_lock, port_dk_to_rep_file_on)

        #create msg
        msg = {
                "fileName":file["file_name"],
                "ip":port_dk_to_rep_file_on
            }
        
        #send msg
        port = "5200"
        context = zmq.Context()
        socket = context.socket(zmq.PAIR)
        socket.connect(datakeepers[0] + port)
        socket.send_pyobj(msg)

        # wait for success from datakeeper
        success_port = port_dk_to_rep_file_on[:-2] + str(int(port_dk_to_rep_file_on[-2]) + 1) + port_dk_to_rep_file_on[-1]
        context = zmq.Context()
        results_receiver = context.socket(zmq.PULL)
        print(success_port)
        results_receiver.connect(success_port)
        success = results_receiver.recv_pyobj()

        # add file to table
        add_to_files_table(files_table, files_table_lock, file["user_id"], file["file_name"], port_dk_to_rep_file_on[:-5], True)


        #set port_dk_to_rep_file_on free
        release_port(ports_table, ports_table_lock, port_dk_to_rep_file_on)


        num_of_replicates -= 1


def replicate(files_table, files_table_lock, ports_table, ports_table_lock, num_of_replicates):
    while True:
        for i in range(len(unique_files)):
            replicate_file(files_table, files_table_lock, ports_table, ports_table_lock, unique_files_names[i], num_of_replicates)
        time.sleep(5)


def upload(files_table, files_table_lock, ports_table, ports_table_lock, msg, socket):
    # checking that file isn't uploaded already
    files_table_lock.acquire()
    for i in files_table:
        if(msg["FileName"] == i['file_name']):
            socket.send_string("filename_exists_already")
            files_table_lock.release()
            return
    files_table_lock.release()

    # find free port
    port = get_free_port(ports_table, ports_table_lock, 'any')
    # port = "tcp://127.0.0.1:5510"
    
    if(port is None):
        socket.send_string("no_free_ports")
        return
        
    # send port to client
    acquire_port(ports_table, ports_table_lock, port)
    socket.send_string(port)

    # wait for success from datakeeper
    success_port = port[:-2] + str(int(port[-2]) + 1) + port[-1]
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    print(success_port)
    results_receiver.connect(success_port)
    success = results_receiver.recv_pyobj()

    print("got success")
    # add file to table
    add_to_files_table(files_table, files_table_lock, msg["clientID"], msg["FileName"], port[:-5], True)
    m = {
        "file_name":msg["FileName"],
        "user_id":msg["clientID"]
    }
    unique_files.append(m)
    print(files_table)

    # send done to client
    #handshake = socket.recv_pyobj()
    #print(handshake)
    success_port_of_client = success['successPort']
    success_context = zmq.Context()
    success_socket = success_context.socket(zmq.PAIR)
    success_socket.connect(success_port_of_client)
    success_socket.send_pyobj(True)

    release_port(ports_table, ports_table_lock, port)


def get_file_loc(files_table, files_table_lock, filename):
    files_table_lock.acquire()
    for i in range(len(files_table)):
        if(files_table[i]['file_name'] == filename and files_table[i]['is_data_node_alive'] == True):
            files_table_lock.release()
            return files_table[i]['data_node_number']
    files_table_lock.release()


def download(files_table, files_table_lock, ports_table, ports_table_lock, msg, socket):
    # find file on which datanode
    loc = get_file_loc(files_table, files_table_lock, msg['FileName'])

    if(loc is None):
        socket.send_string("file_not_found")
        return
    
    # get a free port of that machine
    port = get_free_port(ports_table, ports_table_lock, loc)
    
    
    if(port is None):
        socket.send_string("no_free_ports")
        return
    
    # print(port)
    # send not busy port to client
    acquire_port(ports_table, ports_table_lock, port)
    socket.send_string(port)

    # wait for success from datakeeper
    success_port = port[:-2] + str(int(port[-2]) + 1) + port[-1]
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    print("success port",success_port)
    results_receiver.connect(success_port)
    success = results_receiver.recv_pyobj()

    # send done to client
    success_port_of_client = success['successPort']
    success_context = zmq.Context()
    success_socket = success_context.socket(zmq.PAIR)
    success_socket.connect(success_port_of_client)
    success_socket.send_pyobj(True)

    release_port(ports_table, ports_table_lock, port)



def process(files_table, files_table_lock, ports_table, ports_table_lock, master_process_port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(master_process_port)


    while True:
        print("before receiving /n")
        #  Wait for next request from client
        msg = socket.recv_pyobj()
        #socket.send("World from %s" % port)
        print ("master got message:",msg)

        if msg['Type']==1:
            #socket.send_string(port)
            upload(files_table, files_table_lock, ports_table, ports_table_lock, msg, socket)
        elif msg['Type']==0:
            download(files_table, files_table_lock, ports_table, ports_table_lock, msg, socket)



def get_free_port(ports_table, ports_table_lock, node):
    ports_table_lock.acquire()
    for i in range(len(ports_table)):
        if((ports_table[i]["free"]==True) and (ports_table[i]["alive"]==True) and ((ports_table[i]['ip'][:-5] == node) or node == 'any')):
            ports_table_lock.release()
            return ports_table[i]["ip"]
    ports_table_lock.release()
    return None



def main():
    with Manager() as manager:

        num_of_replicates = int(input("minimum number of replicates: "))
        
        files_table_lock = Lock()
        ports_table_lock = Lock()

        files_table = manager.list()  
        ports_table = manager.list()

        '''
        initialize ports table
        '''
        # initialize_ports = Process(target = initialize_ports_table, args = (ports_table,lock2))
        initialize_ports_table(ports_table, ports_table_lock)
        


        '''
        initialize files table
        '''
        file_exists = os.path.isfile('./files.txt')    
        if(file_exists):
            with open('files.txt', 'rb') as fin:
                files_table = json.load(fin)
                files_table = manager.list(files_table)
        else:
            f= open("files.txt", "w+")

        '''
        start processes
        '''
        
        p1 = Process(target=process, args=(files_table, files_table_lock, ports_table, ports_table_lock, master_ports[0]))
        p2 = Process(target=process, args=(files_table, files_table_lock, ports_table, ports_table_lock, master_ports[1]))
        p3 = Process(target=process, args=(files_table, files_table_lock, ports_table, ports_table_lock, master_ports[2]))
        
        alive_process = Process(target=alive, args=(files_table, ports_table, files_table_lock, ports_table_lock))
        dead_process = Process(target=undertaker, args=(files_table, ports_table, files_table_lock, ports_table_lock))

        replicate_process = Process(target=replicate, args=(files_table, files_table_lock, ports_table, ports_table_lock, num_of_replicates))

        # initialize_ports.start()
        # initialize_ports.join()

        p1.start()
        p2.start()
        p3.start()

        '''
        process responsible for I am alive msgs from datakeepers
        '''
        alive_process.start()

        dead_process.start()


        # loging changes in ports table
        # while True:
        #     print(ports_table)
        #     time.sleep(1)

        p1.join()
        p2.join()
        p3.join()
        alive_process.join()
        dead_process.join()



main()


