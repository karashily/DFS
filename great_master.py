import zmq
from multiprocessing import Process,Value,Lock,Manager
import datetime
import time

datakeepers_ips = [
    "tcp://127.0.0.1:",
    "tcp://127.0.0.1:",
    "tcp://127.0.0.1:"
]

master_own_ip = "tcp://127.0.0.1:"
master_alive_port = "5400"


ports_per_datakeeper = [1,0,0]

datakeepers_ports_ips = []

for j in range(3):
    for i in range(ports_per_datakeeper[j]):
        datakeepers_ports_ips.append(datakeepers_ips[j]+"551"+str(i))



def initialize_ports_table(ports_table, lock):
    for i in range(len(datakeepers_ports_ips)):
        d = {
            'ip': datakeepers_ports_ips[i],
            'free': True,
            'alive': True,
            'last_time_alive': datetime.datetime.now() - datetime.timedelta(seconds=5)
        }

        # d = Manager().dict() # create only 1 dict
        # d['ip'] = data_keepers_ports_ips[i]
        # d['free'] = True
        # d['alive'] = False
        # d['last_time_alive'] = datetime.datetime.now() - datetime.timedelta(seconds=5)

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
        




def add_to_look_up_table(files_table,files_table_lock,user_id,file_name,data_node_number,is_data_node_alive):
    lock.acquire()
    d = {
        "user_id" : user_id,
        "file_name" : file_name,
        "data_node_number" : data_node_number,
        "is_data_node_alive" : is_data_node_alive
    }
    files_table.append(d)
    lock.release()

def upload(files_table, files_table_lock, ports_table, msg ,socket):
    # find free port
    #port = get_free_port(ports_table, 'any')
    port = "tcp://127.0.0.1:5510"
    if (port == None):
        socket.send_string("fatal")
        return
        
    # send port to client
    socket.send_string(port)

    # wait for success from datakeeper
    success_port = port[:-2] + str(int(port[-2]) + 1) + port[-1]
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    print(success_port)
    results_receiver.connect(success_port)
    success = results_receiver.recv_pyobj()

    # add file to table
    add_to_look_up_table(files_table, files_table_lock, msg["clientID"], msg["FileName"], port[:-5], True)
    print(table)

    # send done to client
    #handshake = socket.recv_pyobj()
    #print(handshake)
    success_port_of_client = success['successPort']
    success_context = zmq.Context()
    success_socket = success_context.socket(zmq.PAIR)
    success_socket.connect(success_port_of_client)
    success_socket.send_pyobj(True)

    



def get_file_loc(table, filename):
    for i in range(len(table)):
        if(table[i]['file_name'] == filename and table[i]['is_data_node_alive'] == True):
            return table[i]['data_node_number']


def download(files_table, msg, ports_table, socket):
    # find file on which datanode
    loc = get_file_loc(files_table, msg['FileName'])

    # get a free port of that machine
    port = get_free_port(ports_table, loc)
    
    if(port is None):
        socket.send_string("fatal")
        return
    
    print(port)
    # send not busy port to client
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



def process(files_table, files_table_lock, master_process_port, ports_table):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://127.0.0.1:5500")


    while True:
        print("before receiving /n")
        #  Wait for next request from client
        msg = socket.recv_pyobj()
        #socket.send("World from %s" % port)
        print ("master got message:",msg)

        if msg['Type']==1:
            #socket.send_string(port)
            upload(files_table, files_table_lock, ports_table, msg, socket)
        elif msg['Type']==0:
            download(files_table, msg, ports_table, socket)



def get_free_port(ports_table, node):
    if(node == 'any'):
        print(ports_table,"ay bta3")
        for i in range(len(ports_table)):
           
            if((ports_table[i]["free"]==True) and (ports_table[i]["alive"]==True)):
                return ports_table[i]["ip"]

    else:
        for i in range(len(ports_table)):
            print("ip", ports_table[i]['ip'][:-5], "node", node)
            print(ports_table)
            if((ports_table[i]["free"]==True) and (ports_table[i]["alive"]==True) and (ports_table[i]['ip'][:-5] == node)):
                return ports_table[i]["ip"]

    return None



def main():
    with Manager() as manager:
        files_table_lock = Lock()
        ports_table_lock = Lock()
        # lock3 = Lock()
        # lock4 = Lock()
        # lock5 = Lock()

        files_table = manager.list()
        
        ports_table = manager.list()

        '''
        initialize ports table
        '''
        # initialize_ports = Process(target = initialize_ports_table, args = (ports_table,lock2))
        initialize_ports_table(ports_table, ports_table_lock)
        


        '''
        start processes
        '''
        
        first_process = Process(target=process, args=(files_table, files_table_lock, "5500", ports_table))
        
        alive_process = Process(target=alive, args=(files_table, ports_table, files_table_lock, ports_table_lock))
        dead_process = Process(target=undertaker, args=(files_table, ports_table, files_table_lock, ports_table_lock))


        # initialize_ports.start()
        # initialize_ports.join()

        first_process.start()

        '''
        process responsible for I am alive msgs from datakeepers
        '''
        alive_process.start()

        dead_process.start()


        # loging changes in ports table
        while True:
            print(ports_table)
            time.sleep(1)



        first_process.join()
        alive_process.join()
        dead_process.join()



main()


