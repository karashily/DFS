import zmq
from multiprocessing import Process,Value,Lock,Manager
import datetime


data_keepers_ips = [
    "tcp://127.0.0.1:",
    "tcp://127.0.0.1:",
    "tcp://127.0.0.1:"
]

master_own_ip = "tcp://127.0.0.1:"
master_alive_port = "5400"


num_ports_per_data_keeper = [3,0,0]

data_keepers_ports_ips = []

for j in range(3):
    for i in range(num_ports_per_data_keeper[j]):
        data_keepers_ports_ips.append(data_keepers_ips[j]+"551"+str(i))



def initialize_ports_table(ports_table, lock):
    for i in range(len(data_keepers_ports_ips)):
        d = {
            'ip': data_keepers_ports_ips[i],
            'free': True,
            'alive': False,
            'last_time_alive': datetime.datetime.now() - datetime.timedelta(seconds=5)
        }
        lock.acquire()
        ports_table.append(d)
        lock.release()  


def alive (table,ports_table,lock):
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    results_receiver.bind(master_own_ip + master_alive_port)
    while True:
        d = results_receiver.recv_string()
        lock.acquire()

        for i in range(len(ports_table)):
            x = ports_table[i]
            if (x['ip'][:-5] == d):
                x['alive'] = True
                x['last_time_alive'] = datetime.now() # time object

        for i in range(len(table)):
            x = table[i]
            if (x['data_node_number'][:-5] == d):
                x['is_data_node_alive'] = True

        lock.release()


def check_if_datakeeper_died(table,ports_table,lock):
    while True:
        lock.acquire()
        recently_dead_datakeepers = []
        for i in range(len(ports_table)):
            x = ports_table[i]
            if (((x['last_time_alive']-datetime.now()).total_seconds() > 2) and x['alive'] == True):
                recently_dead_datakeepers.append(x)
                x['alive'] = False


        for i in range(len(recently_dead_datakeepers)):
            x = recently_dead_datakeepers[i]['alive']
            if (not x):
                for j in range(len(table)):
                    if(table[j]['data_node_number'] == recently_dead_datakeepers[i]['ip']):
                        table[j]['is_data_node_alive'] = False

        lock.release()




def add_to_look_up_table(table,lock,user_id,file_name,data_node_number,is_data_node_alive):
    lock.acquire()
    d = {
        "user_id" : user_id,
        "file_name" : file_name,
        "data_node_number" : data_node_number,
        "is_data_node_alive" : is_data_node_alive
    }
    table.append(d)
    lock.release()

def upload(table, lock, ports_table, msg ,socket):
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
    add_to_look_up_table(table,lock,msg["clientID"],msg["FileName"],port[:-5],True)
    print(table)

    # send done to client
    #handshake = socket.recv_pyobj()
    #print(handshake)
    success_port_of_client = success['success_port']
    success_context = zmq.Context()
    success_socket = success_context.socket(zmq.PAIR)
    success_socket.connect(success_port_of_client)
    success_socket.send_pyobj(True)

    



def get_file_loc(table, filename):
    for i in range(len(table)):
        if(table[i]['file_name'] == filename and table[i]['is_data_node_alive'] == True):
            return table[i]['data_node_number']


def download(table, msg, ports_table, socket):
    # find file on which datanode
    loc = get_file_loc(table, msg['FileName'])

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
    handshake = socket.recv_pyobj()
    socket.send_pyobj(True)



def process(table, lock, master_process_port,ports_table):
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
            upload(table, lock,ports_table,  msg ,socket)
        elif msg['Type']==0:
            download(table, msg, ports_table, socket)



def get_free_port(ports_table, node):
    if(node == 'any'):
        print(ports_table,"ay bta3")
        for i in range(len(ports_table)):
           
            if((ports_table[i]["free"]==True) and (ports_table[i]["alive"]==True)):
                return ports_table[i]["ip"]

    else:
        for i in range(len(ports_table)):
            print("ip", ports_table[i]['ip'][:-5], "node", node)
            if((ports_table[i]["free"]==True) and (ports_table[i]["alive"]==True) and (ports_table[i]['ip'][:-5] == node)):
                return ports_table[i]["ip"]

    return None



def main():
    with Manager() as manager:
        print("mian calling process")
        lock1 = Lock()
        print("lock 1 ")
        lock2 = Lock()
        print("lock 2")
        lock3 = Lock()
        print("lock 3 ")
        lock4 = Lock()
        lock5 = Lock()

        table = manager.list()
        
        ports_table = manager.list()
        print(" manager list done ")


        '''
        initialize ports
        '''
        initialize_ports = Process(target = initialize_ports_table, args = (ports_table,lock2))
        initialize_ports.start()
        initialize_ports.join()
        ''''''
        print("main calling process")
        first_process = Process(target = process,args = (table,lock3,"5500",ports_table))
        first_process.start()


        '''
        process responsible for I am alive msgs from datakeepers
        '''
        alive_process = Process(target = alive,args = (table,ports_table,lock4))
        alive_process.start()

        dead_process = Process(target = check_if_datakeeper_died,args = (table,ports_table,lock5))
        dead_process.start()





        first_process.join()
        alive_process.join()
        dead_process.join()



main()


