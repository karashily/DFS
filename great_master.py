import zmq
from multiprocessing import Process,Value,Lock,Manager

data_keepers_ips = [
    "127.0.0.1:",
    "127.0.0.1:",
    "127.0.0.1"
]

num_ports_per_data_keeper = [5,3,4]

data_keepers_ports_ips = []

for j in range(3):
    for i in range(num_ports_per_data_keeper[j]):
        data_keepers_ports_ips.append(data_keepers_ips[j]+":551"+str(i))

    


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
    # TODO:find free port
    #port = get_free_port(ports_table)
    port = "5510"
    if (port == None):
        port = "7z sa3eed el mara el kadma"
        
    # send port to client
    socket.send_string(port)

    # wait for success from datakeeper
    success_port = port[:-2]+"2"+port[-1]
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    print(success_port)
    results_receiver.connect("tcp://127.0.0.1:"+success_port)
    success = results_receiver.recv_pyobj()

    # add file to table
    add_to_look_up_table(table,lock,msg["clientID"],msg["FileName"],port[:-5],True)

    # send done to client
    handshake = socket.recv_string()
    socket.send_pyobj(True)


def get_file_loc (filename):
    for i in range(len(table)):
        if(table[i][file_name] == filename and table[i]['is_data_node_alive'] == True):
            return table[data_node_number]



def download(msg, socket):
    pass
    # find file on which datanode
    loc = get_file_loc(msg[FileName])

    # get a free port of that machine

    # send not busy port to client
    socket.send_string(port)

    # wait for success from datakeeper
    success_port = port[:-2]+"2"+port[-1]
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    print(success_port)
    results_receiver.connect("tcp://127.0.0.1:"+success_port)
    success = results_receiver.recv_pyobj()

    # send done to client
    handshake = socket.recv_string()
    socket.send_pyobj(True)

def process(table, lock, master_process_port,ports_table):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://127.0.0.1:5500" )


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
            download(msg, socket)

def initialize_ports_table(ports_table,lock):
    for i in range(len(data_keepers_ports_ips)):
        d = {
            'ip': data_keepers_ports_ips[i],
            'free': True,
            'alive': True
        }
        lock.acquire()
        ports_table.append(d)
        lock.release()  



def get_free_port(ports_table):
    for i in range(len(ports_table)):
        if((ports_table[i]["free"]==True) and (ports_table[i]["alive"]==True)):
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
        table = manager.list()
        
        ports_table = manager.list()
        print(" manager list done ")


        '''
        initialize ports
        '''
        initialize_ports = Process(target = initialize_ports_table,args = (table,lock2))
        initialize_ports.start()
        initialize_ports.join()
        ''''''
        print("mian calling process")
        first_process = Process(target = process,args = (table,lock3,"5500",ports_table))
        first_process.start()
        first_process.join()

        # p = Process(target = test,args = (table,lock1,1,"dh el file name",3,"dh el file path",True))
        # p.start()
        # p.join()
        # print(table[0])


main()


