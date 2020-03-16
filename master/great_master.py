import zmq
from multiprocessing import Process,Value,Lock,Manager
import datetime
import time
import json
import os
import copy


datakeepers_ips = [
    "tcp://10.147.17.156:",
    "tcp://10.147.17.209:",
    "tcp://10.147.17.54:"
]

master_own_ip = "tcp://10.147.17.156:"

master_ports = [
    master_own_ip+"5500",
    master_own_ip+"5501",
    master_own_ip+"5502"
]

master_alive_port = "5400"


ports_per_datakeeper = [3,3,0]

datakeepers_ports_ips = []

# unique_files = []

for j in range(3):
    for i in range(ports_per_datakeeper[j]):
        datakeepers_ports_ips.append(datakeepers_ips[j]+"551"+str(i))


def initialize_ports_table(ports_table, lock):
    #log
    init = open("init.txt", "a")
    init.write("Master ["+str(int(time.time()))+"] "+"datakeepers ports initialized: \n")
    init.close()

    for i in range(len(datakeepers_ports_ips)):
        d = {
            'ip': datakeepers_ports_ips[i],
            'free': True,
            'alive': False,
            'last_time_alive': datetime.datetime.now() - datetime.timedelta(seconds=5)
        }

        #log
        init = open("init.txt", "a")
        init.write("Master ["+str(int(time.time()))+"] "+str(d)+'\n')
        init.close()

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
                # print(ports_table)
                d1 = {
                    'ip': ports_table[i]['ip'],
                    'free': ports_table[i]['free'],
                    'alive': True,
                    'last_time_alive': datetime.datetime.now()
                }
                ports_table.append(d1)
                ports_table.remove(ports_table[i])
        ports_table_lock.release()

        # Update Files table
        files_table_lock.acquire()
        for i in range(len(files_table)):
            if ((files_table[i]['data_node_number'] == d)and (files_table[i]['is_data_node_alive'] == False)):
                # print(d,files_table[i]['data_node_number'])
                d2 = {
                    "user_id" : files_table[i]['user_id'],
                    "file_name" : files_table[i]['file_name'],
                    "data_node_number" : files_table[i]['data_node_number'],
                    "is_data_node_alive" : True
                }
                files_table.append(d2)
                files_table.remove(files_table[i])
                with open('files.txt', 'w') as fout:
                    json.dump(copy.deepcopy(files_table), fout)
        files_table_lock.release()



def undertaker(files_table, ports_table, files_table_lock, ports_table_lock):
    while True:
        recently_dead_datakeepers = []
        
        # Update ports table
        ports_table_lock.acquire()
        # print(ports_table)
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


                #log
                datakeepers_death = open("datakeepers_death.txt", "a")
                datakeepers_death.write("Master ["+str(int(time.time()))+"] "+"data keeper "+ports_table[i]['ip']+" has died \n")
                datakeepers_death.close()
                
        ports_table_lock.release()

        # Update files Table
        files_table_lock.acquire()
        for i in range(len(recently_dead_datakeepers)):
            for j in range(len(files_table)):
                if(files_table[j]['data_node_number'] == recently_dead_datakeepers[i][:-5]):
                    d = {
                        "user_id" : files_table[j]['user_id'],
                        "file_name" : files_table[j]['file_name'],
                        "data_node_number" : files_table[j]['data_node_number'],
                        "is_data_node_alive" : False
                    }
                    files_table.append(d)
                    files_table.remove(files_table[j])
                    with open('files.txt', 'w') as fout:
                        json.dump(copy.deepcopy(files_table), fout)
        files_table_lock.release()

        


        # time.sleep(1)

    
def acquire_port(ports_table, ports_table_lock, port):
    # Update ports table
    # ports_table_lock.acquire()
    print("acquire: ",port)
    for i in range(len(ports_table)):
        if(ports_table[i]['ip'] == port):
            d = {
                'ip': ports_table[i]['ip'],
                'free': False,
                'alive': ports_table[i]['alive'],
                'last_time_alive': ports_table[i]['last_time_alive']
            }
            ports_table.remove(ports_table[i])
            ports_table.append(d)

            
            #log
            ports_log = open("ports_log.txt", "a")
            ports_log.write("Master ["+str(int(time.time()))+"] "+"port# "+port+" is requested"+"\n")
            # ports_log.write(str(ports_table)+"\n")
            ports_log.close()

            break
    # ports_table_lock.release()



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
            ports_table.remove(ports_table[i])
            ports_table.append(d)
            
            #log
            ports_log = open("ports_log.txt", "a")
            ports_log.write("Master ["+str(int(time.time()))+"] "+"port# "+port+" is released"+"\n")

            # ports_log.write(str(ports_table)+"\n")
            ports_log.close()

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

    with open('files.txt', 'w') as fout:
        json.dump(copy.deepcopy(files_table), fout)

    

    lock.release()


def get_datakeepers_of_file(files_table,file_name):
    datakeepers = set()
    for i in range(len(files_table)):
        if ((files_table[i]['file_name'] == file_name) and (files_table[i]['is_data_node_alive'] == True)):
            datakeepers.add(files_table[i]['data_node_number']+":")
    datakeepers = list(datakeepers)
    return datakeepers

def replicate_file(files_table, files_table_lock,ports_table,ports_table_lock, file, num_of_replicates):
    datakeepers = get_datakeepers_of_file(files_table,file['file_name'])
    if(len(datakeepers)>=num_of_replicates):
        return

    num_of_replicates = num_of_replicates-len(datakeepers)

    list_dks_without_files = list(set(datakeepers_ips)-set(datakeepers))
    i = 0
    while ((num_of_replicates > 0) and (i < len(list_dks_without_files)) and (len(datakeepers)>0)):
        port_dk_to_rep_file_on = get_free_port(ports_table, ports_table_lock, list_dks_without_files[i])
        
        
        i += 1
        if (port_dk_to_rep_file_on == None):
            continue

        #create msg
        msg = {
                "fileName":file["file_name"],
                "ip":port_dk_to_rep_file_on
            }
        

        #log
        replicate_log = open("replicate_log.txt", "a")
        replicate_log.write("Master ["+str(int(time.time()))+"] "+"file: "+msg["fileName"]+ " will be replicated to "+msg["ip"]+" from "+datakeepers[0] +"\n")
        replicate_log.close()

        
        
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
        results_receiver.connect(success_port)
        success = results_receiver.recv_pyobj()

        # add file to table
        add_to_files_table(files_table, files_table_lock, file["user_id"], file["file_name"], port_dk_to_rep_file_on[:-5], True)


        #set port_dk_to_rep_file_on free
        release_port(ports_table, ports_table_lock, port_dk_to_rep_file_on)


        num_of_replicates -= 1


def replicate(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, num_of_replicates):
    while True:
        for i in range(len(unique_files)):
            unique_files_lock.acquire()
            replicate_file(files_table, files_table_lock, ports_table, ports_table_lock, unique_files[i], num_of_replicates)
            unique_files_lock.release()
            #print("unique_files")
        time.sleep(5)


def upload(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, msg, socket):
    # checking that file isn't uploaded already
    files_table_lock.acquire()
    for i in files_table:
        if(msg["fileName"] == i['file_name']):
            socket.send_string("filename_exists_already")
            files_table_lock.release()
            return
    files_table_lock.release()

    # find free port
    port = get_free_port(ports_table, ports_table_lock, 'any')
    print("awl ma gbth",port)
    
    
    if(port is None):
        socket.send_string("no_free_ports")
        return
    

    # send port to client
    socket.send_string(port)


    #log
    upload_log = open("upload_log.txt", "a")
    upload_log.write("Master ["+str(int(time.time()))+"] "+"file: "+msg["fileName"]+ " will be uploaded to "+port+"\n")
    upload_log.close()

    # wait for success from datakeeper
    success_port = port[:-2] + str(int(port[-2]) + 1) + port[-1]
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    results_receiver.connect(success_port)
    success = results_receiver.recv_pyobj()


    #log
    upload_log = open("upload_log.txt", "a")
    upload_log.write("Master ["+str(int(time.time()))+"] "+"got success from datakeeper: "+ port+"\n")
    upload_log.close()

    # add file to table
    add_to_files_table(files_table, files_table_lock, msg["clientID"], msg["fileName"], port[:-5], True)
    m = {
        "file_name":msg["fileName"],
        "user_id":msg["clientID"]
    }
    unique_files_lock.acquire()
    unique_files.append(m)
    unique_files_lock.release()


    # send done to client
    success_port_of_client = success['successPort']
    success_context = zmq.Context()
    success_socket = success_context.socket(zmq.PAIR)
    success_socket.connect(success_port_of_client)
    success_socket.send_pyobj(True)

    #log
    upload_log = open("upload_log.txt", "a")
    upload_log.write("Master ["+str(int(time.time()))+"] "+"sent success message to client: "+ success_port_of_client+"\n")
    upload_log.close()

    print("abl ma a3ml release",port)
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
    loc = get_file_loc(files_table, files_table_lock, msg['fileName'])

    if(loc is None):
        socket.send_string("file_not_found")
        return
    
    loc += ":"
    # get a free port of that machine
    port = get_free_port(ports_table, ports_table_lock, loc)
    
    # print(port)

    if(port is None):
        socket.send_string("no_free_ports")
        return
    
    # send not busy port to client
    socket.send_string(port)

    #log
    download_log = open("download_log.txt", "a")
    download_log.write("Master ["+str(int(time.time()))+"] "+"file: "+msg["fileName"]+ " will be downloaded from "+port+"\n")
    download_log.close()

    # wait for success from datakeeper
    success_port = port[:-2] + str(int(port[-2]) + 1) + port[-1]
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    results_receiver.connect(success_port)
    success = results_receiver.recv_pyobj()

    #log
    download_log = open("download_log.txt", "a")
    download_log.write("Master ["+str(int(time.time()))+"] "+"got success from datakeeper: "+ port+"\n")
    download_log.close()

    # send done to client
    success_port_of_client = success['successPort']
    success_context = zmq.Context()
    success_socket = success_context.socket(zmq.PAIR)
    success_socket.connect(success_port_of_client)
    success_socket.send_pyobj(True)

     #log
    download_log = open("download_log.txt", "a")
    download_log.write("Master ["+str(int(time.time()))+"] "+"sent success message to client: "+ success_port_of_client+"\n")
    download_log.close()

    release_port(ports_table, ports_table_lock, port)



def process(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, master_process_port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(master_process_port)


    while True:
        msg = socket.recv_pyobj()

        if msg['Type']==1:
            upload(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, msg, socket)
        elif msg['Type']==0:
            download(files_table, files_table_lock, ports_table, ports_table_lock, msg, socket)



def get_free_port(ports_table, ports_table_lock, node):
    ports_table_lock.acquire()
    for i in range(len(ports_table)):
        # print(ports_table[i])
        if((ports_table[i]["free"]==True) and (ports_table[i]["alive"]==True) and ((ports_table[i]['ip'][:-4] == node) or node == 'any')):
            port = copy.deepcopy(ports_table[i]['ip'])
            acquire_port(ports_table, ports_table_lock, ports_table[i]["ip"])
            ports_table_lock.release()
            # print("found port", ports_table[i]["ip"])
            return port
    
    ports_table_lock.release()
    return None



def main():
    with Manager() as manager:
        '''
        create logs files
        '''
        init = open("init.txt", "w+")
        datakeepers_death = open("datakeepers_death.txt", "w+")


        ''''''

        num_of_replicates = int(input("minimum number of replicates: "))
        
        files_table_lock = Lock()
        ports_table_lock = Lock()
        unique_files_lock = Lock()

        files_table = manager.list()  
        ports_table = manager.list()
        unique_files = manager.list()

        '''
        initialize ports table
        '''
        initialize_ports_table(ports_table, ports_table_lock)
        


        '''
        initialize files table
        '''
        file_exists = os.path.isfile('./files.txt') 

        if(file_exists):
            if(os.stat('files.txt').st_size != 0):
                with open('files.txt', 'rb') as fin:
                    files_table = json.load(fin)
                    files_table = manager.list(files_table)
        else:
            f= open("files.txt", "w+")

        for i in range(len(files_table)):
            m = {
                    "file_name":files_table[i]["file_name"],
                    "user_id":files_table[i]["user_id"]
                }
            unique_files.append(m)
        
        '''
        start processes
        '''
        p1 = Process(target=process, args=(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, master_ports[0]))
        p2 = Process(target=process, args=(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, master_ports[1]))
        p3 = Process(target=process, args=(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, master_ports[2]))
        
        alive_process = Process(target=alive, args=(files_table, ports_table, files_table_lock, ports_table_lock))
        dead_process = Process(target=undertaker, args=(files_table, ports_table, files_table_lock, ports_table_lock))

        replicate_process = Process(target=replicate, args=(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, num_of_replicates))

       
        p1.start()
        p2.start()
        p3.start()

        '''
        process responsible for I am alive msgs from datakeepers
        '''
        alive_process.start()

        dead_process.start()

        replicate_process.start()

        # loging changes in ports table
        # while True:
        #     print(ports_table)
        #     time.sleep(1)

        p1.join()
        p2.join()
        p3.join()
        alive_process.join()
        dead_process.join()
        replicate_process.join()



main()

