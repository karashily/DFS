# -*- coding: utf-8 -*-
"""
Created on Sat Mar  7 13:07:34 2020

@author: somar
"""
import zmq
import os
import time
from multiprocessing import Process

connectionPort="tcp://127.0.0.1:"
class DataKeeper:
    i_Am_Alive_port="5400"
    replicationPort="5200"
    recvReplica=""#my own ip +port
    #clientport = "5510"
    # mastersuccessport = port[:-2] + str(int(port[-2]) + 1) + port[-1]
    context = zmq.Context()
    def __init__(self, ID, port):
        self.ID = ID
        self.clientport=port
        self.mastersuccessport = port[:-2] + str(int(port[-2]) + 1) + port[-1]
 #################################       
    def HeartBeat(self):
        socket = self.context.socket(zmq.PUB)
        socket.connect(connectionPort+self.i_Am_Alive_port)
        while True:
            #topic = random.randrange(9999,10005)
            messagedata = connectionPort[:-1]
            socket.send_string(messagedata)
            time.sleep(1)
            
 #####################################           
    def UploadFile(self,message):
        name=message['fileName']
        print(name+"/n")
        file=message['File']
#        myfolder="keeper no. %d folder" %self.ID    
#        if not os.path.exists(myfolder):
#            os.makedirs(myfolder)
        
        # with open(os.path.join(myfolder, name), 'rb') as f:
        #     f.write(file)
        
#        filepath = os.path.join(myfolder,name)
        f=open(name,'wb')
        f.write(file)
        f.close()
        
        print("datakeeper:video %s added on machine no %d successfully ^_^ /n" %(name,self.ID))
        return True
 ######################################
    def DownloadFile(self,message,socket):
        print("d5lt el download")
        toBeDownloaded=message
        fileName=toBeDownloaded['fileName']
        f=open(fileName,"rb")
        v=f.read()
        downloadedVideo={'File':v,'fileName':fileName}
        socket.send_pyobj(downloadedVideo)
        print("video downloaded ^_^ /n")
        f.close()
        return True
 ###############################
    def ConnectToClient(self):
        socket = self.context.socket(zmq.PAIR)
        socket.bind(connectionPort+self.clientport)
        mastersocket = self.context.socket(zmq.PUSH)
        mastersocket.bind(connectionPort+self.mastersuccessport) 
        while True:
            message=socket.recv_pyobj()
            print("keeper  received  from client /n")
            Type=message['Type']
            success=False
            clientSuccessPort=message['successport']
            if(Type==1):
                    success= self.UploadFile(message)
                    if(success):
                        msg={'success':True,'successPort':clientSuccessPort}
                        mastersocket.send_pyobj(msg)
                        
            else:
                success=self.DownloadFile(message,socket)
                if(success):
                    # mastersocket = self.context.socket(zmq.PUSH)
                    # mastersocket.bind(connectionPort+self.mastersuccessport)
                    msg={'success':True,'successPort':clientSuccessPort}
                    mastersocket.send_pyobj(msg)
    def SendReplica(self):
        master_socket = self.context.socket(zmq.PAIR)
        master_socket.bind(connectionPort+self.replicationPort)
        #mastersocket = self.context.socket(zmq.PUSH)
        #mastersocket.bind(connectionPort+self.mastersuccessport) 
        while True:
            #message={}
            #while message=={}:
            message=master_socket.recv_pyobj()
            
            print("keeper  received replica req  from master ")
            ip=message['ip']
            message['successport']=""
            message['Type']=1
            replica_socket=self.context.socket(zmq.PAIR)
            replica_socket.connect(ip)
            success=False
            success=self.DownloadFile(message,replica_socket)
            if(success):
                # mastersocket = self.context.socket(zmq.PUSH)
                # mastersocket.bind(connectionPort+self.mastersuccessport)
                msg={'success':True,'successPort':""}#anhy port to be sent?????????
                master_socket.send_pyobj(msg)
######################################################
#    def RecvReplica(self):
#        master_socket = self.context.socket(zmq.PAIR)
#        master_socket.bind(connectionPort+self.replicationPort)
#        replica_socket=self.context.socket(zmq.PAIR)
#        replica_socket.bind(self.recvReplica)
#        while True:
#            message={}
#            while message=={}:
#                message=replica_socket.recv_pyobj()#?????????????
#            print("keeper received replica order from another keeper")
#            success=False
#            success=self.UploadFile(message,replica_socket)
#            if(success):
#                # mastersocket = self.context.socket(zmq.PUSH)
#                # mastersocket.bind(connectionPort+self.mastersuccessport)
#                msg={'success':True,'successPort':clientSuccessPort}#anhy port to be sent?????????
#                mastersocket.send_pyobj(msg)
            
d1=DataKeeper(5,"5510")
d2=DataKeeper(5,"5511")
d3=DataKeeper(5,"5512")

p1 = Process(target = d1.ConnectToClient)
p2 = Process(target = d2.ConnectToClient)
p3 = Process(target = d3.ConnectToClient)

h1 = Process(target = d1.HeartBeat)
h1.start()
p1.start()
p2.start()
p3.start()

h1.join()
p1.join()
p2.join()
p3.join()

# d1.ConnectToClient()