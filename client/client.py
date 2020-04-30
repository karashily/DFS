# -*- coding: utf-8 -*-
"""
Created on Sat Mar  7 16:04:17 2020

@author: somar
"""

import zmq
import random
import sys
import time
import threading 
import os
from multiprocessing import Process,Value,Lock,Manager
 
IP = "tcp://127.0.0.1:"

masterPorts = [
    "tcp://127.0.0.1:5500", 
    "tcp://127.0.0.1:5501", 
    "tcp://127.0.0.1:5502"
    ]

class Client:
    context = zmq.Context() 
    masterPort="5500"      #to 5509
    def __init__(self, ID, port):
        self.ClientID = ID
        self.clientSuccessPort=IP+"530"+str(port)
#hand shaking with master,port number returned from master

    def UploadFile(self,fileName,portUpload):
        socket = self.context.socket(zmq.PAIR)
        socket.connect(portUpload)
        print("client {}: connection to keeper done".format(self.ClientID))
        f=open(fileName,"rb")
        v=f.read()
        uploadedVideo={'File':v,'fileName':fileName,'Type':1,'successport':self.clientSuccessPort}  #type: 1= upload   0= download
        socket.send_pyobj(uploadedVideo)
        print("client {}:video uploaded ^_^".format(self.ClientID))
        f.close()
        mastersocket=self.context.socket(zmq.PAIR)
        mastersocket.bind(self.clientSuccessPort)
        print("client {}: waiting success message from  master".format(self.ClientID))
        success=mastersocket.recv_pyobj()
        if(success==True):
            socket.close()
            mastersocket.close()
            print("client %s: left successfully" %self.ClientID)
 ########################################################       
    def DownloadFile(self,fileName,dataKeeperPort):
        socket = self.context.socket(zmq.PAIR)
        socket.connect(dataKeeperPort)
        toBeDownloaded={'fileName':fileName,'Type':0,'successport':self.clientSuccessPort}

        socket.RCVTIMEO = 3000 # in milliseconds
        recv = True
        while(recv == True):
            try:
                socket.send_pyobj(toBeDownloaded)
                print("client {}: download request sent on ".format(self.ClientID) + dataKeeperPort)
                downloadedVideo=socket.recv_pyobj()
                recv = False
            except:
                recv = True
        socket.close()
        name=downloadedVideo['fileName']
        file=downloadedVideo['File']
        # Create target Directory if don't exist
        myfolder="client no. %s folder" %self.ClientID
        if not os.path.exists(myfolder):
            os.makedirs(myfolder)
        f = open(myfolder+"/"+fileName, "wb")
        f.write(file)
        f.close()
        print("client %s: video %s added successfully ^_^" %(self.ClientID, name))
        mastersocket=self.context.socket(zmq.PAIR)
        mastersocket.bind(self.clientSuccessPort)
        print("client {}: waiting success message from master".format(self.ClientID))
        success=mastersocket.recv_pyobj()
        if(success==True):
            mastersocket.close()
            print("client %s: left successfully" %self.ClientID)
#####################################################
    def connectToMaster(self,operation,Filename):
        #connect to master
        socket = self.context.socket(zmq.REQ)
        
        ports = []
        while len(ports) < len(masterPorts):
            rand = random.randint(0, len(masterPorts) - 1)
            if rand not in ports:
                socket.connect(masterPorts[rand])
                print("client {}: connected to port {}".format(self.ClientID, masterPorts[rand]))
                ports.append(rand)
        # socket.connect(IP+self.masterPort)
        
        message={'clientID':self.ClientID,'Type':operation,'fileName':Filename}
        socket.send_pyobj(message) #send message to master
        print("client {}: operation {} sent to master".format(self.ClientID, operation))
        dataport=socket.recv_string()#wait for port
        while(dataport == 'no_free_ports'):
            print("client {}: All datakeepers ports busy.... Trying again...".format(self.ClientID))
            socket.send_pyobj(message) #send message to master
            print("client {}: operation {} sent to master".format(self.ClientID, operation))
            dataport=socket.recv_string()#wait for port
        
        if(dataport == 'file_not_found'):
            print('client %s: Fatal Error: Requested File Not Found....' %self.ClientID)
            socket.close()
            return
        
        if(dataport == 'filename_exists_already'):
            print('client %s: Fatal Error: Duplicate filename....' %self.ClientID)
            socket.close()
            return

        print("client {}: master responded with port {}".format(self.ClientID, dataport))
        if(operation==1): #upload
            socket.close()
            self.UploadFile(Filename,dataport)
        else: #download
            socket.close()
            self.DownloadFile(Filename,dataport)
        # socket.close()
            
 ######################           
#c1=Client(random.randint(0,9))

clientsNum = int(input("Number of clients: "))
clients=[]
while(clientsNum>10):
    print("maximum allowable no. of clients is 10..try again")
    clientsNum = int(input("Number of clients: "))
for i in range(clientsNum):
    id  = i
    rightOperation = True
    while(rightOperation):
        operation = input("client {}: please enter the operation (download/upload): ".format(id))
        if(operation == 'download'):
            operation = 0
            rightOperation = False
        elif(operation == 'upload'):
            operation = 1
            rightOperation = False
        else:
            print("client {}: Wrong Operation...!".format(id))

    fileName = input("client {}: please enter the filename: ".format(id))

    c = Client(id, i)
    p = Process(target=c.connectToMaster, args=(operation, fileName))
    clients.append(p)
        
    
for i in clients:
    i.start()
    # time.sleep(0.1)

for i in clients:
    i.join()
# c1.connectToMaster(1,"1.mp4")
# c1.connectToMaster(0,"1.mp4")
