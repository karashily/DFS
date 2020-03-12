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
    #clientport = "5510"
    # mastersuccessport = port[:-2] + str(int(port[-2]) + 1) + port[-1]
    context = zmq.Context()
    def __init__(self, ID, port):
        self.ID = ID
        self.clientport=port
        self.mastersuccessport = port[:-2] + str(int(port[-2]) + 1) + port[-1]
 #################################       
    def HeartBeat(self):
        socket = self.context.socket(zmq.push)
        socket.connect(connectionPort+self.i_Am_Alive_port)
        while True:
            #topic = random.randrange(9999,10005)
            messagedata = connectionPort+self.i_Am_Alive_port
            socket.send_string(messagedata)
            time.sleep(1)
            
 #####################################           
    def UploadFile(self,message):
        uploadedVideo=message
        name=uploadedVideo['FileName']
        print(name+"/n")
        file=uploadedVideo['File']
        myfolder="keeper no. %d folder" %self.ID
        if not os.path.exists(myfolder):
            os.makedirs(myfolder)
        f=open(myfolder+'/'+fileName,"rb")
        f.write(file)
        f.close()
        print("datakeeper:video %s added on machine no %d successfully ^_^ /n" %(name,self.ID))
        return True
 ######################################
    def DownloadFile(self,message,socket):
        print("d5lt el download")
        toBeDownloaded=message
        fileName=toBeDownloaded['FileName']
        myfolder="keeper no. %d folder" %self.ID
        if not os.path.exists(myfolder):
            os.makedirs(myfolder)
        f=open(myfolder+'/'+fileName,"rb")
        v=f.read()
        downloadedVideo={'File':v,'FileName':fileName}
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
        
        
d1=DataKeeper(5,"5510")
d2=DataKeeper(5,"5511")
d3=DataKeeper(5,"5512")

p1 = Process(target = d1.ConnectToClient)
p2 = Process(target = d2.ConnectToClient)
p3 = Process(target = d3.ConnectToClient)

p1.start()
p2.start()
p3.start()

p1.join()
p2.join()
p3.join()

# d1.ConnectToClient()