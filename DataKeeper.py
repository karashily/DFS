# -*- coding: utf-8 -*-
"""
Created on Sat Mar  7 13:07:34 2020

@author: somar
"""
import zmq
import random
import time
import threading 
connectionPort="tcp://127.0.0.1:"
class DataKeeper:
    i_Am_Alive_port="5555"
    clientport = "5510"
    mastersuccessport="5520"
    context = zmq.Context()
    def __init__(self, ID):
        self.ID = ID
        
 #################################       
    def HeartBeat(self):
        socket = self.context.socket(zmq.PUB)
        socket.bind(connectionPort+self.i_Am_Alive_port)
        while True:
            topic = random.randrange(9999,10005)
            messagedata = "keeper_no. %d is_alive/n" %self.ID
            socket.send_string("%s %s" % (topic, messagedata))
            time.sleep(1)
            
 #####################################           
    def UploadFile(self,message):
        uploadedVideo=message
        name=uploadedVideo['FileName']
        print(name+"/n")
        file=uploadedVideo['File']
        f = open("uploaded.mp4", "wb")
        f.write(file)
        f.close()
        print("datakeeper:video %s added on machine no %d successfully ^_^ /n" %(name,self.ID))
        return True
 ######################################
    def DownloadFile(self,message,socket):
        toBeDownloaded=message
        fileName=toBeDownloaded['FileName']
        print(fileName+"/n")
        f=open(fileName,"rb")
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
         message=socket.recv_pyobj()
         print("keeper  received  from client /n")
         Type=message['Type']
         success=False
         if(Type==1):
                success= self.UploadFile(message)
                if(success):
                    mastersocket = self.context.socket(zmq.PUSH)
                    mastersocket.bind(connectionPort+self.mastersuccessport)
                    mastersocket.send_pyobj(True)
         else:
             success=self.DownloadFile(message,socket)
             if(success):
                 mastersocket = self.context.socket(zmq.PUSH)
                 mastersocket.bind(connectionPort+self.mastersuccessport)
                 mastersocket.send_pyobj(True)
        
        
d1=DataKeeper(5)

d1.ConnectToClient()