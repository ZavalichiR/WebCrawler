import base64
import json
import pathlib
import os, shutil
import glob
from collections import defaultdict 

from mpi4py import MPI
import multiprocessing as mp

def encryptUrl(url):
    encryptedParentUrl = base64.b16encode(str.encode(url))
    encrypted = format(encryptedParentUrl.decode('utf-8'))
    return encrypted

def decrypt(encrypted):
    decrypted = base64.b16decode(encrypted)
    return decrypted

def readFromJSONFile(path, fileName):
    filePathNameWExt = path + fileName + '.json'
    with open(filePathNameWExt) as json_file:
        data = json.load(json_file)
    return data

MAP_PHASE = 1
REDUCE_PHASE = 2
FREE = 3
STOP = 4
UNLOCK = 5

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
nprocs = comm.Get_size()
status = MPI.Status()

statusProcese = [0] * nprocs

jsonPath = "Data/"
urlsPath = "Data/Urls/"

if rank == 0:   # MASTER
       
    #-----------> MapPhase
    print("MapPhase")
    pathlib.Path(urlsPath).mkdir(parents=True, exist_ok=True)
    data = readFromJSONFile(jsonPath, "source-destinations") 
    for parentUrl in data:
        for url in [parentUrl]:
            toSend = {}
            toSend[parentUrl] = url  
            
            comm.recv(source=MPI.ANY_SOURCE, tag=FREE, status=status)
            comm.send(toSend, dest=status.Get_source(), tag = MAP_PHASE)
            statusProcese[status.Get_source()] = 1
    
# -------------------------------------BARRIER------------------------------------------
    print("Barrier start")
    while sum(statusProcese) !=0 :   
        comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        statusProcese[status.Get_source()] -= 1
    print("Barrier end")
# -------------------------------------BARRIER------------------------------------------
    
    for j in range(1, nprocs):
        comm.send(toSend, dest=j, tag = UNLOCK )

    #-----------> ReducePahse
    print("Reduce Phase")
    encryptedFiles = os.listdir(urlsPath)
    
    for encryptedFile in encryptedFiles:
        comm.recv(source=MPI.ANY_SOURCE, tag=FREE, status=status)
        comm.send(encryptedFile, dest=status.Get_source(), tag = REDUCE_PHASE)

    #----------->  Send STOP
    for j in range(1, nprocs):
        comm.send(toSend, dest=j, tag = STOP )

else:   # WORKER
    work = True
    while work:
        comm.send(0,dest = 0, tag = FREE)   
        idata = comm.recv(source=0, status = status)

        #-----------> Map Phase
        if status.Get_tag() == MAP_PHASE:
            parent = ""
            url = ""
            for k in idata:
                parent = encryptUrl(k)
                url = encryptUrl(idata[k])
            #print(parent, url)
            filePath = urlsPath + format(url) + "_" + format(parent)
            if len(filePath) < 257:
                f = open(filePath, "w+")
                f.close()      
        
        #-----------> Reduce Phase
        if status.Get_tag() == REDUCE_PHASE:
            print(idata)
            fileName = idata.split("_")

            dest = decrypt(fileName[0].encode('utf-8'))
            source = decrypt(fileName[1].encode('utf-8'))

            destDecoded = dest.decode('utf-8')
            sourceDecoded = source.decode('utf-8')  

            print(destDecoded, sourceDecoded)

        #-----------> UNLOCK
        if status.Get_tag() == UNLOCK:
            print("Proces ", rank, " deblocat")
        #-----------> STOP
        if status.Get_tag() == STOP:
            work = False