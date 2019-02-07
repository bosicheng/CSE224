import rpyc
import hashlib
import os
import sys
from metastore import ErrorResponse
import time

"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""


class SurfStoreClient():
    """
    Initialize the client and set up connections to the block stores and
    metadata store using the config file
    """

    def __init__(self, config):
        config_file = open(config, 'r')
        arr = []
        for line in config_file.readlines():
            arr.append(line)
        config_file.close()
        self.numBlockStores=arr[0].split(": ")[1]
        self.metastore=rpyc.connect(arr[1].split(": ")[1].split(":")[0],arr[1].split(": ")[1].split(":")[1].strip())
        self.blockstore=[]
        for line in arr[2:]:
            if line.strip() == "" or line == "\r\n":
                continue
            block_connect=rpyc.connect(line.split(": ")[1].split(":")[0], line.split(": ")[1].split(":")[1].strip())
            self.blockstore.append(block_connect)


    """
    upload(filepath) : Reads the local file, creates a set of 
    hashed blocks and uploads them onto the MetadataStore 
    (and potentially the BlockStore if they were not already present there).
    """

    def upload(self, filepath):
        blockstore_index=self.findServer()
        conn = self.metastore
        file_name = filepath.split("/")[-1]
        (v,hl,bl) = conn.root.read_file(file_name)
        upload_file = open(filepath, 'rb')
        buffer = b''
        count = 0
        upload_hl = []
        upload_dict = {}
        for byte in upload_file:
            if count != 0 and count % 4096 == 0:
                hash_value=hashlib.sha256(buffer).hexdigest()
                upload_hl.append(hash_value)
                upload_dict[hash_value]=buffer
                buffer = b''
            buffer += byte
            count += 1
        hash_value=hashlib.sha256(buffer).hexdigest()
        upload_hl.append(hash_value)
        upload_dict[hash_value]=buffer
        try:
            conn.root.modify_file(file_name, v + 1, upload_hl,blockstore_index)
        except Exception as e:
            if e.error_type==2:
                self.upload(filepath)
            if e.error_type == 1:
                for value in eval(e.missing_blocks_list):
                    self.blockstore[blockstore_index].root.store_block(value,upload_dict[value])
        print("OK")
       
        
        
    """
    delete(filename) : Signals the MetadataStore to delete a file.
    """

    def delete(self, filename):
        conn = self.metastore
        (v,hl,blockstore_index) = conn.root.read_file(filename)
        if v==0:
            print("Not Found")
            return -1
        try:
            conn.root.delete_file(filename, v + 1)
        except Exception as e:
            if e.error_type==2:
                self.delete(filename)
        print('OK')
        
        

    # check if succeed

    """
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
    """

    def download(self, filename, location):
        (v,hl,blockstore_index)=self.metastore.root.read_file(filename)
        # print(v,hl)
        if len(hl)==0:
            print("Not Found")
            return -1        
        else:
            content=b''
            for h in hl:
                # blockstore_index=self.findServer()
                buffer=self.blockstore[blockstore_index].root.get_block(h)
                content+=buffer
        
        filepath=location+"/"+filename
        file=open(filepath,'wb')
        file.write(content)
        file.close()
        print('OK')
    
    def findServer(self):
        blockstore_index=0
        min_RTT=sys.maxsize
        for i in range(len(self.blockstore)):
            send_out=time.time()
            self.blockstore[i].ping()
            recv=time.time()
            RTT=round(recv-send_out,4)
            if RTT<min_RTT:
                blockstore_index=i
                min_RTT=RTT
        return blockstore_index




if __name__ == '__main__':
    client = SurfStoreClient(sys.argv[1])
    operation = sys.argv[2]
    if operation == 'upload':
        client.upload(sys.argv[3])
    elif operation == 'download':
        client.download(sys.argv[3], sys.argv[4])
    elif operation == 'delete':
        client.delete(sys.argv[3])
    else:
        print("Invalid operation")

