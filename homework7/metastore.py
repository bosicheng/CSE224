import rpyc
import sys
from collections import *

'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''


class ErrorResponse(Exception):
    def __init__(self, message):
        super(ErrorResponse, self).__init__(message)
        self.error = message

    def missing_blocks(self,hashlist):
        self.error_type = 1
        self.missing_blocks_list = hashlist


    def wrong_version_error(self, version):
        self.error_type = 2
        self.current_version = version

    def file_not_found(self):
        self.error_type = 3
        


'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''


class MetadataStore(rpyc.Service):
    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """

    def __init__(self, config):
        config_file = open(config, 'r')
        arr = []
        for line in config_file.readlines():
            arr.append(line)
        config_file.close()
        self.numBlockStores=arr[0].split(": ")[1]
        self.blockstore=[]
        for line in arr[2:]:
            if line.strip() == "" or line == "\r\n":
                continue
            block_connect=rpyc.connect(line.split(": ")[1].split(":")[0], line.split(": ")[1].split(":")[1].strip())
            self.blockstore.append(block_connect)
        self.file_map = defaultdict(list)



    '''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''

    def exposed_modify_file(self, filename, version, hashlist,blockstore_index):
        # (v,hl)=self.exposed_read_file(filename)

        mbl = []
        for hash_value in hashlist:
            # blockstore_index=self.findServer(hash_value)
            if not self.blockstore[blockstore_index].root.has_block(hash_value):
                mbl.append(hash_value)
        if mbl==[] and self.file_map[filename][1]!=[]:
            return "OK"
        else:
            if version <= self.file_map[filename][0]:
                error = ErrorResponse("wrong_version_error")
                error.wrong_version_error(self.file_map[filename][0])
                raise error
            self.file_map[filename] = [version, list(hashlist),blockstore_index]
            error = ErrorResponse("missing_blocks")
            error.missing_blocks(mbl)
            raise error


        

    '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''

    # def exposed_delete_file(self, filename, version):
    #     if version<=self.file_map[filename][0]:
    #         error = ErrorResponse("wrong_version_error")
    #         error.wrong_version_error(self.file_map[filename][0])
    #         raise error
    #     [(curr_v,hl)]=self.file_map[filename]
    #     self.file_map[filename]=(curr_v+1,[])
        

    '''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''

    def exposed_read_file(self, filename):
        if filename not in self.file_map.keys():
            self.file_map[filename]=[0,[],-1]
        [v,hl,bl]=self.file_map[filename]
        return (v,hl,bl)
        
    
    # def findServer(self,h):
    #     return int(h,16) % int(self.numBlockStores)

if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer

    server = ThreadedServer(MetadataStore(sys.argv[1]), port=6000)
    server.start()

