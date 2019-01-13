import rpyc
import sys
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

    def missing_blocks(self, hashlist):
        self.error_type = 1
        self.missing_blocks = hashlist

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
        f = open(config)
        cfg = f.read().split('\n')
        end = 0
        for i in range(len(cfg)):
            if cfg[i] == '':
                end = i
                break
        bsinfo = [blockstore_info.split(': ')[1] for blockstore_info in cfg[2:end]]
        self.num_of_bs = int(cfg[0].split(': ')[1])
        self.blockconn = [rpyc.connect(binfo.split(':')[0], int(binfo.split(':')[1])).root for binfo in bsinfo]
        self.hashmap_version = {}
        self.hashmap_hashlist = {}
        self.deleted_files = set()

    '''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''

    def exposed_modify_file(self, filename, version, hash_server):
        # print(hash_server)
        hashlist = list()
        for pair in hash_server:
            hashlist.append(pair[0])
        if filename in self.hashmap_version and version == 1:
            response = ErrorResponse("file not found")
            response.file_not_found()
            raise response

        if version != 1 and (filename not in self.hashmap_version or version != self.hashmap_version[filename] + 1):
            response = ErrorResponse("file not found")
            response.file_not_found()
            raise response
        missingblocks = self.findmissingblocks(list(hash_server))
        # print(hash_server)
        # print(missingblocks)
        if len(missingblocks) != 0:
            response = ErrorResponse("missing blocks")
            response.missing_blocks(missingblocks)
            raise response
        else:
            self.hashmap_version[filename] = version
            self.hashmap_hashlist[filename] = hash_server
            if filename in self.deleted_files:
                self.deleted_files.remove(filename)
        # print(self.hashmap_hashlist)

    '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''

    def exposed_delete_file(self, filename, version):
        if filename in self.hashmap_hashlist:
            if filename not in self.deleted_files:
                if version == self.hashmap_version[filename] + 1:
                    self.deleted_files.add(filename)
                    self.hashmap_version[filename] += 1
                else:
                    response = ErrorResponse("wrong version error")
                    response.wrong_version_error(self.hashmap_version[filename])
                    raise response
            else:
                response = ErrorResponse("file not found")
                response.file_not_found()
                raise response
        else:
            response = ErrorResponse("file not found")
            response.file_not_found()
            raise response

    '''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''

    def exposed_read_file(self, filename):
        v = 0
        hl = []
        if filename in self.hashmap_hashlist:
            v = self.hashmap_version[filename]
            if filename not in self.deleted_files:
                hl = self.hashmap_hashlist[filename]
        return v, tuple(hl)

    def findmissingblocks(self, hash_server):
        missingblocks = []
        for pair in hash_server:
            h = pair[0]
            s = pair[1]
            if not self.blockconn[s].has_block(h):
                missingblocks.append(h)
        print(missingblocks)
        return tuple(missingblocks)

    def find_server_hash(self, h):
        return int(h, 16) % self.num_of_bs

if __name__ == '__main__':
    # from rpyc.utils.server import ThreadPoolServer
    from rpyc.utils.server import ThreadedServer
    server = ThreadedServer(MetadataStore(sys.argv[1]), port=6000)
    server.start()