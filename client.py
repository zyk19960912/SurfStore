import rpyc
import hashlib
import os
import sys
import pingparsing
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
        f = open(config)
        cfg = f.read().split('\n')
        end = 0
        for i in range(len(cfg)):
            if cfg[i] == '':
                end = i
                break
        minfo = cfg[1].split(': ')[1].split(':')
        bsinfo = [blockstore_info.split(': ')[1] for blockstore_info in cfg[2:end]]
        self.num_of_bs = int(cfg[0].split(': ')[1])
        self.metaconn = rpyc.connect(minfo[0], int(minfo[1])).root
        self.blockconn = [rpyc.connect(binfo.split(':')[0], int(binfo.split(':')[1])).root for binfo in bsinfo]
        self.m = {}

    def get_blocks(self,filepath):
        blocks = []
        f = open(filepath,'rb')
        while True:
            data = f.read(4096)
            if not data:
                break
            blocks.append(data)
        return blocks

    def find_server_hash(self,h):
        hosts = ["18.231.0.104",##San Paulo
                 "18.203.250.214",##Ireland
                 "13.125.116.112",##Seoul
                 "52.66.248.28"]##Mumbai
        res = int(h, 16) % self.num_of_bs
        ping_parser = pingparsing.PingParsing()
        transmitter = pingparsing.PingTransmitter()
        sum = 0
        for i in range(len(hosts)):
            transmitter.destination_host = hosts[i]
            transmitter.count = 10
            result = transmitter.ping()
            t = ping_parser.parse(result).as_dict()["rtt_avg"]
            if i!=res:
                sum+=t
        print(sum/3)
        return int(h, 16) % self.num_of_bs

    def find_server_nearest(self):
        hosts = ["18.231.0.104",##San Paulo
                 "18.203.250.214",##Ireland
                 "13.125.116.112",##Seoul
                 "52.66.248.28"]##Mumbai
        # hosts = ["www.google.com",  ##San Paulo
        #          "www.facebook.com",  ##Ireland
        #          "www.bing.com",  ##Seoul
        #          "www.baidu.com"]  ##Mumbai
        rtt = 10000
        index = 0
        ping_parser = pingparsing.PingParsing()
        transmitter = pingparsing.PingTransmitter()
        for i in range(len(hosts)):
            print(i)
            transmitter.destination_host = hosts[i]
            transmitter.count = 10
            result = transmitter.ping()
            t = ping_parser.parse(result).as_dict()["rtt_avg"]
            print(t)
            if t < rtt:
                rtt = t
                index = i
        print(hosts[index])
        print(rtt)
        return index


    """
    upload(filepath) : Reads the local file, creates a set of 
    hashed blocks and uploads them onto the MetadataStore 
    (and potentially the BlockStore if they were not already present there).
    """
    def upload(self, filepath, placement):
        filename = filepath.split('/')[-1]
        blocks = self.get_blocks(filepath)
        hashlist = [hashlib.sha256(block).hexdigest() for block in blocks]
        block_map = {hashlib.sha256(block).hexdigest(): block for block in blocks}
        hashlist = tuple(hashlist)
        version = 1
        hash_server = list()
        if placement == 'hash':
            for h in hashlist:
                hash_server.append((h,self.find_server_hash(h)))
        elif placement == 'nearest':
            s = self.find_server_nearest()
            for h in hashlist:
                hash_server.append((h,s))
        for pair in hash_server:
            self.m[pair[0]] = pair[1]
        hash_server = tuple(hash_server)
        latest_version, latest_hashlist = self.metaconn.read_file(filename)
        if latest_version != 0:
            version = latest_version + 1
        while True:
            try:
                self.metaconn.modify_file(filename, version, hash_server)
                print("OK")
                break
            except Exception as error_response:
                if error_response.error_type == 1:
                    for h in error_response.missing_blocks:
                        print(1)
                        self.blockconn[self.m[h]].store_block(h, block_map[h])


                        # if placement == 'hash':
                        #     self.blockconn[self.find_server_hash(h)].store_block(h, block_map[h])
                        # elif placement == 'nearset':
                        #     self.blockconn[self.find_server_nearest()].store_block(h, block_map[h])
                elif error_response.error_type == 2:
                    version = error_response.current_version + 1

    """
    delete(filename) : Signals the MetadataStore to delete a file.
    """
    def delete(self, filename):
        v, h1 = self.metaconn.read_file(filename)
        if v != 0:
            while True:
                try:
                    self.metaconn.delete_file(filename, v+1)
                    print("OK")
                    break
                except Exception as error_response:
                    if error_response.error_type == 2:
                        v = error_response.current_version
                    elif error_response.error_type == 3:
                        break
        else:
            print("NOT FOUND")

    """
    download(filename, dst): Downloads a file (f) from SurfStore and saves
    it to (dst) folder. Ensures not to download unnecessary blocks.
    """
    def download(self, filename, location, placement):
        full_path = os.path.join(location, filename)
        data = b''
        try:
            v, hl = self.metaconn.read_file(filename)
            if len(hl) == 0:
                raise FileNotFoundError
            for (h,s) in hl:
                data += self.blockconn[s].get_block(h)
            with open(full_path, 'wb') as f:
                f.write(data)
            print("OK")
        except FileNotFoundError:
            print("NOT FOUND")

    """
     Use eprint to print debug messages to stderr
     E.g - 
     self.eprint("This is a debug message")
    """
    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)



if __name__ == '__main__':
    client = SurfStoreClient(sys.argv[1])
    operation = sys.argv[2]
    if operation == 'delete':
        client.delete(sys.argv[3])
    else:
        placement = sys.argv[-1]
        if placement != 'hash' and placement != 'nearest':
            print("Invalid placement")

        if operation == 'upload':
            client.upload(sys.argv[3], placement)
        elif operation == 'download':
            client.download(sys.argv[3], sys.argv[4],placement)
        else:
            print("Invalid operation")
