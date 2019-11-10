import argparse
import xmlrpc.client
import os
import hashlib

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument('hostport', help='host:port of the server')
    parser.add_argument('basedir', help='The base directory')
    parser.add_argument('blocksize', type=int, help='Block size')
    args = parser.parse_args()

    try:
        client = xmlrpc.client.ServerProxy('http://localhost:8080')
        # Test ping
        client.surfstore.ping()
        print("Ping() successful")

        ##### sync file #####
        localFile = {}
        localNewFile = {}
        localUpdatedFile = {}

        # open local index file
        localFileInfo = {}

        # if index.txt does not exist, create
        if not os.path.exists(args.basedir + "index.txt"):
            with open(args.basedir + "index.txt", 'w'): pass

        # key : value = 'xxx.jpg' : [2 ['e52a', '928f', '11c3']]
        with open(args.basedir + "index.txt") as f:
            for line in f:
                linelist = line.split()
                localFileInfo[linelist[0]] = [linelist[1]].append(linelist[2:])

        # client scan base directory
        for filename in os.listdir(args.basedir):
            print(filename)
            if filename == "index.txt":
                continue
            hashlist = []
            with open(args.basedir + filename, "rb") as bytefile:
                while True:
                    piece = bytefile.read(args.blocksize)
                    if piece == b'':
                        break
                    h = hashlib.sha256(piece).hexdigest()
                    hashlist.append(h)
            if filename not in localFileInfo:
                localNewFile[filename] = hashlist
            elif localFileInfo[filename][1] != hashlist:
                localUpdatedFile[filename] = hashlist
        # download remote index file
        remoteFileInfo = client.surfstore.getfileinfomap()

    except Exception as e:
        print("Client: " + str(e))
