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

    # if index.txt does not exist, create
    if not os.path.exists(args.basedir + "index.txt"):
        with open(args.basedir + "index.txt", 'w'):
            pass
    try:
        client = xmlrpc.client.ServerProxy('http://' + args.hostport)
        # Test ping
        client.surfstore.ping()
        print("Ping() successful")

        ##### sync file #####
        # { filename : [h1, h2, h3...]}
        localNewFile = {}
        localUpdatedFile = {}

        # open local index file
        # key : value = 'xxx.jpg' : [version 'e52a', '928f', '11c3']
        localFileInfo = {}
        with open(args.basedir + "index.txt") as f:
            for line in f:
                line = line.split('\n')[0]
                linelist = line.split(" ")
                localFileInfo[linelist[0]] = [int(linelist[1])] + linelist[2:]

        # client scan base directory
        for filename in os.listdir(args.basedir):
            if filename == "index.txt" or filename == ".DS_Store":
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
            elif localFileInfo[filename][1:] != hashlist:
                print(localFileInfo[filename][1:], hashlist)
                localUpdatedFile[filename] = hashlist

        # download remote index file
        remoteFileInfo = client.surfstore.getfileinfomap()

        for filename in remoteFileInfo:
            if (filename not in localFileInfo) or (
                    remoteFileInfo[filename][0] > localFileInfo[filename][0]):
                # remote file not in local or remote version larger than local,
                # download and update local index
                print("Download {} from the server.".format(filename))
                with open(args.basedir + filename, 'wb') as f:
                    for h in remoteFileInfo[filename][1]:
                        block = client.surfstore.getblock(h)
                        f.write(block.data)
                content = remoteFileInfo[filename]
                localFileInfo[filename] = [content[0]] + content[1]

        # upload local new file to the server
        for filename in localNewFile:
            if filename in remoteFileInfo:
                continue
            # if update is successful, update local index.
            print("Upload {} to the server.".format(filename))
            with open(args.basedir + filename, "rb") as bytefile:
                while True:
                    piece = bytefile.read(args.blocksize)
                    if piece == b'':
                        break
                    client.surfstore.putblock(piece)
            if client.surfstore.updatefile(filename, 1,
                                           localNewFile[filename]):
                localFileInfo[filename] = [1] + localNewFile[filename]

        # upload local modified file to the server
        for filename in localUpdatedFile:
            # if local and remote has same version, sync local changes to server
            version = localFileInfo[filename][0]
            if version != remoteFileInfo[filename][0]:
                continue
            version += 1
            print("Update {} on the server.".format(filename))
            with open(args.basedir + filename, "rb") as bytefile:
                while True:
                    piece = bytefile.read(args.blocksize)
                    if piece == b'':
                        break
                    client.surfstore.putblock(piece)
            if client.surfstore.updatefile(filename, version,
                                           localUpdatedFile[filename]):
                localFileInfo[filename] = [version
                                           ] + localUpdatedFile[filename]

        # update local index.txt
        print("Update local index.txt:")
        with open(args.basedir + "index.txt", 'w') as f:
            for filename in localFileInfo:
                version = localFileInfo[filename][0]
                hashlist = localFileInfo[filename][1:]
                f.write(
                    str(filename) + " " + str(version) + " " +
                    ' '.join(hashlist) + '\n')
                print(filename + ' ' + str(version) + ' ' + ' '.join(hashlist))

    except Exception as e:
        print("Client: " + str(e))
