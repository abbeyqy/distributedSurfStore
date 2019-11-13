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
    indexpath = os.path.join(args.basedir, "index.txt")
    if not os.path.exists(indexpath):
        with open(indexpath, 'w'):
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
        localDeletedFile = {}

        # currentFile [file1, file2, file3...]
        currentFile = set()

        # open local index file
        # key : value = 'xxx.jpg' : [version 'e52a', '928f', '11c3']
        localFileInfo = {}
        with open(indexpath) as f:
            for line in f:
                line = line.split('\n')[0]
                linelist = line.split(" ")
                localFileInfo[linelist[0]] = [int(linelist[1])] + linelist[2:]

        # client scan base directory
        for filename in os.listdir(args.basedir):
            if filename == "index.txt" or filename == ".DS_Store":
                continue
            hashlist = []
            with open(os.path.join(args.basedir, filename), "rb") as bytefile:
                while True:
                    piece = bytefile.read(args.blocksize)
                    if piece == b'':
                        break
                    h = hashlib.sha256(piece).hexdigest()
                    hashlist.append(h)

            # found local new file
            if filename not in localFileInfo:
                localNewFile[filename] = hashlist
            elif localFileInfo[filename][
                    1:] != hashlist:  # found local updated file
                print(localFileInfo[filename][1:], hashlist)
                localUpdatedFile[filename] = hashlist

            currentFile.add(filename)

        # scan local deleted file
        print("current local File: ", currentFile)
        for filename in localFileInfo:
            if filename not in currentFile:
                version = localFileInfo[filename][0]
                print("version after deleted: ", version)

                localDeletedFile[filename] = [version]
                localDeletedFile[filename].append("0")
        print("deleted: ", localDeletedFile)

        # download remote index file
        remoteFileInfo = client.surfstore.getfileinfomap()
        print(remoteFileInfo)

        for filename in remoteFileInfo:
            if (filename not in localFileInfo
                ) or (remoteFileInfo[filename][0] > localFileInfo[filename][0]
                      ) and (remoteFileInfo[filename][1] != "0"):
                # remote file not in local or remote version larger than local,
                # download and update local index
                print("Download {} from the server.".format(filename))
                with open(os.path.join(args.basedir, filename), 'wb') as f:
                    for h in remoteFileInfo[filename][1]:
                        block = client.surfstore.getblock(h)
                        f.write(block.data)
                content = remoteFileInfo[filename]
                localFileInfo[filename] = [content[0]] + content[1]

                # if updated from remote, then remove it from local updated
                if filename in localUpdatedFile:
                    del localUpdatedFile[filename]

        print("upload local new file")
        # upload local new file to the server
        for filename in localNewFile:
            if filename in remoteFileInfo:
                continue
            # if update is successful, update local index.
            print("Upload {} to the server.".format(filename))
            with open(os.path.join(args.basedir, filename), "rb") as bytefile:
                while True:
                    piece = bytefile.read(args.blocksize)
                    if piece == b'':
                        break
                    client.surfstore.putblock(piece)
            if client.surfstore.updatefile(filename, 1,
                                           localNewFile[filename]):
                localFileInfo[filename] = [1] + localNewFile[filename]

        print("upload local modified file")
        print(localFileInfo)
        # upload local modified file to the server
        for filename in localUpdatedFile:
            # if local and remote has same version, sync local changes to server
            version = localFileInfo[filename][0]
            if version != remoteFileInfo[filename][0]:
                continue
            version += 1
            print("Update {} on the server.".format(filename))
            with open(os.path.join(args.basedir, filename), "rb") as bytefile:
                while True:
                    piece = bytefile.read(args.blocksize)
                    if piece == b'':
                        break
                    client.surfstore.putblock(piece)
            if client.surfstore.updatefile(filename, version,
                                           localUpdatedFile[filename]):
                localFileInfo[filename] = [version
                                           ] + localUpdatedFile[filename]

        print("upload local deleted file")
        # upload local deleted file to the server
        for filename in localDeletedFile:
            # if local and remote has same version, sync local changes to server
            version = localFileInfo[filename][0]
            if version != remoteFileInfo[filename][0]:
                continue
            version += 1
            print("Update deleted {} on the server.".format(filename))
            if client.surfstore.updatefile(filename, version, "0"):
                localFileInfo[filename] = [version] + ["0"]

            print("remote {} deleted".format(filename))

        print("update remote deleted file")
        # update remote deleted file into localFileInfo
        for filename in remoteFileInfo:
            tombstone = remoteFileInfo[filename][1:]
            if tombstone == ["0"]:
                localFileInfo[filename][0] = remoteFileInfo[filename][0]
                localFileInfo[filename][1:] = remoteFileInfo[filename][1:]
                try:
                    os.remove(os.path.join(args.basedir, filename))
                except Exception as e:
                    print("No such file")

        # update local index.txt
        print("Update local index.txt:")
        with open(indexpath, 'w') as f:
            for filename in localFileInfo:
                version = localFileInfo[filename][0]
                hashlist = localFileInfo[filename][1:]
                f.write(
                    str(filename) + " " + str(version) + " " +
                    ' '.join(hashlist) + '\n')
                print(filename + ' ' + str(version) + ' ' + ' '.join(hashlist))

    except Exception as e:
        print("Client: " + str(e))
