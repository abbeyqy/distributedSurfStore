from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
import hashlib


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2', )


class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


hashBlockMap = {}
fileInfoMap = {}


# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True


# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")
    blockData = hashBlockMap[h]
    # blockData = bytes(4)
    return blockData


# Puts a block
def putblock(b):
    """Puts a block"""
    # b = xml.client.Binary.decode(b)
    b = b.data
    hash_value = hashlib.sha256(b).hexdigest()
    hashBlockMap[hash_value] = b
    print("PutBlock()", hash_value)
    return True


# Given a list of hashes, return the subset that are on this server
def hasblocks(hashlist):
    """Determines which blocks are on this server"""
    hashlistout = [h for h in hashlist if h in hashBlockMap]
    print("HasBlocks()")
    return hashlistout


# Retrieves the server's FileInfoMap
def getfileinfomap():
    """
    Gets the fileinfo map
    key : value = 'xxx.jpg' : [2 ['e52a', '928f', '11c3']]
    """
    print("GetFileInfoMap()")
    print(fileInfoMap)
    return fileInfoMap


# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    print("UpdateFile()")
    # if the file never existed, create it.
    if filename not in fileInfoMap:
        print(filename)
        fileInfoMap[filename] = [version, hashlist]
        print("File uploaded")
        return True
    # if the file was previously deleted, update the version number
    # that is one larger than the "tombstone" record.
    if fileInfoMap[filename][1] == '0':
        fileInfoMap[filename][0] += 1
        fileInfoMap[filename][1] = hashlist
        return True
    # if the file is in the file info map, update it.
    currVersion = fileInfoMap[filename][0]
    if version != currVersion + 1:
        print("Version not right!")
        return False
    print(hashlist)
    fileInfoMap[filename] = [version] + [hashlist]
    print("File updated.")
    return True


# PROJECT 3 APIs below


# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("IsLeader()")
    return True


# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    print("Crash()")
    return True


# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    print("Restore()")
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return True


if __name__ == "__main__":
    try:
        print("Attempting to start XML-RPC Server...")
        server = threadedXMLRPCServer(('localhost', 8080),
                                      requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping, "surfstore.ping")
        server.register_function(getblock, "surfstore.getblock")
        server.register_function(putblock, "surfstore.putblock")
        server.register_function(hasblocks, "surfstore.hasblocks")
        server.register_function(getfileinfomap, "surfstore.getfileinfomap")
        server.register_function(updatefile, "surfstore.updatefile")

        server.register_function(isLeader, "surfstore.isleader")
        server.register_function(crash, "surfstore.crash")
        server.register_function(restore, "surfstore.restore")
        server.register_function(isCrashed, "surfstore.iscrashed")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
