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
		# { filenam : [h1, h2, h3...]}
		localNewFile = {}
		localUpdatedFile = {}

		# open local index file
		localFileInfo = {}

		# if index.txt does not exist, create
		if not os.path.exists(args.basedir + "index.txt"):
			with open(args.basedir + "index.txt", 'w'):
				pass

		# key : value = 'xxx.jpg' : [2 'e52a', '928f', '11c3']
		with open(args.basedir + "index.txt") as f:
			for line in f:
				linelist = line.split()
				localFileInfo[linelist[0]] = [int(linelist[1])] + linelist[2:]

		# client scan base directory
		for filename in os.listdir(args.basedir):
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
			elif localFileInfo[filename][1:] != hashlist:
				localUpdatedFile[filename] = hashlist

		# download remote index file
		remoteFileInfo = client.surfstore.getfileinfomap()

		for filename in remoteFileInfo:
			if (filename not in localFileInfo) and (
					filename not in localNewFile):
				# remote file not in local, download and update local index
				with open(args.basedir + filename, 'wb') as f:
					for h in remoteFileInfo[filename][1]:
						f.write(client.surfstore.getblock(h))
				content = remoteFileInfo[filename]
				localFileInfo[filename] = [content[0]] + content[1]
			elif filename in localFileInfo:
				if remoteFileInfo[filename][0] > localFileInfop[filename][0]:
				# remote version larger than local, update local 	


		# upload local new file to the server
		for filename in localNewFile:
			# if update is successful, update local index.
			if client.surfstore.updatefile(filename, 1,
										   localNewFile[filename]):
				with open(args.basedir + filename, "rb") as bytefile:
					while True:
						piece = bytefile.read(args.blocksize)
						if piece == b'':
							break
						client.surfstore.putblock(piece)
				localFileInfo[filename] = [1] + localNewFile[filename]
			# handle conflicts


# update existed file

	except Exception as e:
		print("Client: " + str(e))
