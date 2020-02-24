import argparse
import xmlrpc.client
import os
import hashlib

#new files uploaded to the cloud
def uploadd(fileinfomap_server, fileinfomap_client, fileinfomap_clientindex):
	version = 1
	for file in fileinfomap_client:
		if file not in fileinfomap_server:
			filepath = based + '/' + file
			fileread = open(filepath, "rb")		
			block = fileread.read(args.blocksize)
			indice = 0
			print("uploading " + file +" to server")
			while block:
				client.surfstore.putblock(fileinfomap_client[file][1][indice], block)
				indice += 1
				block = fileread.read(args.blocksize)			
			
			if file not in fileinfomap_clientindex:
				version = 1
			else:
				if fileinfomap_client[file][1] == fileinfomap_clientindex[file][1]:
					version = fileinfomap_clientindex[file][0]
				else:
					version = fileinfomap_clientindex[file][0] + 1
			flag = client.surfstore.updatefile(file, version, fileinfomap_client[file][1])
			# if flag:
			# 	ll = []
			# 	ll.append(version)
			# 	ll.append(fileinfomap_client[file][1])
			# 	fileinfomap_clientindex[file] = ll
		else:
			if file not in fileinfomap_clientindex:
				version = 1
			else:
				if fileinfomap_client[file][1] == fileinfomap_clientindex[file][1]:
					version = fileinfomap_clientindex[file][0]
				else:
					version = fileinfomap_clientindex[file][0] + 1
				# if fileinfomap_client[file][0] == 1 and fileinfomap_clientindex[file][0] == 1:
				# 	version = 2
			if version == fileinfomap_server[file][0] + 1:
				filepath = based + '/' + file
				fileread = open(filepath, "rb")		
				block = fileread.read(args.blocksize)
				indice = 0
				print("modifying " + file +" on server")
				while block:
					client.surfstore.putblock(fileinfomap_client[file][1][indice], block)
					indice += 1
					block = fileread.read(args.blocksize)
				flag = client.surfstore.updatefile(file, version, fileinfomap_client[file][1])
				# if flag:
				# 	ll = []
				# 	ll.append(version)
				# 	ll.append(fileinfomap_client[file][1])
				# 	fileinfomap_clientindex[file] = ll

#download new files from the cloud
def downloadd(fileinfomap_server, fileinfomap_client, fileinfomap_clientindex):
	for file in fileinfomap_server:
		if file not in fileinfomap_client and fileinfomap_server[file][1] != tombstone:
			if file not in fileinfomap_clientindex or fileinfomap_clientindex[file][1] == tombstone: #download
				filepath = based + '/' + file
				print("downloading file: " + file + " from the server")
				f = open(filepath, 'wb')
				for h in fileinfomap_server[file][1]:
					f.write(client.surfstore.getblock(h).data)
				f.close()
				fileinfomap_clientindex[file] = fileinfomap_server[file]
			else:   #delete file from server
				if fileinfomap_clientindex[file][0] == fileinfomap_server[file][0]:
					print("deleting file: " + file + " on the server")
					client.surfstore.updatefile(file, fileinfomap_server[file][0] + 1, tombstone)
					# ll = []
					# ll.append(fileinfomap_server[file][0] + 1)
					# ll.append(tombstone)					
					# fileinfomap_clientindex[file] = ll
		else:
			if file not in fileinfomap_clientindex or fileinfomap_server[file][0] > fileinfomap_clientindex[file][0]:
				if fileinfomap_server[file][1] == tombstone:
					#delete file on the client
					filepath = based + '/' + file
					if os.path.exists(filepath):
						print("deleting local file: " + file)
						os.remove(filepath)
				else:
					#download
					filepath = based + '/' + file
					print("updating local file: " + file)
					f = open(filepath, 'wb')
					for h in fileinfomap_server[file][1]:
						f.write(client.surfstore.getblock(h).data)
					f.close()					

def updateindex(fileinfomap_clientindex):
	fileinfomap_server = client.surfstore.getfileinfomap()
	f = open(indexfile,"w+")
	for file in fileinfomap_server:
		f.write(file + ' ' + str(fileinfomap_server[file][0]))
		for hash_values in fileinfomap_server[file][1]:
			f.write(' '+ hash_values)
		f.write('\n')
	f.close()


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	parser.add_argument('basedir', help='The base directory')
	parser.add_argument('blocksize', type=int, help='Block size')
	args = parser.parse_args()

	tombstone = ['0']

	#try:
	client = xmlrpc.client.ServerProxy('http://' + args.hostport)
	# Test ping
	client.surfstore.ping()
	print("Ping() successful")

	fileinfomap_server = client.surfstore.getfileinfomap()
	fileinfomap_client = {}
	fileinfomap_clientindex = {}
	based = os.path.abspath(args.basedir)
	if not os.path.isdir(based):
		os.system("mkdir " + based)
	filelist = os.listdir(based)
	indexfile = based + "/index.txt"
	if not os.path.exists(indexfile):
		f = open(indexfile, "w+")
		for filename in filelist:
			if filename == "index.txt":
				continue

			f.write(filename + " 1")
			filepath = based + '/' + filename
			fileread = open(filepath, "rb")
			hashlist = []
			block = fileread.read(args.blocksize)
			while block:
				hash_value = hashlib.sha256(block).hexdigest()
				hashlist.append(hash_value)
				f.write(' ' + hash_value)
				block = fileread.read(args.blocksize)
			f.write("\n")

			fileinfo = []
			fileinfo.append(1) # version
			fileinfo.append(hashlist)        
			fileinfomap_client[filename] = fileinfo
		#fileinfomap_clientindex = fileinfomap_client
		f.close()
	else:
		f = open(indexfile, "r")
		for line in f.readlines():
			indice = 0
			hashlist = []
			fileinfo = []
			line = line.strip('\n')
			infos = line.split(' ')			
			for info in infos:
				if indice == 0:
					filename = info
				if indice == 1:
					version = int(info) 
				if indice >= 2 :
					hashlist.append(info)
				indice += 1
			fileinfo.append(version)
			fileinfo.append(hashlist)
			fileinfomap_clientindex[filename] = fileinfo 
		f.close()
		for filename in filelist:
			if filename == "index.txt":
				continue
			filepath = based + '/' + filename
			fileread = open(filepath, "rb")
			hashlist = []
			block = fileread.read(args.blocksize)
			while block:
				hash_value = hashlib.sha256(block).hexdigest()
				hashlist.append(hash_value)
				block = fileread.read(args.blocksize)
			fileinfo = []
			fileinfo.append(1) # version
			fileinfo.append(hashlist)        
			fileinfomap_client[filename] = fileinfo	    

	print(fileinfomap_clientindex)
	print(fileinfomap_client)
	print(fileinfomap_server)
	downloadd(fileinfomap_server, fileinfomap_client, fileinfomap_clientindex)
	uploadd(fileinfomap_server, fileinfomap_client, fileinfomap_clientindex)
	updateindex(fileinfomap_clientindex)

	#except Exception as e:
	#	print("Client: " + str(e))

