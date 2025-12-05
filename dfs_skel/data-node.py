###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	data node server for the DFS
#

from Packet import *

import sys
import socket
import socketserver
import uuid
import os.path

def usage():
	print("""Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0])
	sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
	"""Creates a connection with the metadata server and
	   register as data node
	"""

	# Establish connection
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((meta_ip, meta_port))

	try:
		response = "NAK"
		sp = Packet()
		while response == "NAK":
			sp.BuildRegPacket(data_ip, data_port)
			sock.sendall(sp.getEncodedPacket())
			response = sock.recv(1024)

			if response == "DUP":
				print("Duplicate Registration")

			if response == "NAK":
				print("Registratation ERROR")

	finally:
		sock.close()
	

class DataNodeTCPHandler(socketserver.BaseRequestHandler):

	def handle_put(self, p):

		"""Receives a block of data from a copy client, and 
		   saves it with an unique ID.  The ID is sent back to the
		   copy client.
		"""

		fname, fsize = p.getFileInfo()
		self.request.send("OK")

		# Generates an unique block id.
		blockid = str(uuid.uuid1())


		# Open the file for the new data block.  
		# Receive the data block.
		# Send the block id back
		file_path = os.path.join(DATA_PATH, blockid)					##creates a file path with the blockid as the File's name

		data = self.request.recv(fsize)									##Receive the data block.
		decoded_data = data.decode("utf-8")								##Decode into string

		try:
			with open(file_path, 'wb') as f:							##Open the file for the new data block.
				f.write(decoded_data)									##Write the data recieved 
			
			self.request.send(blockid)									##Send the block id back (type string)
		except:
			print("Error while writing block to node")		

	def handle_get(self, p):
		
		# Get the block id from the packet
		blockid = p.getBlockID()


		# Read the file with the block id data
		# Send it back to the copy client.
		
		file_path = os.path.join(DATA_PATH, blockid)

		if os.path.exists(file_path):
			with open(file_path, 'rb') as f:
				data = f.read()
			self.request.sendall(data.encode("utf-8"))
		
		else:
			print("Error: Block not found")



	def handle(self):
		msg = self.request.recv(1024)
		print(msg, type(msg))

		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()
		if cmd == "put":
			self.handle_put(p)

		elif cmd == "get":
			self.handle_get(p)
		

if __name__ == "__main__":

	META_PORT = 8000
	if len(sys.argv) < 4:
		usage()

	try:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])
		DATA_PATH = sys.argv[3]

		if len(sys.argv > 4):
			META_PORT = int(sys.argv[4])

		if not os.path.isdir(DATA_PATH):
			print("Error: Data path %s is not a directory." % DATA_PATH)
			usage()
	except:
		usage()


	register("localhost", META_PORT, HOST, PORT)
	server = socketserver.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
	server.serve_forever()
