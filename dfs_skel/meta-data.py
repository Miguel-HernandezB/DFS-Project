###############################################################################
#
# Filename: meta-data.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#
# Please modify globals with appropiate info.

from mds_db import *
from Packet import *
import sys
import socketserver

def usage():
	print ("""Usage: python %s <port, default=8000>""" % sys.argv[0])
	sys.exit(0)


class MetadataTCPHandler(socketserver.BaseRequestHandler):

	def handle_reg(self, db, p):
		"""Register a new client to the DFS  ACK if successfully REGISTERED
			NAK if problem, DUP if the IP and port already registered
		"""
		try:
			
			#Retriving address and port 
			address = p.getAddr()
			port = p.getPort()

			## Using AddDataNode() returns row ID on success, if duplicate then zero 
			if db.AddDataNode(address, port):
				self.request.sendall("ACK") 
			else:
				self.request.sendall("DUP")
		except:
			self.request.sendall("NAK")

	def handle_list(self, db):
		"""Get the file list from the database and send list to client"""
		try:
			
			file_list = db.GetFiles()													##We get files from data base	

			response_list_packet = Packet.BuildListResponse(file_list)					##File list is transformed into a packet

			encoded_response_packet = response_list_packet.GetEncodedPacket()			##The packet is then encoded into a json format to be sent to the network

			self.request.sendall(encoded_response_packet)								##Send encoded packet list

		except:
			self.request.sendall("NAK")	

	def handle_put(self, db, p):
		"""Insert new file into the database and send data nodes to save
		   the file.
		"""
		##Getting the file size and name from the packet
		file_name, file_size = p.getFileInfo()
	
	
		if db.InsertFile(file_name, file_size):											##InsertFile returns 1 if file was successfully inserted, otherwise 0
			available_nodes = db.GetDataNodes()											##Search for the available data nodes in database, GetDataNode() returns a list of tuples (addr, port)
			
			p.BuildPutResponse(available_nodes)											##Build Packet with the available nodes
			self.request.sendall(p.getEncodePacket())									##Send encoded packet

		else:
			self.request.sendall("DUP")
	
	def handle_get(self, db, p):
		"""Check if file is in database and return list of
			server nodes that contain the file.
		"""

		file_name = p.getFileName()														##Getting desired file name from packet
		
		fsize, file_blocks = db.GetFileInode(file_name)									##Search file name in the database using .GetFileInfo() (returns tuple of fsize and , otherwise returns none)

		if fsize:																		##fsize == none file not found
			p.BuildGetResponse(file_blocks, fsize)										##Make packet with the list of the file's data block's location
			self.request.sendall(p.getEncodedPacket())									##Send encoded packet

		else:
			self.request.sendall("NFOUND")

	def handle_blocks(self, db, p):
		"""Add the data blocks to the file inode"""

		# Fill code to get file name and blocks from
		# packet
	
		# Fill code to add blocks to file inode

		
	def handle(self):

		# Establish a connection with the local database
		db = mds_db("dfs.db")
		db.Connect()

		# Define a packet object to decode packet messages
		p = Packet()

		# Receive a msg from the list, data-node, or copy clients
		msg = self.request.recv(1024)
		print msg, type(msg)
		
		# Decode the packet received
		p.DecodePacket(msg)
	

		# Extract the command part of the received packet
		cmd = p.getCommand()

		# Invoke the proper action 
		if   cmd == "reg":
			# Registration client
			self.handle_reg(db, p)

		elif cmd == "list":
			# Client asking for a list of files
			# Fill code
		
		elif cmd == "put":
			# Client asking for servers to put data
			# Fill code
		
		elif cmd == "get":
			# Client asking for servers to get data
			# Fill code

		elif cmd == "dblks":
			# Client sending data blocks for file
			 # Fill code


		db.Close()

if __name__ == "__main__":
    HOST, PORT = "", 8000

    if len(sys.argv) > 1:

		try:
			PORT = int(sys.argv[1])
		except:
			usage()

	server = socketserver.TCPServer((HOST, PORT), MetadataTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
