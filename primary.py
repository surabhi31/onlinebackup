from socket import *
import sys
import os
import time
homedir = "./"
def main():
    # Your implementation here
    serverName = sys.argv[1] 
    serverPort = int(sys.argv[2])
    fileName = sys.argv[3]
    print "Server is: ", serverName 
    print "Port is: ", serverPort
    print "Filename is: ", fileName

    filepath = homedir + fileName;
    #print "Filepath is ", filepath

    fileSize = os.path.getsize(filepath)
    #print "Filesize is ", fileSize

    # estabish a connection with the backup server 
    clientSock = socket(AF_INET, SOCK_STREAM)
    if clientSock.connect((serverName,serverPort)):
	print "Connection to the server done"

    try:
        m_init = fileName + ";" + str(fileSize)
        #print "m_init is ", m_init

        message_send(clientSock, m_init)
        m_opcode = message_receive(clientSock)
        #print "m_opcode is", m_opcode
        bytesTransferred = 0
        buf = ""
        m_payload = ""
        if m_opcode.split(';')[1] == "OP_ALREADY_HAVE":
            print "Client: File already backed up"
        elif m_opcode.split(';')[1] == "OP_READY_TO_RECEIVE":
            print "Client: server is ready to receive now"
            toRecv = int(m_opcode.split(';')[-1])
            # open file for reading now
            fh = open(filepath, 'rb')
            bytesRemainingToTransfer = toRecv
            #curTime = time.time()
            #time.sleep(20)
            while bytesRemainingToTransfer > 0:
                fh.seek(int(fileSize) - bytesRemainingToTransfer)
                if (bytesRemainingToTransfer) >= 8192:
			buf = fh.read(8192)
			bytesTransferred = bytesTransferred + 8192
			bytesRemainingToTransfer = toRecv - bytesTransferred
		else:
			buf = fh.read(bytesRemainingToTransfer)
                #time.sleep(20)
		m_payload = fileName + ";" + buf 
		message_send(clientSock, m_payload)
		print "Client: payload sent to server"
		m_opcode = message_receive(clientSock)
		if m_opcode.split(";")[1] == "OP_SYNC_COMPLETE":
			print "Sync is complete"
                        #totalTime = time.time() - curTime
                        #print "Total time to transfer file is:", totalTime
                        m_opcode = "OP_TERMINATE" 
                        message_send(clientSock, m_payload)
			fh.close()
			#sys.exit()
		elif m_opcode.split(";")[1] == "OP_CHUNK_RECEIVED":
			print "Client: server received chunk"
        elif m_opcode is '':
            sys.exit() 
    finally:
        clientSock.close()
        sys.exit()
        
def message_send(conSock, message):
    m_opcode = str(len(message)) + ";" + message
    conSock.send(m_opcode)
    return m_opcode

def message_receive(conSock):
    message_size = ""
    message_size_char = b''.join(conSock.recv(1))
    while(message_size_char is not ";"):
      message_size += message_size_char
      message_size_char = b''.join(conSock.recv(1))
    #print "message_receive_size: " + message_size
    message = conSock.recv(int(message_size))
    return message

if __name__ == '__main__':
    main()

