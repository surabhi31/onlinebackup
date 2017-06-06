import socket
import select 
import sys
import pickle
import os.path
import time

homedir = "./"
datapkl = homedir + "fs.pkl"
timeout_sec = 15
fileWriteBufferFreq = 8

class FileConnectionManager:
   def __init__(self, fileno, filedb):
      self.fileno = fileno
      self.filedb = filedb
      self.request_state = "INITIALIZE"
      self.response_state = ""
      self.other_states = {}
      self.nextRequestHolder = b''
      self.nextResponseHolder = ""
      self.file = ""
      self.fileWriteBuffer = b''
      self.fileWriteBufferSize = 0

   def processReadMessage(self):
      m_data = self.nextRequestHolder
      self.nextRequestHolder = b'' #Making this empty for next read of message
      filename = m_data.split(";")[0]
      if self.request_state == "INITIALIZE":
         if filename == "OP_TERMINATE":
            sys.exit("Backup server stopped by OP_TERMINATE")
         else:
            self.file = filename
            filepath = homedir + filename
            filesize = int(m_data.split(";")[1])
            if filename not in self.filedb.keys():
               self.filedb[filename] = [0, filepath]

            oldSize = self.filedb[filename][0]
            print "OLD SIZE:" + str(oldSize) +"  Asked size:" + str(filesize)
            diff = filesize - oldSize
            if diff == 0:
               self.response_state = "OP_ALREADY_HAVE"
               print "File already fully backed up"
            else:
               self.response_state = "OP_READY_TO_RECEIVE"
               self.byteRecvd = 0
               self.totalBytesToReceive = diff
               self.chunkCounter = 0
               print "Server ready to receive payload"         
      elif self.request_state == "PROCESS_PAYLOAD":
         byteRecvd = self.byteRecvd
         totalBytesToReceive = self.totalBytesToReceive
         payld = m_data.replace(filename + ";","",1)
         #print len(payld)

         self.fileWriteBufferSize += 1;
         self.fileWriteBuffer += payld
         self.filehandler(False)
         
         print self.filedb[filename]
         byteRecvd += len(payld)
         self.byteRecvd = byteRecvd
         #print "bytes received till now", byteRecvd
         if(byteRecvd < totalBytesToReceive):
            self.response_state = "OP_CHUNK_RECEIVED"
         else:
            self.response_state = "OP_SYNC_COMPLETE"
         self.chunkCounter += 1
      else:
         print "Something sinister is going on in here"

   def processWriteMessage(self):
      if self.response_state == "OP_ALREADY_HAVE":
         self.nextResponseHolder = self.generateOpcode(self.file + ";" + "OP_ALREADY_HAVE")
         self.request_state = "CLOSE"
      elif self.response_state == "OP_SYNC_COMPLETE":
         self.nextResponseHolder = self.generateOpcode(self.file + ";" + "OP_SYNC_COMPLETE" + ";" + str(self.chunkCounter))
         self.request_state = "CLOSE"
      elif self.response_state == "OP_READY_TO_RECEIVE":
         self.nextResponseHolder = self.generateOpcode(self.file + ";" + "OP_READY_TO_RECEIVE" + ";" + str(self.totalBytesToReceive))
         self.request_state = "PROCESS_PAYLOAD"
      elif self.response_state == "OP_CHUNK_RECEIVED":
         self.nextResponseHolder = self.generateOpcode(self.file + ";" + "OP_CHUNK_RECEIVED" + ";" + str(self.chunkCounter))
      else:
         print "Something sinister is going on in here"
         self.request_state = "CLOSE"

   def generateOpcode(self, msg):
      return str(len(msg)) + ";" + msg

   def processClose(self):
      self.filehandler(True)

   def filehandler(self, isEOF):
      if not isEOF and self.fileWriteBufferSize < fileWriteBufferFreq:
         print "Bufferring data"
         return
      if self.fileWriteBufferSize > 0:
         print "Writing data to disk"
         f = open(self.filedb[self.file][1], "ab+")
         f.write(self.fileWriteBuffer)
         f.close()	
         self.filedb[self.file][0] += len(self.fileWriteBuffer)
         self.fileWriteBufferSize = 0
         self.fileWriteBuffer = b''
         
def main():
   #Process Pickle
   if not os.path.exists(datapkl):	 
      filedb = dict()
   else:
      fh = open(datapkl,"r+")	
      filedb = pickle.load(fh)
      fh.close()

   serversocket = connection_s(sys.argv)
   epoll = select.epoll()
   epoll.register(serversocket.fileno(), select.EPOLLIN)
   try:
      connections = {}; connections_timers ={}; filehandlers = {}
      while True:
         events = epoll.poll(1)
         for fileno, event in events:
            if fileno == serversocket.fileno():
               connection, address = serversocket.accept()
               connection.setblocking(0) 
               epoll.register(connection.fileno(), select.EPOLLIN)
               
               connections[connection.fileno()] = connection
               connections_timers[connection.fileno()] = int(time.time()) + timeout_sec
               filehandlers[connection.fileno()] = FileConnectionManager(fileno, filedb)
               
            elif event & select.EPOLLIN:
               connections_timers[fileno] = int(time.time()) + timeout_sec
               receivedMessagePart = connections[fileno].recv(8192)
               if len(receivedMessagePart) == 0:
                  print "Received a null message, client must have been killed. Exiting"
                  isMessageFullyRead = False
                  epoll.modify(fileno, 0)
                  connections[fileno].shutdown(socket.SHUT_RDWR)
               else:
                  isMessageFullyRead = message_receive(receivedMessagePart, filehandlers[fileno])
               if(isMessageFullyRead):
                  filehandlers[fileno].processReadMessage()
                  filehandlers[fileno].processWriteMessage()
                  epoll.modify(fileno, select.EPOLLOUT)

            elif event & select.EPOLLOUT:
               byteswritten = connections[fileno].send(filehandlers[fileno].nextResponseHolder)
               filehandlers[fileno].nextResponseHolder = filehandlers[fileno].nextResponseHolder[byteswritten:]
               if len(filehandlers[fileno].nextResponseHolder) == 0:
                  if filehandlers[fileno].request_state != "CLOSE":
                     epoll.modify(fileno, select.EPOLLIN)
                  else:
                     epoll.modify(fileno, 0)
                     connections[fileno].shutdown(socket.SHUT_RDWR)
               connections_timers[fileno] = int(time.time()) + timeout_sec
            elif event & select.EPOLLHUP:
               filehandlers[fileno].processClose()
               pkl_handle_s(filedb)
               epoll.unregister(fileno)
               connections[fileno].close()
               connections_timers.pop(fileno)
               filehandlers.pop(fileno)

         ##Timeout expired connections##
         curr_time = int(time.time())
         for filno in connections_timers.keys():
            if connections_timers[filno] < curr_time:
               epoll.modify(filno, 0)
               connections[filno].shutdown(socket.SHUT_RDWR)
               print "Connection with fileno: "+ str(filno)+ " timed out"

   except SystemExit:
      print "Exiting gracefully, everything will be backed up finally"
   except KeyboardInterrupt:
      print "Exiting gracefully, everything will be backed up finally"
      
   finally:
      print "Saving all bufferred files to disk......."
      for fileno in filehandlers:
         filehandlers[fileno].processClose()
      print "Writing filedb to pickle file....."
      pkl_handle_s(filedb)
      print "Done writing filedb to pickle file....."
      epoll.unregister(serversocket.fileno())
      epoll.close()
      serversocket.close()

def connection_s(argv):
   server_name = argv[1]
   server_port = int(argv[2])
   serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
   serversocket.bind((server_name, server_port))
   serversocket.listen(5)
   serversocket.setblocking(0)
   return (serversocket)

def message_receive(receivedMessagePart, filehandler):
   filehandler.nextRequestHolder += receivedMessagePart
   total_message_size_recd = len(filehandler.nextRequestHolder)

   if total_message_size_recd > 10:
      message_size = ""
      for idx, val in enumerate(filehandler.nextRequestHolder):
         message_size_char = b''.join(val)
         total_message_size_recd -= 1
         if message_size_char == ";":
            break
         message_size += message_size_char
      #print "message_receive_size: " + message_size
      if int(message_size) == total_message_size_recd:
         filehandler.nextRequestHolder = filehandler.nextRequestHolder.replace(message_size + ";","",1)
         return True
   return False

def pkl_handle_s(filedb):
    fh = open(datapkl, "w+")
    pickle.dump(filedb, fh)
    fh.close()

if __name__ == '__main__':
    main()
