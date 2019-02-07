import socket 
import os
import time
import sys
import _thread
import threading
import datetime


class MyServer:
    def __init__(self, port, doc_root):
        self.port = port
        self.doc_root = doc_root
        self.host = "0.0.0.0"
        self.print_lock = threading.Lock() 
  
  
    def main(self): 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        s.bind((self.host, self.port)) 
    
        s.listen() 
    
        while True: 
            conn, addr = s.accept() 

            self.print_lock.acquire()  
    
            _thread.start_new_thread(self.threaded, (conn,)) 


    def threaded(self,conn): 
        buffer=b''
        try:
            conn.settimeout(5.0)
            data=conn.recv(1024)
            buffer+=data.decode('utf-8')
            if '\r\n\r\n' in buffer:
                response=self.doString(buffer)
                self.print_lock.release()
                conn.sendall(response)
        except socket.timeout:
            response=self.doString(buffer)
            self.print_lock.release()
            conn.sendall(response) 
        finally:
            conn.close()        
       


    def doString(self,buffer):
        if '\r\n\r\n' not in buffer:
            return self.is400()
        
        list=buffer.split('\r\n\r\n')[0].split('\r\n')

        if list[0].split()[0]!="GET" or list[0].split()[2]!="HTTP/1.1" or type(list[0].split()[1])!=str:
            return self.is400()

        for line in list[1:]:
            if ": " in line:
                lineList=line.split(': ')
                if len(lineList)==2:
                    for key_or_val in lineList:
                        if ':' not in key_or_val:
                            continue
                        else:
                            return self.is400()
                else:
                    return self.is400()
            else:
                return self.is400()
            
        
        if list[0].split()[1][0]=='/': # start with /
            route=self.doc_root+list[0].split()[1]
        else:
            route=self.doc_root+'/'+list[0].split()[1]

        self.url=list[0].split()[1]

        if list[0].split()[1]=='/':
            if os.path.exists(route+'index.html'):
                # 200
                return self.is200('text/html',route)
            else:
                # 404
                return self.is404()
        else: # check if file path is valid 
            if route[-4:]=='.jpg':
                if os.path.exists(route):
                    # 200
                    return self.is200('image/jpeg',route)
                else:
                    # 404
                    return self.is404()
                
            elif route[-4:]=='.png':
                if os.path.exists(route):
                    # 200
                    return self.is200('image/png',route)
                else:
                    # 404
                    return self.is404()

            elif route[-5:]=='.html':
                if os.path.exists(route):
                    # 200
                    return self.is200('text/html',route)
                else:
                    # 404
                    return self.is404()
        
    
    def is400(self):
        strlen=len('HTTP/1.1 400 Client Error\r\nServer: Myserver 1.0\r\nContent-Type: text/html\r\nContent-Length: ')
        strlen+=len(str(strlen))
        return 'HTTP/1.1 400 Client Error\r\nServer: Myserver 1.0\r\nContent-Type: text/html\r\nContent-Length: '+str(strlen)+'\r\n\r\n'
    def is404(self):
        strlen=len('HTTP/1.1 404 Client Error\r\nServer: Myserver 1.0\r\nContent-Type: text/html\r\nContent-Length: ')
        strlen+=len(str(strlen))
        return 'HTTP/1.1 404 Client Error\r\nServer: Myserver 1.0\r\nContent-Type: text/html\r\nContent-Length: '+str(strlen)+'\r\n\r\n'
    def is200(self,ttype,route):
        f=open(self.doc_root+self.url,'rb')
        file_content=f.read()
        resList=[]
        resList.append('HTTP/1.1 200 OK')
        resList.append('Server: Myserver 1.0')
        t = os.path.getmtime(route)
        tt = datetime.datetime.fromtimestamp(t)
        ttt = tt.strftime('%a, %d %b %y %T %z')
        resList.append('Last-Modified: '+ttt)
        resList.append('Content-Type: '+ ttype)
        resList.append('Content-Length: '+str(os.path.getsize(route)))
        resList.append('\r\n')
        res='\r\n'.join(resList)
        response=bytes(res,'utf-8')+file_content
        return response


if __name__ == '__main__': 
    input_port = int(sys.argv[1])
    input_doc_root = sys.argv[2]
    server = MyServer(input_port, input_doc_root)
    server.main()





