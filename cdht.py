import socket
import sys
import random
import threading
import time

class Ping_and_file:
    def __init__(self, identity, first, second,mss,loss_r):
        self.my_name = identity
        self.first_peer= first
        self.first_lost = 0    
        self.second_lost = 0   
        self.request_port = 0
        self.run = True
        self.port = 50000
        self.MSS = mss
        self.loss = loss_r*100
        self.second_peer= second
        self.first_pre= None
        self.second_pre= None
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


    def UDPclient(self):
        while True and self.run:
            self.ping_peers("first") 
            self.ping_peers("second")
            time.sleep(20)
    
    def ping_peers(self, string):
        if string == "first":
            message = "A ping request message was received from peer {0} first".format(self.my_name)
            self.sock.sendto(message.encode(), ('localhost', self.first_peer+ self.port))
        else:
            message = "A ping request message was received from peer {0} second".format(self.my_name)
            self.sock.sendto(message.encode(), ('localhost', self.second_peer+ self.port))
        try:
            self.sock.settimeout(2.0)     
            data, addr = self.sock.recvfrom(4096)
            self.sock.settimeout(None)
        except socket.timeout:
            if string == "first":
                self.first_lost += 1
                if self.first_lost > 2:   
                    print("Peer {0} is no longer alive.".format(self.first_peer))
                    self.first_peer= self.second_peer 
                    print("My first successor is now peer {0}".format(self.first_peer))
                    temp = self.TCPclient(self.second_peer+ self.port, "What's your next successor")   
                    self.second_peer= int(temp)   
                    print("My second successor is now peer {0}".format(self.second_peer))
                    self.ping_peers("first")
                    time.sleep(1)     
                    self.ping_peers("second")
            if string == "second":
                self.second_lost += 1
                if self.second_lost > 2:
                    print("Peer {0} is no longer alive.".format(self.second_peer))
                    print("My first successor is now peer {0}".format(self.first_peer))
                    temp = self.TCPclient(self.first_peer+ self.port, "What's your next successor")
                    self.second_peer= int(temp)
                    print("My second successor is now peer {0}".format(self.second_peer))
                    self.ping_peers("first")
                    time.sleep(1)     
                    self.ping_peers("second")
        else:
            print(data.decode())
    def UDPSend(self,peer_port,message):
        a = random.randint(0,100)
        if a>self.loss:
            self.sock.sendto(message,('localhost',peer_port))
            return 1
        else:
            return 0
    def sendfile(self,peer_port,filename):
        check = 0
        total_size = 1
        receive_size = 0
        log = []
        with open(filename,'rb') as file:
            while check < 2:
                loss = random.randint(0,100)
                message = file.read(self.MSS)
                time1 = time.time()
                send = "snd"
                recv = "rcv"
                Rtx = "RTX"
                Drop = "Drop"
                Rtx_d = "RTX/Drop"
                p_size = len(message)
                check_rtx = 0
                check_success = 0
                time1 = time.time()
                log_m = "{0}      {1}      {2}      {3}      {4}\n".format(send,time1,total_size,p_size,receive_size)
                log.append(log_m)
                while check_success == 0:
                    check_success = self.UDPSend(peer_port,message)
                    time1 = time.time()
                    if check_success == 0 and check_rtx == 0:
                        time1 = time.time()
                        log_m = "{0}      {1}      {2}      {3}      {4}\n".format(Drop,time1,total_size,p_size,receive_size)
                        log.append(log_m)
                    try:
                        self.sock.settimeout(1)
                        data,addr = self.sock.recvfrom(2048)
                        self.sock.settimeout(None)
                        data = data.decode()
                        if "Have received." in data:
                            time1 = time.time()
                            total_size = total_size + p_size
                            log_m = "{0}      {1}      {2}      {3}      {4}\n".format(recv,time1,receive_size,p_size,total_size)
                            log.append(log_m)
                            check_rtx = 0
                    except socket.timeout:
                        if check_rtx == 0:
                            time1 = time.time()
                            log_m = "{0}      {1}      {2}      {3}      {4}\n".format(Rtx,time1,receive_size,p_size,total_size)
                            log.append(log_m)
                            check_rtx+=1
                        else:
                            time1 = time.time()
                            log_m = "{0}      {1}      {2}      {3}      {4}\n".format(Rtx_d,time1,receive_size,p_size,total_size)
                            log.append(log_m)
                if len(message)<self.MSS:
                    check =  check + 1           
        file.close()
        with open("sender_log.txt",'w') as log_file:
            for item in log:
                log_file.write(item)
        log_file.close()

                
    def UDPserver(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('localhost', self.my_name + self.port))
        c = b'A'
        data_pdf= []
        receive_log = []
        send = "snd"
        receive = "rcv"
        total_size = 1
        send_size = 0
        while True and self.run: 
            data1, addr = sock.recvfrom(4096)
            data2 = data1
            check = data2.split()
            p_size = len(data1)
            if len(data1) == 0:
                    filename = "received{0}.pdf".format(self.my_name)
                    with open(filename,'wb') as file:
                        for item in data_pdf:
                            file.write(item)
                    file.close()
                    with open("receiver_log.txt",'w') as rec_log:
                        for item in receive_log:
                            rec_log.write(item)
                    rec_log.close()
            elif len(data1)>55 or len(data1)<50:
                data_pdf.append(data1)
                message = "Have received."
                time1 = time.time()
                sock.sendto(message.encode(), addr)
                send_m = "{0}     {1}     {2}     {3}     {4}\n".format(receive,time1,total_size,p_size,send_size)
                receive_log.append(send_m)
                total_size = total_size + p_size
                time1 = time.time()
                send_m = "{0}     {1}     {2}     {3}     {4}\n".format(send,time1,send_size,p_size,total_size)
                receive_log.append(send_m)
            else:
                data = data1.decode().split()
                if data[-1] == "first":
                    self.first_pre= int(data[-2])
                    print(' '.join(data[:-1]))
                    response = "A ping response message was received from Peer {0}".format(self.my_name)
                    sock.sendto(response.encode(), addr)
                elif data[-1] == "second":
                    self.second_pre= int(data[-2])
                    print(' '.join(data[:-1]))
                    response = "A ping response message was received from Peer {0}".format(self.my_name)
                    sock.sendto(response.encode(), addr)
                        
    def TCPclient(self, port, message = None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', port))
        sock.sendall(message.encode())
        if "File request message for" in message:
            sock.close()
            return None
        if ("is not stored here" in message) or ("Received a response message from peer" in message) or ("will depart from the network" in message):
            sock.close()
            return None
        if "will depart from" in message:
            sock.close()
            return None
        data = sock.recv(4096).decode()
        sock.close()
        return data

    def TCPserver(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', self.my_name + self.port))
        sock.listen(100)
        while True and self.run:
            conn, addr = sock.accept()
            data = conn.recv(4096).decode()
            if "What's your next successor" in data:
                message = str(self.first_peer)
                conn.sendall(message.encode())
            if ("File request message for" in data) or ("is not stored here" in data):
                messages = data.split()
                if "File request message for" in data:
                    file = int(messages[4])
                if "is not stored here" in data:
                    file = int(messages[1])
                port = int(messages[-1])
                request_port = int(messages[-2])
                stored = file % 256
                if (port < stored and stored <= self.my_name) or (self.my_name < port and stored > port) or (self.my_name < port and stored <= self.my_name):
                    message = "Received a response message from peer {0} , which has the file {1}".format(self.my_name, file)
                    print("File {0} is here.".format(file))
                    print("A response message, destined for peer {0}, has been sent.".format(request_port))
                    self.TCPclient(request_port + self.port, message)
                else:
                    message = "File {0} is not stored here.\nFile request message has been forwarded to my successor. {1} {2}".format(file, request_port, self.my_name)
                    print(message[:-4])
                    self.TCPclient(self.first_peer+ self.port, message)
            if "Waiting for transfer" in data:
                messages = data.split()
                peer_port = int(messages[-2])+self.port
                file = messages[-1]
                lenm = 4 - len(file)
                file_name = ""
                for i in range(0,lenm):
                    file_name = file_name + "0"
                file_name = file_name + "{0}.pdf".format(messages[-1])
                self.sendfile(peer_port,file_name)
            if "Received a response message from peer" in data:
                print(data)
                messages = data.split()
                peer_port = int(messages[6])+self.port
                filename = messages[-1]
                message = "Waiting for transfer {0} {1}".format(self.my_name,filename)
                self.TCPclient(peer_port,message)
            if "will depart from the network." in data:
                parts = data.split()
                if self.first_peer == int(parts[1]):
                    print(" ".join(parts[:-2]))
                    self.first_peer= int(parts[-2])
                    self.second_peer= int(parts[-1])
                    print("My first successor is now peer {0}.".format(self.first_peer))
                    print("My second successor is now peer {0}.".format(self.second_peer))
                    
                    self.ping_peers("first")
                    time.sleep(1)
                    self.ping_peers("second")
                elif self.second_peer == int(parts[1]):
                    print(" ".join(parts[:-2]))
                    self.second_peer= int(parts[-2])
                    print("My first successor is now peer {0}.".format(self.first_peer))
                    print("My second successor is now peer {0}.".format(self.second_peer))
                    self.TCPclient(self.first_peer+ self.port, data)
                    self.ping_peers("first")
                    time.sleep(1)
                    self.ping_peers("second")
                else:
                    self.TCPclient(self.first_peer+ self.port, data)
                
        
    def get_input(self):
        while True and self.run:
            string = input()
            if "request" in string:
                message = "File request message for {0} has been sent to my successor. {1} {2}".format(string.split()[1], self.my_name, self.my_name)
                print(message[:-4])
                self.TCPclient(self.first_peer+ self.port, message)

            if "quit" in string:
                print("Leaving...Please wait...")
                message = "Peer {0} will depart from the network. {1} {2}".format(self.my_name, self.first_peer, self.second_peer)           
                self.TCPclient(self.first_peer+ self.port, message)
                
                self.run = False

                
                

def run_peer():
    identity = int(sys.argv[1])
    first = int(sys.argv[2])
    second = int(sys.argv[3])
    mss = int(sys.argv[4])
    loss_r = float(sys.argv[5])
    Start_peer = Ping_and_file(identity, first, second,mss,loss_r)
    UDPclient = threading.Thread(target = Start_peer.UDPclient, args = ())
    UDPserver = threading.Thread(target = Start_peer.UDPserver, args = ())
    TCPserver = threading.Thread(target = Start_peer.TCPserver, args = ())
    quit_and_request = threading.Thread(target = Start_peer.get_input)
    UDPclient.start()
    UDPserver.start()
    TCPserver.start()
    quit_and_request.start()


run_peer()
