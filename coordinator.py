import threading
import socket
import time
import sys
import json

curr_port = int(sys.argv[1])
other_ports = [int(p) for p in sys.argv[2:]]
last_hb_recv = {port: time.time() for port in other_ports}

def listen_port():
    global last_hb_recv
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', curr_port))
    server.listen()

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        msg = json.loads(data.decode())
        sender = msg['from_port']
        last_hb_recv[sender] = time.time()
        print(f"Heartbeat detected from {sender} ")

def check_status():
    while True:
        time.sleep(5)
        for port in other_ports:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(('localhost', port))
                msg = {"from_port": curr_port}
                client.send(json.dumps(msg).encode())
                client.close()

            except ConnectionRefusedError:
                print(f'Could not reach coordinator on port {port}')

def check_dead_leader():
    global last_hb_recv
    while True:
        time.sleep(5)
        for port in other_ports:
            elapsed = time.time() - last_hb_recv[port]
            print(f"Time since last heartbeat {elapsed:.1f} seconds..")
            if elapsed > 15:
                print(f"Coordinator on {port} appears dead, starting leader election..")

thread = threading.Thread(target=listen_port)
thread.start()

heartbeat_thread = threading.Thread(target=check_status)
heartbeat_thread.start()

check_dead_thread = threading.Thread(target=check_dead_leader)
check_dead_thread.start()

while True:

    time.sleep(2)
    print("Coordinator running...")
