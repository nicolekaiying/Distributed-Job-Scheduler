import threading
import socket
import time
import sys
import json

curr_port = int(sys.argv[1])
other_ports = [int(p) for p in sys.argv[2:]]
last_hb_recv = {port: time.time() for port in other_ports}
all_ports = other_ports + [curr_port]
leader_port = max(all_ports)

def listen_port():
    global last_hb_recv
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', curr_port))
    server.listen()

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        msg = json.loads(data.decode())

        if msg["type"] == "heartbeat":
            sender = msg["from_port"]
            last_hb_recv[sender] = time.time()
            print(f"Heartbeat detected from port {sender}")
        elif msg["type"] == "new_leader":
            leader_port = msg["leader_port"]
            print(f"New leader port {leader_port} notified.")

def check_status():
    while True:
        time.sleep(5)
        for port in other_ports:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(('localhost', port))
                msg = {"type": "heartbeat", "from_port": curr_port}
                client.send(json.dumps(msg).encode())
                client.close()

            except ConnectionRefusedError:
                print(f'Could not reach coordinator on port {port}')

def check_dead_leader():
    global last_hb_recv
    while True:
        time.sleep(5)
        if leader_port == curr_port:
            continue

        elapsed = time.time() - last_hb_recv[leader_port]
        print(f"Time since last heartbeat from leader ({leader_port}): {elapsed:.1f} seconds..")
        if elapsed > 15:
            print(f"Leader on {leader_port} appears dead...")
            promote_new_leader()

def promote_new_leader():
    global leader_port
    alive_ports = [curr_port]

    for port in other_ports:
        if port == leader_port:
            continue
        elapsed = time.time() - last_hb_recv[port]
        if elapsed < 15:
            alive_ports.append(port)

        new_leader = max(alive_ports)
        leader_port = new_leader
        print(f"New leader elected: {leader_port}.")

        for port in other_ports:
            if port == new_leader:
                continue
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(('localhost', port))
                msg = {"type": "new_leader", "leader_port": leader_port}
                client.send(json.dumps(msg).encode())
                client.close()
            except ConnectionRefusedError:
                print(f"Could not reach {port} to announce new leader.")

thread = threading.Thread(target=listen_port)
thread.start()

heartbeat_thread = threading.Thread(target=check_status)
heartbeat_thread.start()

check_dead_thread = threading.Thread(target=check_dead_leader)
check_dead_thread.start()

while True:

    time.sleep(2)
    print("Coordinator running...")
    