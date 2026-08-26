import threading
import socket
import time
import sys

curr_port = int(sys.argv[1])
target_port = int(sys.argv[2])

def listen_port():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', curr_port))
    server.listen()

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        print("Received:", data)

def check_status():
    while True:
        time.sleep(5)
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(('localhost', target_port))
            client.send(b'Heartbeat...')
            client.close()

        except ConnectionRefusedError:
            print(f'Could not reach coordinator on port {target_port}')

thread = threading.Thread(target=listen_port)
thread.start()

heartbeat_thread = threading.Thread(target=check_status)
heartbeat_thread.start()

while True:

    time.sleep(2)
    print("Coordinator running...")
