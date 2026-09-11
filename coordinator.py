import threading
import socket
import time
import sys
import json
import psycopg2

curr_port = int(sys.argv[1])
worker_port = int(sys.argv[2])
other_ports = [int(p) for p in sys.argv[3:]]
last_hb_recv = {port: time.time() for port in other_ports}
all_ports = other_ports + [curr_port]
leader_port = None

term = 0
max_attempts = 3

def listen_port():
    global last_hb_recv, leader_port, term
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', curr_port))
    server.listen()

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        msg = json.loads(data.decode())

        if msg["type"] == "who_is_leader":
            reply = {"leader_port": leader_port, "term": term}
            conn.send(json.dumps(reply).encode())
            continue
            
        if msg["term"] < term:
            print(f"Ignoring outdated message from term {msg['term']}, current term is {term}.")
            continue

        if msg["term"] > term:
            term = msg["term"]
            if msg["type"] == "new_leader":
                leader_port = msg["leader_port"]
            print(f"Updated to term {term}")

        if msg["type"] == "heartbeat":
            sender = msg["from_port"]
            last_hb_recv[sender] = time.time()
            print(f"Heartbeat detected from port {sender}")
        elif msg["type"] == "new_leader":
            leader_port = msg["leader_port"]
            print(f"New leader port {leader_port} {term}.")

def check_status():
    while True:
        time.sleep(5)
        for port in other_ports:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(('localhost', port))
                msg = {"type": "heartbeat", "from_port": curr_port, "term": term}
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
    global leader_port, term
    alive_ports = [curr_port]

    for port in other_ports:
        if port == leader_port:
            continue
        elapsed = time.time() - last_hb_recv[port]
        if elapsed < 15:
            alive_ports.append(port)

    new_leader = max(alive_ports)

    if new_leader != curr_port:
        return

    term += 1
    leader_port = new_leader
    print(f"New leader elected: {leader_port} term {term}.")

    for port in other_ports:
        if port == new_leader:
            continue
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(('localhost', port))
            msg = {"type": "new_leader", "leader_port": leader_port, "term": term}
            client.send(json.dumps(msg).encode())
            client.close()
        except ConnectionRefusedError:
            print(f"Could not reach {port} to announce new leader.")

def ask_who_is_leader():
    global leader_port, term
    for port in other_ports:
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(('localhost', port))
            client.send(json.dumps({"type": "who_is_leader"}).encode())
            response = client.recv(1024)
            reply = json.loads(response.decode())
            client.close()

            if reply["term"] > term:
                term = reply["term"]
                leader_port = reply["leader_port"]
                print(f"Learned from {port}: leader is {leader_port}, term {term}")
                return
        except ConnectionRefusedError:
            continue

def handle_worker(conn):

    try:
        print("connecting to database.")
        db_conn = psycopg2.connect(
        dbname="djs",
        user="kais",
        host="localhost",
        port=5432
        )
        cur = db_conn.cursor()
        print("database connecting successfully...")

        data = conn.recv(1024)
        print(data)

        one_job = None

        claimed_time = time.time()

        cur.execute("""
            UPDATE tasks SET status = %s, claimed_time = %s
            WHERE job_id = (SELECT job_id FROM tasks WHERE status = 'pending' LIMIT 1)
            RETURNING *
        """, ("running", claimed_time))
        result = cur.fetchone()
        db_conn.commit()

        if result is None:
            conn.send(json.dumps({"job": None}).encode())
        else:
            queue_job = {
                    "job_id": result[0],
                    "status": result[1],
                    "attempts": result[2],
                    "claimed_time": result[3]
                }

            conn.send(json.dumps({"job": queue_job}).encode())

            job_back = conn.recv(1024)
            job_text = job_back.decode()
            job_result = json.loads(job_text)

            if job_result["status"] == "success":
                job_status = "success"
            elif job_result["attempts"] < max_attempts:
                job_status = "pending"
            else:
                job_status = "dead"

            cur.execute("UPDATE tasks SET status = %s, attempts = %s WHERE job_id = %s", (job_status, job_result['attempts'], job_result['job_id']))
            db_conn.commit()

        db_conn.close()

    except Exception as e:
        print(f"CRASH in handle_worker: {e}")

def listen_for_workers():
    worker_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    worker_server.bind(('localhost', worker_port))
    worker_server.listen()

    while True:
        conn, addr = worker_server.accept()
        print("Worker connection accepted, leader_port is:", leader_port, "curr_port is:", curr_port)

        if leader_port != curr_port:
            conn.send(json.dumps({"error": "not_leader", "leader_port": leader_port}).encode())
            conn.close()
            continue

        worker_thread = threading.Thread(target=handle_worker, args=(conn,))
        worker_thread.start()

def startup():
    global leader_port, term
    leader_port = max(all_ports)
    ask_who_is_leader()

startup()

thread = threading.Thread(target=listen_port)
thread.start()

heartbeat_thread = threading.Thread(target=check_status)
heartbeat_thread.start()

check_dead_thread = threading.Thread(target=check_dead_leader)
check_dead_thread.start()

worker_listener_thread = threading.Thread(target=listen_for_workers)
worker_listener_thread.start()

while True:

    time.sleep(2)
    print("Coordinator running...")
    