import threading
import socket
import time
import sys
import json
import psycopg2
import os
from psycopg2 import pool

curr_port = int(sys.argv[1])
worker_port = int(sys.argv[2])
my_host = os.environ.get("COORD_HOST", "localhost")
db_host = os.environ.get("DB_HOST", "localhost")
peer_hosts = {}
other_ports = []
last_hb_recv = {port: time.time() for port in other_ports}
all_ports = other_ports + [curr_port]
leader_port = None

term = 0
max_attempts = 3

def listen_port():
    global last_hb_recv, leader_port, term
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        server.bind(('0.0.0.0', curr_port)) #Listen on all network interfaces
    except OSError as e:
        print(f"Cannot bind port {curr_port}: {e}")
        os._exit(1)

    server.listen()

    while True:
        conn, addr = server.accept()
        with conn: #Using with conn because there are multiple loop bodies, this ensures conn gets closed whatever happens.
            data = conn.recv(1024)
            try:
                msg = json.loads(data.decode())
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"Ignoring bad message: {e}")
                continue

            if msg["type"] == "who_is_leader":
                reply = {"leader_port": leader_port, "term": term}
                conn.send(json.dumps(reply).encode())
                continue
                
            if msg["term"] < term:
                print(f"[OUTDATED] Message from term {msg['term']}, current term is {term}.")
                continue

            if msg["term"] > term:
                term = msg["term"]
                if msg["type"] == "new_leader":
                    leader_port = msg["leader_port"]
                print(f"[UPDATED] Term to {term}.")

            if msg["type"] == "heartbeat":
                sender = msg["from_port"]
                last_hb_recv[sender] = time.time()
                print(f"[HEARTBEAT] Detected from port {sender}.")
            elif msg["type"] == "new_leader":
                last_hb_recv[msg["leader_port"]] = time.time()
                print(f"[NEW LEADER] Claim for {msg['leader_port']} (term {msg['term']}); current leader is {leader_port} at term {term}.")

def check_status():
    while True:
        time.sleep(5)
        for port in other_ports:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect((peer_hosts.get(port, 'localhost'), port)) #Fallsback to localhost if port doesnt exists
                msg = {"type": "heartbeat", "from_port": curr_port, "term": term}
                client.send(json.dumps(msg).encode())
                client.close()

            except OSError:
                print(f'[UNREACHABLE] on port {port}.')

def check_dead_leader():
    global last_hb_recv, leader_port
    while True:
        time.sleep(5)

        if leader_port == curr_port:
            alive = [curr_port] + [p for p in other_ports
                                   if time.time() - last_hb_recv.get(p, 0) < 15]

            if len(alive) <= len(all_ports) / 2:
                print(f"[STEPPING DOWN] Only {len(alive)} of {len(all_ports)} reachable.")
                leader_port = None
            continue

        if leader_port is None:
            promote_new_leader()
            continue

        elapsed = time.time() - last_hb_recv.get(leader_port, 0) #Using brackets because [] demands the key exists, () falls back to 0 if it doesnt exists.
        print(f"[LEADER|{leader_port}] Time since last heartbeat: {elapsed:.1f} seconds.")
        if elapsed > 15:
            print(f"[LEADER] on {leader_port} appears dead.")
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

    if len(alive_ports) <= len(all_ports)/2:
        print(f"[UNREACHABLE] Only {len(alive_ports)} of {len(all_ports)} coordinators reachable -- Waiting.")
        return

    new_leader = max(alive_ports)

    if new_leader != curr_port:
        return

    try:
        term = next_term()
    except psycopg2.Error as e:
        print(f"Could not reach database to allocate a term: {e}")
        return

    leader_port = new_leader
    print(f"[ELECTED] {leader_port} elected as new Leader, term {term}.")

    for port in other_ports:
        if port == new_leader:
            continue
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((peer_hosts.get(port, 'localhost'), port))
            msg = {"type": "new_leader", "leader_port": leader_port, "term": term}
            client.send(json.dumps(msg).encode())
            client.close()
        except OSError:
            print(f"{port}] Could not be reach to announce new leader.")

def ask_who_is_leader():
    global leader_port, term, last_hb_recv
    best_term = term
    best_leader = leader_port

    for port in other_ports:
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((peer_hosts.get(port, 'localhost'), port))
            client.send(json.dumps({"type": "who_is_leader"}).encode())
            response = client.recv(1024)
            reply = json.loads(response.decode())
            client.close()

            if reply["leader_port"] is not None and reply["term"] >= best_term:
                best_term = reply["term"]
                best_leader = reply["leader_port"]
        except (OSError, json.JSONDecodeError):
            continue

    term = best_term
    leader_port = best_leader

    if leader_port is not None:
        last_hb_recv.setdefault(leader_port, time.time()) #Falls back to time.time() only if key is missing.
        print(f"[LEARNED] Leader is {leader_port}, term {term}")

def handle_worker(conn):
    db_conn = None

    try:
        db_conn = db_pool.getconn()
        cur = db_conn.cursor()

        data = conn.recv(1024)

        claimed_time = time.time()

        cur.execute("""
            WITH next_job AS (
            SELECT job_id FROM tasks
            WHERE status = 'pending'
            ORDER BY created_time ASC, job_id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE tasks SET status = %s, claimed_time = %s
        WHERE job_id = (SELECT job_id FROM next_job)
        RETURNING *
        """,("running", claimed_time))

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

    except Exception as e:
        print(f"[CRASH] in handle_worker: {e}")

    finally:
        if db_conn is not None:
            db_pool.putconn(db_conn)
        conn.close()

def listen_for_workers():
    worker_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        worker_server.bind(('0.0.0.0', worker_port))
    except OSError as e:
        print(f"Cannot bind port {worker_port}: {e}")
        os._exit(1)
    
    worker_server.listen()

    while True:
        conn, addr = worker_server.accept()

        if leader_port != curr_port:
            conn.send(json.dumps({"error": "not_leader", "leader_port": leader_port}).encode())
            conn.close()
            continue

        worker_thread = threading.Thread(target=handle_worker, args=(conn,))
        worker_thread.start()

def register_once():
    db_conn = psycopg2.connect(dbname="djs", user="kais", host=db_host, port=5432)
    cur = db_conn.cursor()

    cur.execute("""
        INSERT INTO coordinators (curr_port, worker_port, host, last_seen)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (curr_port) DO UPDATE SET last_seen = %s, worker_port = %s, host = %s
    """, (curr_port, worker_port, my_host, time.time(), time.time(), worker_port, my_host))

    db_conn.commit()
    db_conn.close()

def discover_coords():
    db_conn = psycopg2.connect(dbname="djs", user="kais", host=db_host, port=5432)
    cur = db_conn.cursor()

    cur.execute("SELECT curr_port, host FROM coordinators WHERE curr_port != %s AND last_seen > %s", (curr_port, time.time() - 15))
    rows = cur.fetchall()
    db_conn.close()

    found = {}
    for row in rows:
        found[row[0]] = row[1]

    return found

def register_self():
    db_conn = None

    while True:
        time.sleep(5)

        try:
            if db_conn is None:
                db_conn = psycopg2.connect(dbname="djs", user="kais", host=db_host, port=5432)
                cur = db_conn.cursor()

            if db_conn is not None:
                cur.execute("UPDATE coordinators SET last_seen = %s WHERE curr_port = %s", (time.time(), curr_port))
                db_conn.commit()
                refresh_peers()

        except psycopg2.Error as e:
            db_conn = None
            print("[RECONNECTING] to database.")

def monitor_stuck_jobs():

    while True:
        time.sleep(5)

        if leader_port != curr_port:
            continue

        print("[CHECKING] for stuck Jobs.")

        broken = False
        db_conn = None

        try:
            db_conn = db_pool.getconn()
            cur = db_conn.cursor()
            cur.execute("SELECT * FROM tasks WHERE status = 'running'")
            rows = cur.fetchall()

            for row in rows:
                job_id = row[0]
                claimed_time = row[3] or 0 #Safety measure for if it returns NULL.
                elapsed = time.time() - claimed_time

                if elapsed > 10:
                    cur.execute("UPDATE tasks SET status = %s WHERE job_id = %s", ("pending", job_id))
                    db_conn.commit()
                    print(f"[RECLAIMED] Job: {job_id}.")

        except psycopg2.Error as e:
            print(f"[RECONNECTING] Pool connection was broken: {e}")
            broken = True

        finally:
            if db_conn is not None:
                db_pool.putconn(db_conn, close=broken)

def next_term():
    db_conn = psycopg2.connect(dbname="djs", user="kais", host=db_host, port=5432)

    try:
        cur = db_conn.cursor()
        cur.execute("UPDATE election SET term = term + 1 WHERE id = 1 RETURNING term")
        new_term = cur.fetchone()[0] #Return the value inside the tuple.
        db_conn.commit()
        return new_term
        
    finally:
        db_conn.close()

def current_term():
    db_conn = psycopg2.connect(dbname="djs", user="kais", host=db_host, port=5432)

    try:
        cur = db_conn.cursor()
        cur.execute("SELECT term FROM election WHERE id = 1")
        return cur.fetchone()[0]
    
    finally:
        db_conn.close()

def refresh_peers():
    global other_ports, all_ports, last_hb_recv

    discovered = discover_coords()

    for port, host in discovered.items(): #.items() returns dictionary's key-value pairs.
        peer_hosts[port] = host

        if port not in other_ports:
            other_ports.append(port)
            last_hb_recv.setdefault(port, time.time())
            print(f"[DISCOVERED] New peer on port {port}")

    all_ports = other_ports + [curr_port]

def startup():
    global leader_port, term, other_ports, all_ports, last_hb_recv, peer_hosts

    register_once()
    term = current_term()

    print("[SETTLING] Discovering peers before deciding leadership.")
    time.sleep(10)

    discovered = discover_coords()
    peer_hosts.update(discovered) #Merge dictionaries
    other_ports = list(discovered.keys())
    all_ports = other_ports + [curr_port]
    last_hb_recv = {port: time.time() for port in other_ports}
    leader_port = None

    ask_who_is_leader()

    if leader_port is None:
        promote_new_leader()

    print(f"[SETTLED] Known peers: {other_ports}. Leader: {leader_port}")

startup()

db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=25,
    dbname="djs",
    user="kais",
    host=db_host,
    port=5432
)

thread = threading.Thread(target=listen_port)
thread.start()

heartbeat_thread = threading.Thread(target=check_status)
heartbeat_thread.start()

check_dead_thread = threading.Thread(target=check_dead_leader)
check_dead_thread.start()

worker_listener_thread = threading.Thread(target=listen_for_workers)
worker_listener_thread.start()

register_thread = threading.Thread(target=register_self)
register_thread.start()

stuck_job_thread = threading.Thread(target=monitor_stuck_jobs)
stuck_job_thread.start()

while True:

    time.sleep(2)
    print("...")
    