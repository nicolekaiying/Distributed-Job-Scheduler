import socket
import json
import os
import threading
import time
import psycopg2
from dis import roll_decide

max_attempts = 3

tasks = [
    {"job_id": "job-1", "status": "pending", "attempts": 0},
    {"job_id": "job-2", "status": "pending", "attempts": 0},
    {"job_id": "job-3", "status": "pending", "attempts": 0},
]

conn = psycopg2.connect(
    dbname="djs",
    user="kais",
    host="localhost",
    port=5432
)

cur = conn.cursor()

def handle_client(conn):
    one_job = None

    cur.execute("SELECT * FROM tasks WHERE status = 'pending' LIMIT 1")
    result = cur.fetchone()

    if result is None:
        conn.send(json.dumps({"job": None}).encode())
    else:
        queue_job = {
                "job_id": result[0],
                "status": result[1],
                "attempts": result[2],
                "claimed_time": result[3]
            }

        cur.execute("UPDATE tasks SET status = %s WHERE job_id = %s", ('running', queue_job["job_id"]))
        conn.commit()
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

    
    data = conn.recv(1024)
    print(data)

    if one_job is None: 
        conn.send(json.dumps({"job": None}).encode())
    else:
        conn.send(json.dumps({"job": one_job}).encode())
        result_data = conn.recv(1024)
        result_text = result_data.decode()
        result = json.loads(result_text)
        print(result)

        with job_lock:
            for queue in loaded_tasks:
                if queue["job_id"] == result["job_id"]:
                    queue["attempts"] = result["attempts"]
                    if result["status"] == "success":
                        queue["status"] = "success"
                    elif result["attempts"] < max_attempts:
                        queue["status"] = "pending"
                    else:
                        queue["status"] = "dead"

            with open("tasks.json", "w") as f:
                json.dump(loaded_tasks, f, indent=2) #indent=2 just makes the json file easier to read.

def monitor_stuck_jobs():
    while True:
        time.sleep(5)
        print("[CHECKING FOR STUCK JOBS]")
        with job_lock: 
            for queue in loaded_tasks:
                if queue["status"] == "running":
                    elapsed_time = time.time() - queue["claimed_time"]
                    print(f"[{queue['job_id']} HAS BEEN ELAPSING FOR {elapsed_time} SECONDS]")
                    if elapsed_time > 10:
                        queue["status"] = "pending"
                        with open("tasks.json", "w") as f: 
                            json.dump(loaded_tasks, f, indent=2)
                        print(f"Reclaimed stale job: {queue['job_id']}") #explicit prints to ensure heartbeat is working.

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("localhost", 5001))
server.listen()

job_lock = threading.Lock()

stuck_check = threading.Thread(target=monitor_stuck_jobs)
stuck_check.start()

while True:

    conn, addr = server.accept()
    thread = threading.Thread(target=handle_client, args=(conn,))
    thread.start()
