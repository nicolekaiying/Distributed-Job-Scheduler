import socket
import json
import threading
import time
import psycopg2
from dis import roll_decide

max_attempts = 3

def handle_client(conn):

    db_conn = psycopg2.connect(
    dbname="djs",
    user="kais",
    host="localhost",
    port=5432
    )
    cur = db_conn.cursor()

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

def monitor_stuck_jobs():

    db_conn = psycopg2.connect(
        dbname="djs",
        user="kais",
        host="localhost",
        port=5432
    )
    cur = db_conn.cursor()

    while True:
        time.sleep(5)
        print("[Checking for stuck jobs.]")

        cur.execute("SELECT * from tasks WHERE status = 'running'")
        rows = cur.fetchall()

        for row in rows:
            job_id = row[0]
            job_claim_time = row[3]
            elapsed_time = time.time() - job_claim_time

            if elapsed_time > 10:
                cur.execute("UPDATE tasks SET status = %s WHERE job_id = %s", ("pending", job_id))
                db_conn.commit()
                print(f"{job_id} reclaimed.")

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
