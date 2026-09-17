import socket
import json
import time
import os
from job_sim import roll_decide

targets = os.environ.get("WORKER_TARGETS", "localhost:5001,localhost:5002,localhost:5003")
worker_targets = [(h, int(p)) for h, p in (t.split(":") for t in targets.split(","))]

while True:

    job_schd = None

    for host, port in worker_targets:
        try: 
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((host, port))
            client.send(b"Give me a job...")
            data = client.recv(1024)
            job_schd = json.loads(data.decode())

            if "error" in job_schd and job_schd["error"] == "not_leader":
                job_schd = None
                continue

            break
        except OSError:
            continue

    if job_schd is None:
        print("Could not reach any coordinators or find a leader. Retrying shortly.")
        time.sleep(3)
        continue


    if job_schd["job"] is None:
        print("No job at the moment.")
        time.sleep(2)
        continue
    else:
        curr_job = job_schd["job"]
        success = roll_decide()
        curr_job["attempts"]+=1

        if success: 
            curr_job["status"] = "success" 
        else:
            curr_job["status"] = "failed"

        client.send(json.dumps(curr_job).encode())

    time.sleep(0.05)
