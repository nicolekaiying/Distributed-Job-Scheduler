import socket
import json
import time
import random
from dis import roll_decide

worker_ports = [5001, 5002, 5003]

while True:

    job_schd = None

    for port in worker_ports:
        try: 
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(('localhost', port))
            client.send(b"Give me a job...")
            data = client.recv(1024)
            job_schd = json.loads(data.decode())

            if "error" in job_schd and job_schd["error"] == "not_leader":
                job_schd = None
                continue

            break
        except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError):
            continue

    if job_schd is None:
        print("Could not reach any coordinators or find a leader. Retrying shortly.")
        time.sleep(3)
        continue


    if job_schd["job"] is None:
        print("No job at the moment.")
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
