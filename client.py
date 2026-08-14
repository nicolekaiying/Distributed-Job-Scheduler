import socket
import json
import time
import random
from dis import roll_decide

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("localhost", 5001))
time.sleep(random.uniform(0.5, 1.5))
client.send(b"give me a job.")
data = client.recv(1024)
text = data.decode()              
job_schd = json.loads(text)            

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
