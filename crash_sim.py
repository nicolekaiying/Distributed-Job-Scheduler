import socket
import json

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("localhost", 5001))
client.send(b"give me a job.")
data = client.recv(1024)
text = data.decode()
job_schd = json.loads(text)
print("Got job, then dying:", job_schd)