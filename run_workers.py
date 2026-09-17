import subprocess


for x in range(20):
    subprocess.Popen(["python3", "worker.py"])