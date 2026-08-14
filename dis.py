import random

tasks = [
    {"job_id": "job-1", "status": "pending", "attempts": 0},
    {"job_id": "job-2", "status": "pending", "attempts": 0},
    {"job_id": "job-3", "status": "pending", "attempts": 0},
]

max_attempt = 3

def roll_decide():
    roll = random.random()
    return roll >= 0.5

def decide(queue, max_attempts):
    queue["status"] = "running"
    print(queue)
    success = roll_decide()
    queue["attempts"] = queue["attempts"] + 1
    if success:
        queue["status"] = "success"
        print(queue)
    else:
        if queue["attempts"] < max_attempts:
            queue["status"] = "failed"
            print(queue)
            queue["status"] = "pending"
        else:
            queue["status"] = "dead"
            print(queue)

def run_tasks(tasks:list, max_attempt:int):

    while True:

        for queue in tasks:
            if queue["status"] == "pending":
                decide(queue, max_attempt)
        
        if all(queue["status"] in ("success", "dead") for queue in tasks):
            break