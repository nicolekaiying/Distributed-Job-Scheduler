import psycopg2

conn = psycopg2.connect(
    dbname="djs",
    user="kais",
    host="localhost",
    port=5432
)

print("[Successfully connected to database.]")

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        job_id TEXT PRIMARY KEY,
        status TEXT,
        attempts INTEGER, 
        claimed_time DOUBLE PRECISION
    )
""")

conn.commit()
print("[Table created successfully.]")

# cur.execute(
#     "INSERT INTO tasks (job_id, status, attempts, claimed_time) VALUES (%s, %s, %s, %s)", 
#     ("job-1", "pending", "0", None)
# )

# conn.commit()
# print("[VALUES INSERTED]")

cur.execute("SELECT * FROM tasks WHERE status = 'pending' LIMIT 1")
result = cur.fetchone()

if result is None:
    print("[No pending jobs at the moment.]")
else:
    job_id = result[0]
    cur.execute("UPDATE tasks SET status = %s WHERE job_id = %s", ("running", job_id))
    conn.commit()

conn.close()