import psycopg2
import time

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

cur.execute("""
    CREATE TABLE IF NOT EXISTS coordinators (
        curr_port INTEGER PRIMARY KEY,
        worker_port INTEGER,
        last_seen DOUBLE PRECISION
    )
""")

cur.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_time DOUBLE PRECISION DEFAULT extract(epoch from now())")
cur.execute("UPDATE tasks SET created_time = %s WHERE created_time IS NULL", (time.time(),))

# cur.execute("""
#     INSERT INTO tasks (job_id, status, attempts, created_time) VALUES
#   ('zeta', 'pending', 0, extract(epoch from now())),
#   ('alpha', 'pending', 0, extract(epoch from now()) + 10),
#   ('beta', 'pending', 0, extract(epoch from now()) + 20);
# """)

cur.execute("""
    CREATE TABLE IF NOT EXISTS election (
        id INTEGER PRIMARY KEY,
        term INTEGER NOT NULL DEFAULT 0
    )
""")
cur.execute("INSERT INTO election (id, term) VALUES (1, 0) ON CONFLICT (id) DO NOTHING")

conn.commit()

print("[Tables created successfully.]")

# cur.execute(
#     "INSERT INTO tasks (job_id, status, attempts, claimed_time) VALUES (%s, %s, %s, %s)", 
#     ("job-1", "pending", "0", None)
# )

# conn.commit()
# print("[VALUES INSERTED]")

# cur.execute("SELECT * FROM tasks WHERE status = 'pending' LIMIT 1")
# result = cur.fetchone()

# if result is None:
#     print("[No pending jobs at the moment.]")
# else:
#     job_id = result[0]
#     cur.execute("UPDATE tasks SET status = %s WHERE job_id = %s", ("running", job_id))
#     conn.commit()

conn.close()