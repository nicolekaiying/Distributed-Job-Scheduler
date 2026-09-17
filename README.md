# DISTRIBUTED JOB SCHEDULER

Three coordinator processes run a leader election between themselves. The
elected leader dispatches jobs from a shared PostgreSQL queue to any number of
workers. Kill the leader mid-run and the survivors elect a new one, orphaned
jobs get reclaimed, and the workers carry on without being restarted.

Built on plain TCP sockets and Postgres. No message broker, no consensus
library. Terms defined in [GLOSSARY.md](GLOSSARY.md).

Each coordinator listens on two ports, one for peers and one for workers, so a
slow job request can never delay a heartbeat. Only the leader dispatches jobs.
Followers reply `not_leader` and workers try the next one.

## LEADER ELECTION

Coordinators heartbeat every 5 seconds. A leader silent for 15 seconds is
presumed dead.

Every election runs under a term allocated atomically by
`UPDATE election SET term = term + 1 ... RETURNING term`, so two candidates can
never get the same number and a restarted node cannot reuse one it spent.
Messages carrying a lower term than the receiver's are discarded, which fences
out a stale leader coming back online.

A candidate must win votes from a quorum, meaning more than half of all known
coordinators. A leader that can no longer reach a quorum steps down. Together
those prevent split-brain: a minority can never become leader, and a leader that
falls into a minority does not stay one.

A starting coordinator asks its peers who the leader is before assuming
anything, so it rejoins as a follower rather than taking over.

## THE QUEUE

Claiming a job is one atomic statement:

```sql
WITH next_job AS (
    SELECT job_id FROM tasks
    WHERE status = 'pending'
    ORDER BY created_time ASC, job_id ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE tasks SET status = 'running', claimed_time = %s
WHERE job_id = (SELECT job_id FROM next_job)
RETURNING *
```

FIFO by creation time, tie-broken by `job_id`. `SKIP LOCKED` lets workers claim
different jobs concurrently instead of blocking on the same row. Because it is
one statement, two workers cannot take the same job.

A claim is a lease. Anything left `running` for more than 10 seconds is assumed
abandoned and returned to `pending`, so a crashed worker does not lose the job.
Failures retry three times, then move to `dead`.

Delivery is at-least-once, not exactly-once. A worker that finishes a job and
dies before reporting will have it run again, so jobs need to be idempotent.

## RUNNING IT

```bash
docker compose run --rm coord1 python3 database_setup.py   # first time only
docker compose up --build --scale worker=5
```

Or directly, with peer port first and worker port second:

```bash
python3 database_setup.py
python3 coordinator.py 6001 5001      # and 6002 5002, 6003 5003
python3 worker.py
```

Both run from the same code. Without the Docker environment variables
(`COORD_HOST`, `DB_HOST`, `WORKER_TARGETS`) everything defaults to `localhost`.

## RESULTS

`python3 bench.py 1000 20` against a running cluster:

- 1,000 jobs, 20 concurrent workers, nothing lost and nothing run twice,
  verified by reconciling claim counts against job counts.
- Around 1,600 jobs/sec over loopback. The payload is trivial, so this measures
  dispatch overhead, roughly 0.6ms per job.
- 16 of 100 jobs exhausted their retries, against 12.5% predicted for a 50%
  success rate and a three-attempt cap.

## DESIGN NOTES

- **Postgres instead of a broker.** RabbitMQ or Kafka would be faster and ship
  retries for free, but they would hide the mechanics I set out to build.
  Postgres gives atomic claiming, which is the hard part.
- **Terms from a shared counter, not per-node disk.** Raft persists terms
  locally, which stops a node reusing its own but still lets two candidates pick
  the same one, resolved by voting. One atomic counter makes the collision
  impossible instead. The cost is that elections need Postgres reachable.
- **Membership grows but never shrinks.** A silent coordinator still counts.
  Removing it would shrink the quorum denominator, and a smaller denominator is
  easier to claim, so two partitioned groups could each think they were the
  majority.
- **Three coordinators, not four.** Both survive exactly one failure, but four
  gives you an extra machine that can break. Useful sizes are odd.

## LIMITATIONS

- **Membership is discovered, not configured**, so a coordinator restarting while
  partitioned finds no peers, decides it is a new single-node cluster, and
  elects itself. Raft requires an explicit member list for this reason.
- **A long job holds a pooled database connection** while the coordinator waits
  for the result, so `maxconn` is the real ceiling on jobs in flight.
- **Workers find the leader by trial and error** rather than asking once and
  going straight there.
- **Failure detection is timing-based.** A crashed node and a slow node look the
  same from outside.
- **Testing is manual.** No automated suite.

