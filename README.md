# beyond-queue

Drop-in SQS replacement backed by PostgreSQL. Runs inside your network; no external dependencies.

Accepts both SQS wire protocols (JSON and Query/form-encoded) and a native REST API. Your existing SQS SDK works unchanged — point it at this instead.

Built on [pgmq](https://github.com/tembo-io/pgmq). The schema, table layout, and core SQL are pgmq's. We replaced the hot paths with a pgrx Rust extension: direct heap inserts, WaitLatch-based long-polling, and push notification on commit via `XactCallback`. The REST API and SQS protocol layers are new.

## Quick Start

```sh
docker run -e DATABASE_URL=postgres://... -p 4566:4566 beyond/queue
```

Point your SQS client at `http://localhost:4566`:

```python
import boto3
sqs = boto3.client("sqs", endpoint_url="http://localhost:4566")
q = sqs.create_queue(QueueName="jobs")["QueueUrl"]
sqs.send_message(QueueUrl=q, MessageBody='{"job": "resize"}')
msgs = sqs.receive_message(QueueUrl=q, WaitTimeSeconds=5)
```

Or use the REST API directly:

```sh
curl -X POST http://localhost:4566/v1/queues -d '{"name":"jobs"}'
curl -X POST http://localhost:4566/v1/queues/jobs/messages -d '{"message":{"job":"resize"}}'
curl "http://localhost:4566/v1/queues/jobs/messages?wait=5&vt=30"
```

## What it does

- **Standard queues** — send, receive with visibility timeout, delete, batch operations
- **FIFO queues** — per-group ordering, group locking, deduplication
- **Long polling** — `?wait=N` blocks up to N seconds; woken immediately when a message arrives (no busy polling)
- **Async commit** — opt out of WAL fsync per-send for higher throughput when durability can be relaxed
- **SQS compatibility** — CreateQueue, SendMessage, ReceiveMessage, DeleteMessage, ChangeMessageVisibility, and more, in both JSON and Query protocols

## Benchmarks

Comparison against pgmq (PL/pgSQL, the upstream baseline). Both run on PostgreSQL 18, synchronous commit, same host. Queues are pre-filled; receive measures sustained read throughput.

**Hardware:** AMD Ryzen 7 255 (16 threads), 27 GB RAM, NVMe SSD, Linux 6.12.

| Scenario            | pgmq msgs/s | ours msgs/s |    Δ | pgmq p99 | ours p99 | Δ p99 |
| ------------------- | ----------: | ----------: | ---: | -------: | -------: | ----: |
| send c=1            |       2,245 |       2,495 | +11% |   807 µs |   738 µs |   -9% |
| send c=8            |      12,633 |      13,850 | +10% | 1,054 µs | 1,187 µs |  +13% |
| send c=32           |      29,537 |      33,389 | +13% | 5,811 µs | 3,417 µs |  -41% |
| send c=1 b=100      |      68,346 |      72,173 |  +6% | 2,125 µs | 2,209 µs |   +4% |
| send c=8 b=100      |     355,318 |     364,956 |  +3% | 2,685 µs | 3,123 µs |  +16% |
| receive c=1         |       8,218 |       9,806 | +19% |   212 µs |   197 µs |   -7% |
| receive c=8         |      47,849 |      69,799 | +46% |   298 µs |   184 µs |  -38% |
| receive-sharded c=8 |      78,725 |      83,096 |  +6% |   176 µs |   153 µs |  -13% |
| round-trip c=1      |         870 |         945 |  +9% | 1,592 µs | 1,533 µs |   -4% |
| round-trip c=8      |       5,938 |       6,400 |  +8% | 1,827 µs | 1,713 µs |   -6% |

`c=N` = concurrent workers. `b=100` = batch size. Round-trip = send + receive + delete.

```sh
mise run bench        # quick profile
mise run bench full   # full profile
```

## Development

```sh
mise tasks                        # list all tasks
mise run build                    # cargo build
mise run test                     # integration tests (Docker required)
mise run extension:build:linux    # cross-compile the pgrx .so (Docker required)
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design.
