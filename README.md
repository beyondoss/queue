# beyond/queue

Drop-in SQS + SNS replacement backed by PostgreSQL. Runs inside your network; no external dependencies.

Accepts both SQS and SNS wire protocols (JSON and Query/form-encoded) and a native REST API. Your existing SQS/SNS SDK works unchanged — point it at this instead.

Built on [pgmq](https://github.com/tembo-io/pgmq). The schema, table layout, and core SQL are pgmq's. We replaced the hot paths with a pgrx Rust extension: direct heap inserts, WaitLatch-based long-polling, and push notification on commit via `XactCallback`. The REST API, SQS protocol layer, SNS protocol layer, and HTTP delivery worker are new.

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

Fan out to queues and HTTP webhooks via topics:

```sh
# Bind a queue to a pattern (SQS fan-out)
curl -X POST http://localhost:4566/v1/topics/orders.*/subscriptions \
  -d '{"queue_name":"jobs"}'

# Bind an HTTP endpoint (webhook delivery, raw payload by default)
curl -X POST http://localhost:4566/v1/topics/orders.*/subscriptions \
  -d '{"protocol":"https","endpoint":"https://example.com/hooks/orders"}'

# Publish — both subscribers receive the message
curl -X POST http://localhost:4566/v1/topics/orders.placed \
  -d '{"message":{"order_id":42}}'
```

Or use the SNS wire protocol with your existing SDK:

```python
import boto3
sns = boto3.client("sns", endpoint_url="http://localhost:4566")
topic = sns.create_topic(Name="orders.*")["TopicArn"]
sns.subscribe(TopicArn=topic, Protocol="https", Endpoint="https://example.com/hooks/orders")
sns.publish(TopicArn="arn:aws:sns:us-east-1:000000000000:orders.placed",
            Message='{"order_id": 42}')
```

## What it does

- **Standard queues** — send, receive with visibility timeout, delete, batch operations
- **FIFO queues** — per-group ordering, group locking, deduplication
- **Long polling** — `?wait=N` blocks up to N seconds; woken immediately when a message arrives (no busy polling)
- **Async commit** — opt out of WAL fsync per-send for higher throughput when durability can be relaxed
- **Topic fan-out** — publish to a routing key, fan out to any number of bound queues or HTTP endpoints; wildcard patterns (`orders.*`, `events.#`)
- **HTTP/HTTPS webhook delivery** — push to subscriber endpoints with automatic retry and exponential backoff (10s → 30s → 60s → 5m); dead-lettered rows retained for inspection
- **SNS-compatible envelope** — outbound webhooks carry a signed SNS notification envelope (`Type`, `MessageId`, `TopicArn`, `Signature`, `SignatureVersion: 2`) for compatibility with SNS consumers; opt out per-subscription for raw payload delivery
- **SQS compatibility** — CreateQueue, SendMessage, ReceiveMessage, DeleteMessage, ChangeMessageVisibility, and more, in both JSON and Query protocols
- **SNS compatibility** — CreateTopic, Subscribe (`sqs`/`http`/`https`), Publish, ListSubscriptions, Unsubscribe, GetSubscriptionAttributes, in both JSON and Query protocols

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

Topic fanout routes a single send to every queue whose binding pattern matches the routing key. The pgmq baseline uses a PL/pgSQL loop calling `queue.send()` once per bound queue. Our implementation uses a shared-memory routing cache (invalidated by trigger on `topic_subscriptions` writes) so the regex scan is skipped on every call after the first, plus datum passthrough to avoid per-queue JSON re-serialization.

`n=N` = number of bound queues the routing key matches.

| Scenario                  | pgmq msgs/s | ours msgs/s |     Δ |   pgmq p99 |  ours p99 | Δ p99 |
| ------------------------- | ----------: | ----------: | ----: | ---------: | --------: | ----: |
| send-topic n=1 c=1        |       1,529 |       1,676 |  +10% |   1,189 µs |    970 µs |  -18% |
| send-topic n=4 c=1        |       1,242 |       1,445 |  +16% |   1,309 µs |  1,173 µs |  -10% |
| send-topic n=16 c=1       |         760 |         937 |  +23% |   2,319 µs |  1,881 µs |  -19% |
| send-topic n=4 c=8        |       5,836 |       7,360 |  +26% |   2,245 µs |  1,834 µs |  -18% |
| send-topic n=16 c=8       |       1,839 |       7,255 | +295% |  21,375 µs |  1,888 µs |  -91% |
| send-topic b=100 n=4 c=1  |      12,365 |      33,677 | +172% |  31,983 µs | 15,039 µs |  -53% |
| send-topic b=100 n=16 c=1 |       5,082 |      15,133 | +198% | 151,167 µs | 21,375 µs |  -86% |
| send-topic b=100 n=4 c=8  |      14,975 |      58,105 | +288% | 175,231 µs | 26,751 µs |  -85% |

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
