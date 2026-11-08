# 10 - Async processing

Calling external systems (REST APIs, databases...) from a topology with `MapAsync`, `MapValuesAsync`, `FlatMapAsync`, `FlatMapValuesAsync` and `ForeachAsync`, with a retry policy.

```bash
cd examples/10-AsyncProcessing/src
dotnet run -- <example>
```

The external services are simulated by default. Set `CUSTOMER_API_URL` to call a real customer API (`GET {url}/customers/{id}` returning `{"name": "..."}`) in `enrich-with-http` and `retry-to-dead-letter-queue`. The tests use fakes.

Docs: [async processing](../../docs/async-processing.md).

## Retry policy

```csharp
RetryPolicy.NewBuilder()
    .RetriableException<HttpRequestException>() // only these exceptions are retried (none by default)
    .NumberOfRetry(3)                          // retries after the first call (default 5, 0 = no retry)
    .RetryBackOffMs(200)                       // wait between attempts (default 100 ms)
    .TimeoutMs(10_000)                         // total time for one record (default max.poll.interval.ms / 3)
    .RetryBehavior(EndRetryBehavior.SKIP)      // when the attempts are exhausted (default SKIP)
    .MaxConcurrency(16)                        // V2 only: calls in flight per processor and task (default 16)
    .Build()
```

When the attempts are exhausted:
- `SKIP`: the record is dropped (counted in the dropped-records metric) and sent to the dead letter queue if `DeadLetterQueueTopicName` is set.
- `FAIL`: in V1 the external thread stops; in V2 the error goes through the `ProcessingExceptionHandler`, then the `InnerExceptionHandler`.
- `BUFFERED`: the record is retried later. V2 retries it until it succeeds, holding back the following records of the same key.

## V1 and V2

`StreamConfig.AsyncProcessingMode` chooses the engine:

| | V1 (default) | V2 |
| --- | --- | --- |
| Where calls run | one external thread per instance | inside the stream task |
| Internal topics | a `-request` and a `-response` topic per operation | only the `-response` topic of `MapAsync` / `FlatMapAsync` |
| Concurrency | one call at a time | up to `MaxConcurrency` keys at a time |
| Order | arrival order | per key (a key's records run one after the other) |
| Offsets | committed by the external thread | never past a record whose call isn't finished |

Switching an existing application from V1 to V2: records still waiting in the V1 request topics aren't processed by V2. Drain them first.

## enrich-with-http

`MapValuesAsync`: adds the customer name to each order, retrying HTTP errors and timeouts.

| Input `orders` (order id → customer id) | Output `orders-enriched` |
| --- | --- |
| `order-1` → `c-1` | `order-1` → `c-1:Alice` |
| `order-2` → `c-2` (first call: 503) | `order-2` → `c-2:Bob` (second attempt) |

## rekey-async

`MapAsync`: replaces the key (an e-mail address) by the account id returned by a lookup service.

`signups`: `alice@example.com` → `pro` → `signups-by-account`: `acc-42` → `pro`.

## audit-sink

`ForeachAsync`: a terminal operation writing every payment to an external audit log, retrying when the log times out.

## retry-to-dead-letter-queue

V2, `SKIP` and `DeadLetterQueueTopicName`: a record whose call keeps failing is written to `orders-dead-letter`, with its original key, value and headers, plus `__streams.errors.*` headers describing the error.

| Input `orders-to-enrich` | Output |
| --- | --- |
| `order-1` → `c-1` | `orders-enriched-or-dlq`: `order-1` → `c-1:Alice` |
| `order-2` → `c-broken` (always fails) | `orders-dead-letter`: `order-2` → `c-broken`, header `__streams.errors.exception` = `...NoneRetryableException` |

## parallel-v2

V2 with `MaxConcurrency(16)`: payments of different accounts are scored in parallel by a slow service, while the payments of one account are scored in order. The test sends 12 payments for 4 accounts and checks that several calls ran at once and that each account's results keep their order.
