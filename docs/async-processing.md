# Asynchronous processing

Often, you want to create a stream processing application, process data from Kafka topic to another Kafka topic, but in the middle of your topology, you need to get data from another tier system (oracle db, mongo db, http rest api or whatever). 

Since `1.4.0` release, Streamiz release multiple asynchronous processors :
- MapAsync(...)
- MapValuesAsync(...)
- FlatMapAsync(...)
- FlatMapValuesAsync(...)
- ForEachAsync(...)

These processors use the pattern request/reponse to satisfy the asynchronous request with some retry policy options.

## Example

**Use case: Enrich data from a mongodb instance**

- Create a directory which names : `mongo-init` and a file `init.js` inside 

``` javascript
db = db.getSiblingDB('streamiz');
db.createCollection('adress');
db.adress.insert({ "address": { "city": "Paris", "zip": "123" }, "name": "Mike", "phone": "1234" });
db.adress.insert({ "address": { "city": "Marsel", "zip": "321" }, "name": "Helga", "phone": "4321" });
```

- Use this `docker-compose` file 

```yaml
---
version: '2'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.1.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
      KAFKA_OPTS: "-Dzookeeper.4lw.commands.whitelist=*"

  broker:
    image: confluentinc/cp-server:7.1.0
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_CONFLUENT_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: broker:29092
      CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 1
      CONFLUENT_METRICS_ENABLE: 'true'
      CONFLUENT_SUPPORT_CUSTOMER_ID: 'anonymous'

  schema-registry:
    image: confluentinc/cp-schema-registry:7.1.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'broker:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  akhq:
    image: tchiotludo/akhq:latest
    environment:
      AKHQ_CONFIGURATION: |
        akhq:
          server:
            access-log:
              enabled: false
          connections:
            docker-kafka-server:
              properties:
                bootstrap.servers: "broker:29092"
              schema-registry:
                type: "confluent"
                url: "http://schema-registry:8081"
    ports:
      - 8082:8080
    links:
      - broker

  mongo:
    image: 'mongo'
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: admin
    volumes:
      - ./mongo-init/:/docker-entrypoint-initdb.d/:ro
    ports:
      - "27017:27017"


```

- Create the input topic

```bash
docker-compose exec broker kafka-topics --bootstrap-server broker:29092 --topic input --create --partitions 4
```

- Create your streamiz topology

```csharp
    internal class Program
    {
        public class Address
        {
            public string city { get; set; }
            public string zip { get; set; }
        }

        public class Person
        {
            public ObjectId _id { get; set; }
            public Address address { get; set; }
            public string name { get; set; }
            public string phone { get; set; }
        }

        
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app2";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;

            StreamBuilder builder = new StreamBuilder();

            var client = new MongoClient(
                "mongodb://admin:admin@localhost:27017"
            );
            var database = client.GetDatabase("streamiz");

            builder
                .Stream<string, string>("input")
                .MapValuesAsync(async (record, _) => {
                    var persons = await database
                        .GetCollection<Person>("adress")
                        .FindAsync((p) => p.name.Equals(record.Key))
                        .Result.ToListAsync();
                    return persons.FirstOrDefault()?.address.city;
                })
                .To("person-city");
            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (o, e) => stream.Dispose();

            await stream.StartAsync();
        }
    }
```

- Subscribe output topic `person-city`

```bash
docker-compose exec broker kafka-console-consumer --bootstrap-server broker:29092 --topic person-city --property print.key=true --property --print.value=true --from-beginning --max-messages 1
```

- Open a new terminal and produce one message in the source topic

```bash
docker-compose exec broker kafka-console-producer --bootstrap-server broker:29092 --topic input --property parse.key=true --property key.separator=:

> Mike:Mike
```

- Assert the kafka console consumer output

```bash
> Mike    Paris
```

- Here the schema of this topology

![async-topology](./assets/async-topology.png)
## Async processing V2 (in-task)

Set `AsyncProcessingMode = AsyncProcessingMode.V2` (`async.processing.mode`, default `V1`) to run the asynchronous processors inside the stream tasks, like a parallel consumer. The DSL is the same (`MapAsync`, `MapValuesAsync`, `FlatMapAsync`, `FlatMapValuesAsync`, `ForeachAsync`, `RetryPolicy`, `ExternalContext`).

``` csharp
var config = new StreamConfig<StringSerDes, StringSerDes>();
config.ApplicationId = "async-app";
config.AsyncProcessingMode = AsyncProcessingMode.V2;

builder.Stream<string, string>("input")
    .MapValuesAsync(async (record, _) => await CallExternalServiceAsync(record.Value),
        RetryPolicy.NewBuilder()
            .MaxConcurrency(32)
            .RetriableException<HttpRequestException>()
            .NumberOfRetry(5)
            .RetryBackOffMs(100)
            .Build())
    .To("output");
```

**What changes compared to V1**

- There is no request topic and no external stream thread (and no `<application.id>-external` consumer group): the async processor is a processor of the stream task. The topology is rewritten when the `KafkaStream` (or the `TopologyTestDriver`) is created, so `Topology.Describe()` on the built topology still shows the V1 shape.
- The response topic of `MapAsync`, `MapValuesAsync`, `FlatMapAsync` and `FlatMapValuesAsync` is kept: it repartitions the results by their (possibly new) key, exactly as in V1, so the topology downstream of the async processor is the same in both modes. `RequestSerDes` is not used in V2.

**Execution model**

- The async processor starts the call on the thread pool and returns immediately. The calls run concurrently for different keys; the records with the same key are called strictly one after the other, in offset order.
- When a call completes, its result is queued; the stream thread forwards it downstream (with the record's original timestamp and headers) at its next iteration, so the downstream processors, state stores and the producer are only used by the stream thread. While calls are in progress, the stream thread polls Kafka with a short timeout (5 ms) to forward the results quickly.
- Back-pressure: `RetryPolicy.MaxConcurrency` (default 16) is the maximum number of records accepted by one async processor in one task and not completed yet (running, retrying or waiting behind a record with the same key). When it's reached, the task stops processing new records (they stay in the task's record queue) and the stream thread pauses the task's partitions; they are resumed when the async processors of the task are at most half full.

**Offsets (at-least-once)**

A record whose call is not completed is not committed: for each partition, the task commits up to the oldest record still in progress (excluded). The repartition records are purged with the same rule. A crash can process again the records completed after that offset.

When a task is revoked or the application is closed, the task waits for the calls in progress (at most half of `MaxPollIntervalMs`), forwards their results, then commits. When the partitions are lost, the calls in progress are cancelled and their results ignored (they are processed again by the new owner). With exactly-once, the results are produced by the stream thread inside the task's transaction and the committed offsets never pass a record in progress, so the guarantee is kept by design (this is not covered by an automated test yet: the test driver forces at-least-once).

**Retries and end of retries**

- The retries are asynchronous (`Task.Delay(RetryBackOffMs)`), for the exceptions of `RetriableException<T>()`. `TimeoutMs` (default `MaxPollIntervalMs / 3`) bounds the total time of the calls of one record (a call can't be cancelled: its late result is ignored).
- `EndRetryBehavior.SKIP`: the record is dropped (dropped-records metric) and sent to the dead letter queue if `DeadLetterQueueTopicName` is set.
- `EndRetryBehavior.FAIL`, or an exception which is not retriable: the error is raised on the stream thread when the completion is handled, through the `ProcessingExceptionHandler` (`CONTINUE` skips the record, `FAIL` raises it to the stream thread and the `InnerExceptionHandler`). In V1, the external thread stops instead.
- `EndRetryBehavior.BUFFERED`: the record is retried until it succeeds; the next records with the same key wait for it (`TimeoutMs` and `NumberOfRetry` don't apply). `MemoryBufferSize` is not used in V2.

**TopologyTestDriver**

In `SYNC_TASK` mode, `PipeInput` and `PipeInputs` wait (at most one minute) for the calls started by the piped records and forward their results before returning, so the outputs are available right away. `PipeInputs` processes all its records before waiting, so their calls run concurrently.
