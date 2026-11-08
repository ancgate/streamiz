# 09 - Serdes

How records are turned into bytes and back. Every topic read or written, and every state store, needs a key serdes and a value serdes:
- the defaults of the configuration (`StreamConfig<KeySerDes, ValueSerDes>`, or `DefaultKeySerDes` / `DefaultValueSerDes`);
- or serdes given per operation, as type parameters (`Stream<K, V, KS, VS>`, `To<KS, VS>`, `InMemory.As<K, V, KS, VS>`) or as instances (`Stream(topic, keySerdes, valueSerdes)`).

```bash
cd examples/09-Serdes/src
dotnet run -- <example>
```

The Schema Registry examples need a Schema Registry: `SCHEMA_REGISTRY_URL`, default `http://localhost:8081`. The tests use a `mock://scope` url instead, which makes the serdes use an in-memory registry.

Docs: [stateless processors](../../docs/stateless-processors.md), [stream configuration](../../docs/stream-configuration.md).

## built-in-serdes

Serdes given per operation when the types differ from the defaults. Reads `double` temperatures (`DoubleSerDes`), keeps the hot ones, and writes them rounded as `int` (`Int32SerDes`).

| Input `temperatures` (string → double) | Output `heat-alerts` (string → int) |
| --- | --- |
| `sensor-1` → `21.4` | |
| `sensor-2` → `31.6` | `sensor-2` → `32` |
| `sensor-1` → `30.0` | `sensor-1` → `30` |

Built-in serdes: `StringSerDes`, `Int32SerDes`, `Int64SerDes`, `NullableInt64SerDes`, `DoubleSerDes`, `FloatSerDes`, `CharSerDes`, `BytesSerDes`, `ByteArraySerDes`, `JsonSerDes<T>` (Newtonsoft), `TimeWindowedSerDes<K>`, `SessionWindowedSerDes<K>`.

## nullable-serdes

`NullableInt64SerDes`: a `long?` value where `null` is a real Kafka null (a tombstone). `Int64SerDes` can't represent null. Splits stock levels into restock requests (20 minus the units left) and discontinued products (null).

| Input `stock-levels` (string → long?) | Output |
| --- | --- |
| `sku-1` → `3` | `restock-requests`: `sku-1` → `17` |
| `sku-2` → `50` | |
| `sku-3` → `null` | `discontinued-products`: `sku-3` → *(tombstone)* |

## materialized-serdes

Default string serdes for the input, and `Int64SerDes` for the count store and the output, given through `Materialized` and `To<KS, VS>`.

| Input `page-views` (user → page) | Output `views-per-user` (user → long) |
| --- | --- |
| `alice` → `/home` | `alice` → `1` |
| `bob` → `/pricing` | `bob` → `1` |
| `alice` → `/docs` | `alice` → `2` |

## custom-json-serdes

A custom serdes: derive from `AbstractSerDes<T>` and implement `Serialize` and `Deserialize`. `SystemTextJsonSerDes<T>` writes camelCase JSON with System.Text.Json (Streamiz also ships a Newtonsoft-based `JsonSerDes<T>`). Keeps the orders of 100 or more.

`orders`: `o-2` → `{"id":"o-2","customer":"bob","amount":250,"currency":"USD"}` → `large-orders`: the same order.

## schema-registry-json

`SchemaJsonSerDes<T>` (package `Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json`): registers the JSON schema generated from the class and writes the Confluent wire format. Configure `SchemaRegistryUrl` and `AutoRegisterSchemas` on the stream configuration. Keeps the EUR orders: `orders-json` → `orders-json-eur`.

## schema-registry-avro

`SchemaAvroSerDes<T>` (package `Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro`) with a specific record class (`Model/Payment.cs`; normally generated from the `.avsc` with `avrogen`). Routes the `FAILED` payments: `payments-avro` → `payments-failed`.

## schema-registry-protobuf

`SchemaProtobufSerDes<T>` (package `Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf`). `Customer` is generated at build time from `Protos/customer.proto` by the `Grpc.Tools` package (see the csproj). Re-keys the VIP customers by country: `customers-proto` → `vip-customers-by-country`.

## cloud-events

`CloudEventSerDes<T>` (package `Streamiz.Kafka.Net.SerDes.CloudEvents`): in binary content mode the value is the JSON payload and the CloudEvents attributes are `ce_*` headers (`ce_id`, `ce_type`, `ce_source`...). The event id is taken from the order instead of a new Guid. Republishes the large orders: `orders-cloudevents` → `large-orders-cloudevents`.
