using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.MongoDb.RemoteStorage.Internal
{
    /// <summary>
    /// <see cref="IMongoDbOperations"/> on a MongoDB collection. Every query and the index use the simple collation,
    /// so the hex keys compare as binary strings even if the collection has a default collation.
    /// </summary>
    internal class MongoDbOperations : IMongoDbOperations
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(MongoDbOperations));

        // MongoDB error codes when an index with the same name or keys already exists with other options
        private const int INDEX_OPTIONS_CONFLICT = 85;
        private const int INDEX_KEY_SPECS_CONFLICT = 86;

        private static readonly FilterDefinitionBuilder<BsonDocument> filter = Builders<BsonDocument>.Filter;
        private static readonly SortDefinitionBuilder<BsonDocument> sort = Builders<BsonDocument>.Sort;

        private readonly MongoClient client;
        private readonly IMongoCollection<BsonDocument> collection;

        public MongoDbOperations(MongoDbRemoteStorageOptions options, string collectionName)
        {
            client = new MongoClient(options.ConnectionString);
            collection = client
                .GetDatabase(options.DatabaseName)
                .GetCollection<BsonDocument>(collectionName);
        }

        public void EnsureIndex()
        {
            var keys = Builders<BsonDocument>.IndexKeys
                .Ascending(MongoDbKeys.TASK_FIELD)
                .Ascending(MongoDbKeys.KEY_FIELD);
            var indexOptions = new CreateIndexOptions
            {
                Name = MongoDbKeys.INDEX_NAME,
                Unique = true,
                Collation = Collation.Simple
            };

            try
            {
                collection.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(keys, indexOptions));
            }
            catch (MongoCommandException e) when (e.Code == INDEX_OPTIONS_CONFLICT || e.Code == INDEX_KEY_SPECS_CONFLICT)
            {
                log.LogWarning(
                    "The index {Index} of the collection {Collection} already exists with other options, it is kept : {Message}",
                    MongoDbKeys.INDEX_NAME, collection.CollectionNamespace.FullName, e.Message);
            }
        }

        public byte[] Get(string task, string hexKey)
        {
            var document = collection
                .Find(ById(task, hexKey), new FindOptions {Collation = Collation.Simple})
                .Project(Builders<BsonDocument>.Projection.Include(MongoDbKeys.VALUE_FIELD))
                .FirstOrDefault();
            return Value(document);
        }

        public void Put(MongoDbWrite write)
        {
            if (write.Value == null)
                collection.DeleteOne(ById(write.Task, write.HexKey), new DeleteOptions {Collation = Collation.Simple});
            else
                collection.ReplaceOne(ById(write.Task, write.HexKey), Document(write),
                    new ReplaceOptions {IsUpsert = true, Collation = Collation.Simple});
        }

        public byte[] Delete(string task, string hexKey)
            => Value(collection.FindOneAndDelete(ById(task, hexKey),
                new FindOneAndDeleteOptions<BsonDocument> {Collation = Collation.Simple}));

        public void BulkWrite(IReadOnlyList<MongoDbWrite> writes)
        {
            if (writes.Count == 0)
                return;

            var models = writes.Select(write => write.Value == null
                ? (WriteModel<BsonDocument>) new DeleteOneModel<BsonDocument>(ById(write.Task, write.HexKey))
                    {Collation = Collation.Simple}
                : new ReplaceOneModel<BsonDocument>(ById(write.Task, write.HexKey), Document(write))
                    {IsUpsert = true, Collation = Collation.Simple});

            // ordered : the writes of a key are applied in order, so the last one wins
            collection.BulkWrite(models, new BulkWriteOptions {IsOrdered = true});
        }

        public IEnumerable<KeyValuePair<string, byte[]>> Find(string task, string fromHex, string toHex, bool descending)
        {
            var query = filter.Eq(MongoDbKeys.TASK_FIELD, task);
            if (fromHex != null)
                query &= filter.Gte(MongoDbKeys.KEY_FIELD, fromHex);
            if (toHex != null)
                query &= filter.Lte(MongoDbKeys.KEY_FIELD, toHex);

            var options = new FindOptions<BsonDocument>
            {
                Collation = Collation.Simple,
                Sort = descending ? sort.Descending(MongoDbKeys.KEY_FIELD) : sort.Ascending(MongoDbKeys.KEY_FIELD)
            };

            using var cursor = collection.FindSync(query, options);
            while (cursor.MoveNext())
            {
                foreach (var document in cursor.Current)
                    yield return new KeyValuePair<string, byte[]>(
                        document[MongoDbKeys.KEY_FIELD].AsString, Value(document));
            }
        }

        public long Count(string task)
            => collection.CountDocuments(filter.Eq(MongoDbKeys.TASK_FIELD, task),
                new CountOptions {Collation = Collation.Simple});

        public void Dispose() => client.Dispose();

        private static FilterDefinition<BsonDocument> ById(string task, string hexKey)
            => filter.Eq(MongoDbKeys.ID_FIELD, MongoDbKeys.Id(task, hexKey));

        private static BsonDocument Document(MongoDbWrite write)
            => new()
            {
                {MongoDbKeys.ID_FIELD, MongoDbKeys.Id(write.Task, write.HexKey)},
                {MongoDbKeys.TASK_FIELD, write.Task},
                {MongoDbKeys.KEY_FIELD, write.HexKey},
                {MongoDbKeys.VALUE_FIELD, new BsonBinaryData(write.Value)}
            };

        private static byte[] Value(BsonDocument document)
            => document != null && document.TryGetValue(MongoDbKeys.VALUE_FIELD, out var value) && value.IsBsonBinaryData
                ? value.AsByteArray
                : null;
    }
}
