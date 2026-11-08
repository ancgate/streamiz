using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Bigtable.Admin.V2;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using Grpc.Core;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Gcp.RemoteStorage.Internal
{
    /// <summary>
    /// <see cref="IBigtableOperations"/> on a real Bigtable instance (or emulator), through <see cref="BigtableClient"/>.
    /// </summary>
    internal class BigtableOperations : IBigtableOperations
    {
        internal static readonly BigtableByteString Qualifier = new("value");

        private readonly GcpRemoteStorageOptions options;
        private readonly TableName tableName;
        private readonly string family;
        private readonly BigtableClient client;
        private readonly RowFilter valueFilter;
        // service account key, or null for Application Default Credentials
        private readonly GoogleCredential credential;

        public BigtableOperations(GcpRemoteStorageOptions options)
        {
            this.options = options;
            family = options.ColumnFamily;
            tableName = new TableName(options.ProjectId, options.InstanceId, options.TableId);
            if (string.IsNullOrEmpty(options.EmulatorHost) && !string.IsNullOrEmpty(options.CredentialsPath))
                credential = CredentialFactory.FromFile<ServiceAccountCredential>(options.CredentialsPath).ToGoogleCredential();
            client = Configure(new BigtableClientBuilder()).Build();
            // only the latest cell of the value column
            valueFilter = RowFilters.Chain(
                RowFilters.FamilyNameExact(family),
                RowFilters.ColumnQualifierExact(Qualifier),
                RowFilters.CellsPerColumnLimit(1));
        }

        private BigtableClientBuilder Configure(BigtableClientBuilder builder)
        {
            if (!string.IsNullOrEmpty(options.EmulatorHost))
            {
                builder.Endpoint = options.EmulatorHost;
                builder.ChannelCredentials = ChannelCredentials.Insecure;
            }
            else
            {
                // BIGTABLE_EMULATOR_HOST if it is set, else the production endpoint
                builder.EmulatorDetection = EmulatorDetection.EmulatorOrProduction;
                if (credential != null)
                    builder.GoogleCredential = credential;
            }

            return builder;
        }

        private BigtableTableAdminClientBuilder Configure(BigtableTableAdminClientBuilder builder)
        {
            if (!string.IsNullOrEmpty(options.EmulatorHost))
            {
                builder.Endpoint = options.EmulatorHost;
                builder.ChannelCredentials = ChannelCredentials.Insecure;
            }
            else
            {
                builder.EmulatorDetection = EmulatorDetection.EmulatorOrProduction;
                if (credential != null)
                    builder.GoogleCredential = credential;
            }

            return builder;
        }

        public void EnsureTable()
        {
            var admin = Configure(new BigtableTableAdminClientBuilder()).Build();
            var columnFamily = new ColumnFamily {GcRule = new GcRule {MaxNumVersions = 1}};
            try
            {
                var table = admin.GetTable(tableName);
                if (!table.ColumnFamilies.ContainsKey(family))
                {
                    var request = new ModifyColumnFamiliesRequest {TableName = tableName};
                    request.Modifications.Add(new ModifyColumnFamiliesRequest.Types.Modification
                        {Id = family, Create = columnFamily});
                    admin.ModifyColumnFamilies(request);
                }
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {
                try
                {
                    var table = new Google.Cloud.Bigtable.Admin.V2.Table();
                    table.ColumnFamilies.Add(family, columnFamily);
                    admin.CreateTable(new InstanceName(options.ProjectId, options.InstanceId), options.TableId, table);
                }
                catch (RpcException e2) when (e2.StatusCode == StatusCode.AlreadyExists)
                {
                    // created at the same time by another task
                }
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists)
            {
                // column family created at the same time by another task
            }
        }

        public byte[] Read(byte[] rowKey)
            => ReadRows(RowSet.FromRowKey(new BigtableByteString(rowKey)), false, 1)
                .Select(kv => kv.Value)
                .FirstOrDefault();

        public void Write(byte[] rowKey, byte[] value)
        {
            var request = new MutateRowRequest
            {
                TableNameAsTableName = tableName,
                RowKey = ByteString.CopyFrom(rowKey),
                AppProfileId = options.AppProfileId ?? string.Empty
            };
            request.Mutations.AddRange(MutationsFor(value));
            client.MutateRow(request);
        }

        public void WriteAll(IReadOnlyList<KeyValuePair<byte[], byte[]>> rows)
        {
            var request = new MutateRowsRequest
            {
                TableNameAsTableName = tableName,
                AppProfileId = options.AppProfileId ?? string.Empty
            };
            foreach (var row in rows)
                request.Entries.Add(Mutations.CreateEntry(new BigtableByteString(row.Key), MutationsFor(row.Value)));

            var response = client.MutateRows(request);
            var failures = response.Entries.Where(e => e.Status != null && e.Status.Code != 0).ToList();
            if (failures.Any())
                throw new ProcessorStateException(
                    $"{failures.Count} of {rows.Count} rows couldn't be written in Bigtable table {tableName}: {failures[0].Status.Message}");
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(byte[] startClosed, byte[] endOpen, bool reversed)
        {
            var range = RowRange.ClosedOpen(
                new BigtableByteString(startClosed),
                endOpen == null ? (BigtableByteString?) null : new BigtableByteString(endOpen));
            return ReadRows(RowSet.FromRowRanges(range), reversed, null);
        }

        private IEnumerable<KeyValuePair<byte[], byte[]>> ReadRows(RowSet rows, bool reversed, long? limit)
        {
            var request = new ReadRowsRequest
            {
                TableNameAsTableName = tableName,
                Rows = rows,
                Filter = valueFilter,
                Reversed = reversed,
                AppProfileId = options.AppProfileId ?? string.Empty
            };
            if (limit.HasValue)
                request.RowsLimit = limit.Value;

            var enumerator = client.ReadRows(request).GetAsyncEnumerator(CancellationToken.None);
            try
            {
                while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                {
                    var row = enumerator.Current;
                    var cell = row.Families.SelectMany(f => f.Columns).SelectMany(c => c.Cells).FirstOrDefault();
                    if (cell != null)
                        yield return new KeyValuePair<byte[], byte[]>(row.Key.ToByteArray(), cell.Value.ToByteArray());
                }
            }
            finally
            {
                enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
        }

        // one cell per row: the previous cells of the column are deleted with the new one written (atomic in the row),
        // so the table doesn't need a garbage collection rule
        private Mutation[] MutationsFor(byte[] value)
            => value == null
                ? new[] {Mutations.DeleteFromRow()}
                : new[]
                {
                    Mutations.DeleteFromColumn(family, Qualifier),
                    Mutations.SetCell(family, Qualifier, new BigtableByteString(value))
                };

        public void Dispose()
        {
            // BigtableClient has no resource to release (its channels are pooled)
        }
    }
}
