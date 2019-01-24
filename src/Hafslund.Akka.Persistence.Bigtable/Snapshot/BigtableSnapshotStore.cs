using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Snapshot;
using AkkaPersistenceSerialization = Akka.Persistence.Serialization;
using Akka.Serialization;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;

namespace Hafslund.Akka.Persistence.Bigtable.Snapshot
{
    public class BigtableSnapshotStore : SnapshotStore
    {
        private static readonly ByteString SnapshotColumnQualifier = ByteString.CopyFromUtf8("s");
        private static readonly ByteString TimestampColumnQualifier = ByteString.CopyFromUtf8("t");
        private static readonly string RowKeySeparator = "#";
        private readonly string _family;
        private readonly BigtableClient _bigtableClient;
        private readonly TableName _tableName;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly Serializer _snapshotSerializer;
        private readonly Address _transportSerializationFallbackAddress;
        private readonly bool _serializeWithTransport;

        public BigtableSnapshotStore() : this(BigtablePersistence.Get(Context.System))
        {
        }

        public BigtableSnapshotStore(BigtablePersistence bigtablePersistence) : this(
            bigtablePersistence.SnapshotSettings,
            bigtablePersistence.TransportSerializationSetttings)
        {
        }

        public BigtableSnapshotStore(BigtableSnapshotSettings settings, BigtableTransportSerializationSettings transportSerializationSettings)
        {

            _tableName = TableName.Parse(settings.TableName);
            _family = settings.FamilyName;
            _bigtableClient = CreateBigtableClient();
            _snapshotSerializer = Context.System.Serialization.FindSerializerForType(typeof(AkkaPersistenceSerialization.Snapshot));
            _serializeWithTransport = settings.EnableSerializationWithTransport;
            _transportSerializationFallbackAddress = _serializeWithTransport ? transportSerializationSettings.GetFallbackAddress(Context) : null;

            _log.Debug($"{nameof(BigtableSnapshotStore)}: constructing, with table name '{settings.TableName}'");
            _log.Debug($"EnableSerializationWithTransport: {_serializeWithTransport}");
            _log.Debug($"TransportSerializationFallbackAddress: {_transportSerializationFallbackAddress}");
        }

        protected virtual BigtableClient CreateBigtableClient()
        {
            return BigtableClient.Create();
        }

        protected override void PreStart()
        {
            _log.Debug("Initializing Bigtable Snapshot Storage...");
            base.PreStart();
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            await _bigtableClient.MutateRowAsync(
                _tableName,
                GetRowKey(metadata.PersistenceId, metadata.SequenceNr),
                new List<Mutation> { Mutations.DeleteFromRow() }).ConfigureAwait(false);
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var filter = RowFilters.Chain
            (
                RowFilters.ColumnQualifierExact(TimestampColumnQualifier),
                RowFilters.CellsPerColumnLimit(1),
                RowFilters.ValueRange(ValueRange.Closed(GetByteString(criteria.MinTimestamp), GetByteString(criteria.MaxTimeStamp))),
                RowFilters.StripValueTransformer()
            );

            var readRowsRequest = new ReadRowsRequest
            {
                TableNameAsTableName = _tableName,
                Filter = filter,
                Rows = GetRowSet(persistenceId, criteria.MinSequenceNr, criteria.MaxSequenceNr)
            };

            var deleteMutations = await _bigtableClient
                .ReadRows(readRowsRequest)
                .Select(row => Mutations.CreateEntry(row.Key, Mutations.DeleteFromRow()))
                .ToList()
                .ConfigureAwait(false);

            if (deleteMutations.Count > 0)
            {
                await _bigtableClient.MutateRowsAsync(_tableName, deleteMutations).ConfigureAwait(false);
            }
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            if (criteria.MinSequenceNr > criteria.MaxSequenceNr)
            {
                return null;
            }

            return await _bigtableClient
                .ReadRows(GetReadRowsRequest(persistenceId, criteria))
                .Select(PersistentFromBigtableRow)
                .FirstOrDefault()
                .ConfigureAwait(false);
        }

        private ReadRowsRequest GetReadRowsRequest(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var filter = RowFilters.Chain
            (
                RowFilters.CellsPerColumnLimit(1),
                RowFilters.ValueRange(ValueRange.Closed(GetByteString(criteria.MinTimestamp), GetByteString(criteria.MaxTimeStamp)))
            );
            return new ReadRowsRequest
            {
                TableNameAsTableName = _tableName,
                Filter = filter,
                Rows = GetRowSet(persistenceId, criteria.MinSequenceNr, criteria.MaxSequenceNr)
            };
        }

        private BigtableByteString? GetByteString(DateTime? date)
        {
            if (date.HasValue)
            {
                return ByteString.CopyFrom(GetBytes(date.Value));
            }

            return null;
        }

        private RowSet GetRowSet(string persistenceId, long minSequenceNr, long maxSequenceNr)
        {
            var from = GetRowKey(persistenceId, maxSequenceNr);
            var to = GetRowKey(persistenceId, minSequenceNr);

            RowSet rowSet;
            if (minSequenceNr == maxSequenceNr)
            {
                rowSet = RowSet.FromRowKey(from);
            }
            else
            {
                rowSet = RowSet.FromRowRanges(RowRange.Closed(from, to));
            }

            return rowSet;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var snapshotBytes = SnapshotToBytes(metadata, snapshot, Context.System);
            var timestampUtc = metadata.Timestamp.ToUniversalTime();
            var timestampBytes = ByteString.CopyFrom(GetBytes(timestampUtc));

            var request = new MutateRowRequest();
            var version = new BigtableVersion(timestampUtc);
            request.TableNameAsTableName = _tableName;
            request.Mutations.Add(Mutations.SetCell(_family, SnapshotColumnQualifier, ByteString.CopyFrom(snapshotBytes), version));
            request.Mutations.Add(Mutations.SetCell(_family, TimestampColumnQualifier, timestampBytes, version));
            request.RowKey = GetRowKey(metadata.PersistenceId, metadata.SequenceNr);
            await _bigtableClient.MutateRowAsync(request).ConfigureAwait(false);
        }

        private byte[] GetBytes(DateTime dateTime)
        {
            var bytes = BitConverter.GetBytes(dateTime.Ticks);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return bytes;
        }

        private DateTime GetTimestamp(Row row)
        {
            var bytes = row.Families
                .Single(f => f.Name.Equals(_family)).Columns
                .Single(c => c.Qualifier.Equals(TimestampColumnQualifier)).Cells
                .First().Value.ToByteArray();

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            var ticks = BitConverter.ToInt64(bytes, 0);
            return new DateTime(ticks, DateTimeKind.Utc);
        }

        private SnapshotMetadata SnapshotMetadataFromBigtableRow(Row row)
        {
            var rowKey = row.Key.ToStringUtf8();
            var rowKeySeparatorIndex = rowKey.LastIndexOf(RowKeySeparator);
            var pid = rowKey.Substring(0, rowKeySeparatorIndex);
            long sequenceNumber = long.MaxValue - long.Parse(rowKey.Substring(rowKeySeparatorIndex + 1));

            var timestamp = GetTimestamp(row);

            return new SnapshotMetadata(pid, sequenceNumber, timestamp);
        }

        private byte[] SnapshotToBytes(SnapshotMetadata metadata, object snapshotData, ActorSystem actorSystem)
        {
            var snapshot = new AkkaPersistenceSerialization.Snapshot(snapshotData);
            if (_serializeWithTransport)
            {
                return Serialization.SerializeWithTransport(actorSystem, _transportSerializationFallbackAddress, () => _snapshotSerializer.ToBinary(snapshot));
            }
            else
            {
                return _snapshotSerializer.ToBinary(snapshot);
            }
        }

        private SelectedSnapshot PersistentFromBigtableRow(Row row)
        {
            if (row == null)
            {
                return null;
            }

            var snapshotBytes = row.Families
                .Single(f => f.Name.Equals(_family)).Columns
                .Single(c => c.Qualifier.Equals(SnapshotColumnQualifier)).Cells
                .First().Value.ToArray();

            return new SelectedSnapshot(SnapshotMetadataFromBigtableRow(row), SnapshotFromBytes(snapshotBytes).Data);
        }

        private AkkaPersistenceSerialization.Snapshot SnapshotFromBytes(byte[] bytes)
        {
            return _snapshotSerializer.FromBinary<AkkaPersistenceSerialization.Snapshot>(bytes);
        }
        private static string ToRowKeyString(string persistenceId, long sequenceNumber)
        {
            return $"{persistenceId}{RowKeySeparator}{(long.MaxValue - sequenceNumber).ToString("D19")}";
        }

        private static ByteString GetRowKey(string persistenceId, long sequenceNumber)
        {
            return ByteString.CopyFromUtf8(ToRowKeyString(persistenceId, sequenceNumber));
        }
    }
}