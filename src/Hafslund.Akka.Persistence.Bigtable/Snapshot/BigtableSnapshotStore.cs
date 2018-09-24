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
        private static readonly ByteString SnapshotMetaDataColumnQualifier = ByteString.CopyFromUtf8("m");
        private static readonly string RowKeySeparator = "#";
        private readonly string _family;
        private readonly BigtableClient _bigtableClient;
        private readonly TableName _tableName;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly Serializer _snapshotSerializer;
        private readonly Serializer _snapshotMetadataSerializer;
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
            _bigtableClient = BigtableClient.Create();
            _snapshotSerializer = Context.System.Serialization.FindSerializerForType(typeof(AkkaPersistenceSerialization.Snapshot));
            _snapshotMetadataSerializer = Context.System.Serialization.FindSerializerForType(typeof(SnapshotMetadata));
            _serializeWithTransport = settings.EnableSerializationWithTransport;
            _transportSerializationFallbackAddress = _serializeWithTransport ? transportSerializationSettings.GetFallbackAddress(Context) : null;

            _log.Debug($"{nameof(BigtableSnapshotStore)}: constructing, with table name '{settings.TableName}'");
            _log.Debug($"EnableSerializationWithTransport: {_serializeWithTransport}");
            _log.Debug($"TransportSerializationFallbackAddress: {_transportSerializationFallbackAddress}");
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
                RowFilters.ColumnQualifierExact(SnapshotMetaDataColumnQualifier),
                // this filter ensures that we only download snapshot metadata
                RowFilters.TimestampRange(
                    ToUtc(criteria.MinTimestamp),
                    ToUtc(criteria.MaxTimeStamp)?.AddMilliseconds(1)
                    // add a milliseconds since the upper bound is exclusive
                    ),
                RowFilters.CellsPerColumnLimit(1)
            );

            var readRowsRequest = new ReadRowsRequest
            {
                TableNameAsTableName = _tableName,
                Filter = filter,
                Rows = GetRowSet(persistenceId, criteria.MinSequenceNr, criteria.MaxSequenceNr)
            };

            var deleteMutations = await _bigtableClient
                .ReadRows(readRowsRequest)
                .Select(SnapshotMetadataFromBigtableRow)
                .Where(metadata => SatisfiesTimestampCriteria(criteria, metadata))
                .Select(metadata => Mutations.CreateEntry(GetRowKey(persistenceId, metadata.SequenceNr), Mutations.DeleteFromRow()))
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
                .OrderByDescending(snapshot => snapshot.Metadata.SequenceNr)
                .ThenByDescending(snapshot => snapshot.Metadata.Timestamp)
                .FirstOrDefault(snapshot => SatisfiesTimestampCriteria(criteria, snapshot.Metadata))
                .ConfigureAwait(false);
        }

        private ReadRowsRequest GetReadRowsRequest(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var filter = RowFilters.Chain
            (
                RowFilters.TimestampRange(
                    ToUtc(criteria.MinTimestamp)?.AddMilliseconds(-1),
                    // subtract millisecond since bigtable only has millisecond granularity 
                    ToUtc(criteria.MaxTimeStamp)?.AddMilliseconds(1)
                    // add a milliseconds since the upper bound is exclusive
                    ),
                RowFilters.CellsPerColumnLimit(1)
            );
            return new ReadRowsRequest
            {
                TableNameAsTableName = _tableName,
                Filter = filter,
                Rows = GetRowSet(persistenceId, criteria.MinSequenceNr, criteria.MaxSequenceNr)
            };
        }

        private DateTime? ToUtc(DateTime? dateTime)
        {
            if (dateTime.HasValue)
            {
                var dt = dateTime.Value.ToUniversalTime();
                if (dt.Year <= 1 || dt.Year >= 9999)
                {
                    return null;
                }

                return dt;
            }

            return null;
        }

        private RowSet GetRowSet(string persistenceId, long minSequenceNr, long maxSequenceNr)
        {
            var from = GetRowKey(persistenceId, minSequenceNr);
            var to = GetRowKey(persistenceId, maxSequenceNr);

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

        private bool SatisfiesTimestampCriteria(SnapshotSelectionCriteria criteria, SnapshotMetadata metadata)
        {
            return
                metadata.Timestamp >= criteria.MinTimestamp &&
                metadata.Timestamp <= criteria.MaxTimeStamp;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var snapshotBytes = SnapshotToBytes(metadata, snapshot, Context.System);
            byte[] snapshotMetadataBytes = SnapshotMetadataToBytes(metadata);
            var request = new MutateRowRequest();
            var version = new BigtableVersion(metadata.Timestamp.ToUniversalTime());
            request.TableNameAsTableName = _tableName;
            request.Mutations.Add(Mutations.SetCell(_family, SnapshotColumnQualifier, ByteString.CopyFrom(snapshotBytes), version));
            request.Mutations.Add(Mutations.SetCell(_family, SnapshotMetaDataColumnQualifier, ByteString.CopyFrom(snapshotMetadataBytes), version));
            request.RowKey = GetRowKey(metadata.PersistenceId, metadata.SequenceNr);
            await _bigtableClient.MutateRowAsync(request).ConfigureAwait(false);
        }

        private byte[] SnapshotMetadataToBytes(SnapshotMetadata metadata)
        {
            return _snapshotMetadataSerializer.ToBinary(metadata);
        }

        private byte[] SnapshotToBytes(SnapshotMetadata metadata, object snapshotData, ActorSystem actorSystem)
        {
            var selectedSnapshot = new AkkaPersistenceSerialization.Snapshot(snapshotData);
            if (_serializeWithTransport)
            {
                return Serialization.SerializeWithTransport(actorSystem, _transportSerializationFallbackAddress, () => _snapshotSerializer.ToBinary(selectedSnapshot));
            }
            else
            {
                return _snapshotSerializer.ToBinary(selectedSnapshot);
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

        private SnapshotMetadata SnapshotMetadataFromBigtableRow(Row row)
        {
            var snapshotMetaDataBytes = row.Families
                .Single(f => f.Name.Equals(_family)).Columns
                .Single(c => c.Qualifier.Equals(SnapshotMetaDataColumnQualifier)).Cells
                .First().Value.ToArray();

            return SnapshotMetadataFromBytes(snapshotMetaDataBytes);
        }

        private SnapshotMetadata SnapshotMetadataFromBytes(byte[] bytes)
        {
            return _snapshotMetadataSerializer.FromBinary<SnapshotMetadata>(bytes);
        }

        private AkkaPersistenceSerialization.Snapshot SnapshotFromBytes(byte[] bytes)
        {
            return _snapshotSerializer.FromBinary<AkkaPersistenceSerialization.Snapshot>(bytes);
        }
        private static string ToRowKeyString(string persistenceId, long sequenceNumber)
        {
            return $"{persistenceId}{RowKeySeparator}{sequenceNumber.ToString("D19")}";
        }

        public static ByteString GetRowKey(string persistenceId, long sequenceNumber)
        {
            return ByteString.CopyFromUtf8(ToRowKeyString(persistenceId, sequenceNumber));
        }

    }
}