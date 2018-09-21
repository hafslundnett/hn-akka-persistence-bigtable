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

            _log.Info($"{nameof(BigtableSnapshotStore)}: constructing, with table name '{settings.TableName}'");
            _tableName = TableName.Parse(settings.TableName);
            _family = settings.FamilyName;
            _bigtableClient = BigtableClient.Create();
            _snapshotSerializer = Context.System.Serialization.FindSerializerForType(typeof(AkkaPersistenceSerialization.Snapshot));
            _snapshotMetadataSerializer = Context.System.Serialization.FindSerializerForType(typeof(SnapshotMetadata));
            _serializeWithTransport = settings.EnableSerializationWithTransport;
            _transportSerializationFallbackAddress = _serializeWithTransport ?  transportSerializationSettings.GetFallbackAddress(Context) : null;

            _log.Info($"EnableSerializationWithTransport: {_serializeWithTransport}");
            _log.Info($"TransportSerializationFallbackAddress: {_transportSerializationFallbackAddress}");
        }

        protected override void PreStart()
        {
            _log.Info("Initializing Bigtable Snapshot Storage...");
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
            var startKey = GetRowKey(persistenceId, criteria.MinSequenceNr);
            var endKey = GetRowKey(persistenceId, criteria.MaxSequenceNr);

            var rows = await _bigtableClient.ReadClosedRowRangeAsync(_tableName, startKey, endKey).ConfigureAwait(false);

            var deletes = rows.Select(PersistentFromBigtableRow)
                .Where(p => SatisfiesCriteria(criteria, p))
                .Select(p => Mutations.CreateEntry(GetRowKey(persistenceId, p.Metadata.SequenceNr), Mutations.DeleteFromRow()));

            if (deletes.Any())
            {
                await _bigtableClient.MutateRowsAsync(_tableName, deletes).ConfigureAwait(false);
            }
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var startKey = GetRowKey(persistenceId, criteria.MinSequenceNr);
            var endKey = GetRowKey(persistenceId, criteria.MaxSequenceNr);

            var rows = await _bigtableClient.ReadClosedRowRangeAsync(_tableName, startKey, endKey).ConfigureAwait(false);

            var selectedSnapshot = rows.Select(PersistentFromBigtableRow)
                .OrderByDescending(persistent => persistent.Metadata.SequenceNr)
                .ThenByDescending(persistent => persistent.Metadata.Timestamp)
                .FirstOrDefault(persistent => SatisfiesCriteria(criteria, persistent));

            return selectedSnapshot;
        }

        private bool SatisfiesCriteria(SnapshotSelectionCriteria criteria, SelectedSnapshot snapshot)
        {
            return
                snapshot.Metadata.SequenceNr >= criteria.MinSequenceNr &&
                snapshot.Metadata.SequenceNr <= criteria.MaxSequenceNr &&
                snapshot.Metadata.Timestamp >= criteria.MinTimestamp &&
                snapshot.Metadata.Timestamp <= criteria.MaxTimeStamp;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var snapshotBytes = SnapshotToBytes(metadata, snapshot, Context.System);
            byte[] snapshotMetadataBytes = SnapshotMetadataToBytes(metadata);
            var request = new MutateRowRequest();
            request.TableNameAsTableName = _tableName;
            request.Mutations.Add(Mutations.SetCell(_family, SnapshotColumnQualifier, ByteString.CopyFrom(snapshotBytes), new BigtableVersion(-1)));
            request.Mutations.Add(Mutations.SetCell(_family, SnapshotMetaDataColumnQualifier, ByteString.CopyFrom(snapshotMetadataBytes), new BigtableVersion(-1)));
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

        private SelectedSnapshot PersistentFromBigtableRow(Row BigtableRow)
        {
            var snapshotBytes = BigtableRow.Families
                .Single(f => f.Name.Equals(_family)).Columns
                .Single(c => c.Qualifier.Equals(SnapshotColumnQualifier)).Cells
                .First().Value.ToArray();
            
            var snapshotMetaDataBytes = BigtableRow.Families
                .Single(f => f.Name.Equals(_family)).Columns
                .Single(c => c.Qualifier.Equals(SnapshotMetaDataColumnQualifier)).Cells
                .First().Value.ToArray();

            return new SelectedSnapshot(SnapshotMetadataFromBytes(snapshotMetaDataBytes), SnapshotFromBytes(snapshotBytes).Data);
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