using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Persistence;
using Akka.Persistence.Snapshot;
using Akka.Serialization;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;

namespace Hafslund.Akka.Persistence.Bigtable.Snapshot
{
    public class BigtableSnapshotStore : SnapshotStore
    {
        private static readonly Type SnapshotType = typeof(SelectedSnapshot);
        private static string Family = "f";
        private static ByteString SnapshotColumnQualifier = ByteString.CopyFromUtf8("s");
        private static string RowKeySeparator = "#";
        private BigtableClient _BigtableClient;
        private readonly TableName _tableName;
        private Serializer _serializer;

        public BigtableSnapshotStore()
        {
            var tableNameAsString = GetTableName();
            _tableName = TableName.Parse(tableNameAsString);
            _BigtableClient = BigtableClient.Create();
            _serializer = Context.System.Serialization.FindSerializerForType(SnapshotType);
        }

        protected virtual string GetTableName()
        {
            return Context.System.Settings.Config.GetConfig("akka.persistence.snapshot-store.bigtable").GetString("table-name");
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            await _BigtableClient.MutateRowAsync(
                _tableName,
                GetRowKey(metadata.PersistenceId, metadata.SequenceNr),
                new List<Mutation> { Mutations.DeleteFromRow() }).ConfigureAwait(false);
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {

            var startKey = GetRowKey(persistenceId, criteria.MinSequenceNr);
            var endKey = GetRowKey(persistenceId, criteria.MaxSequenceNr);

            var rows = await _BigtableClient.ReadClosedRowRangeAsync(_tableName, startKey, endKey).ConfigureAwait(false);

            var deletes = rows.Select(PersistentFromBigtableRow)
                .Where(p => SatisfiesCriteria(criteria, p))
                .Select(p => Mutations.CreateEntry(GetRowKey(persistenceId, p.Metadata.SequenceNr), Mutations.DeleteFromRow()));

            if (deletes.Any())
            {
                await _BigtableClient.MutateRowsAsync(_tableName, deletes).ConfigureAwait(false);
            }
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var startKey = GetRowKey(persistenceId, criteria.MinSequenceNr);
            var endKey = GetRowKey(persistenceId, criteria.MaxSequenceNr);

            var rows = await _BigtableClient.ReadClosedRowRangeAsync(_tableName, startKey, endKey).ConfigureAwait(false);

            var selectedSnapshot = rows.Select(PersistentFromBigtableRow)
                .OrderByDescending(persistent => persistent.Metadata.SequenceNr)
                .ThenByDescending(persistent => persistent.Metadata.Timestamp)
                .Where(persistent => SatisfiesCriteria(criteria, persistent))
                .FirstOrDefault();

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
            var bytes = PersistentToBytes(metadata, snapshot);
            var request = new MutateRowRequest();
            request.TableNameAsTableName = _tableName;
            request.Mutations.Add(Mutations.SetCell(Family, SnapshotColumnQualifier, ByteString.CopyFrom(bytes), new BigtableVersion(-1)));
            request.RowKey = GetRowKey(metadata.PersistenceId, metadata.SequenceNr);
            await _BigtableClient.MutateRowAsync(request).ConfigureAwait(false);
        }

        private byte[] PersistentToBytes(SnapshotMetadata metadata, object snapshot)
        {
            return _serializer.ToBinary(new SelectedSnapshot(metadata, snapshot));
        }

        private SelectedSnapshot PersistentFromBigtableRow(Row BigtableRow)
        {
            var bytes = BigtableRow.Families
                .Single(f => f.Name.Equals(Family)).Columns
                .Single(c => c.Qualifier.Equals(SnapshotColumnQualifier)).Cells
                .First().Value.ToArray();

            return PersistentFromBytes(bytes);
        }

        private SelectedSnapshot PersistentFromBytes(byte[] bytes)
        {
            return _serializer.FromBinary<SelectedSnapshot>(bytes);
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