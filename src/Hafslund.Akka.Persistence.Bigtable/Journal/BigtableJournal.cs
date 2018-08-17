using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence;
using Akka.Persistence.Journal;
using Akka.Serialization;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using System.Linq;
using Google.Cloud.Bigtable.Common.V2;

namespace Hafslund.Akka.Persistence.Bigtable.Journal
{
    public class BigtableJournal : AsyncWriteJournal
    {
        private static readonly Type PersistentRepresentationType = typeof (IPersistentRepresentation);
        private static string Family = "f";
        private static ByteString PayloadColumnQualifier = ByteString.CopyFromUtf8("p");
        public static string RowKeySeparator = "#";
        private BigtableClient _BigtableClient;
        private readonly TableName _tableName;
        private Serializer _serializer;

        public BigtableJournal()
        {
            var tableNameAsString = GetTableName();
            _tableName = TableName.Parse(tableNameAsString);
            _BigtableClient = BigtableClient.Create();
            _serializer = Context.System.Serialization.FindSerializerForType(PersistentRepresentationType);
        }

        protected virtual string GetTableName()
        {
            return Context.System.Settings.Config.GetConfig("akka.persistence.journal.Bigtable").GetString("table-name");
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var rowRange = RowRange.ClosedOpen(
                ToRowKeyBigtableByteString(persistenceId, fromSequenceNr),
                ToRowKeyBigtableByteString(persistenceId, long.MaxValue));
            var rows = RowSet.FromRowRanges(rowRange);
            var stream = _BigtableClient.ReadRows(_tableName, rows: rows);
            var lastRow = await stream.LastOrDefault().ConfigureAwait(false);
            return lastRow == null ? 0 : GetSequenceNumber(lastRow);
        }
        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            if (max <= 0 || toSequenceNr < fromSequenceNr)
            {
                return;
            }

            var startKey = ToRowKeyBigtableByteString(persistenceId, fromSequenceNr);
            var endKey = ToRowKeyBigtableByteString(persistenceId, toSequenceNr);
            RowSet rowSet;
            if (fromSequenceNr == toSequenceNr)
            {
                rowSet = RowSet.FromRowKey(startKey);
            }
            else
            {
                rowSet = RowSet.FromRowRanges(RowRange.Closed(startKey, endKey));
            }

            var stream = _BigtableClient.ReadRows(_tableName, rows: rowSet, rowsLimit: max);

            using (var asyncEnumerator = stream.GetEnumerator())
            {
                while (await asyncEnumerator.MoveNext().ConfigureAwait(false))
                {
                    var persitentRepresentation = ToPersistentRepresentation(asyncEnumerator.Current);
                    if (persitentRepresentation != null)
                    {
                        recoveryCallback.Invoke(persitentRepresentation);
                    }
                }
            }
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var rows = RowSet.FromRowRanges(RowRange.Closed(ToRowKeyBigtableByteString(persistenceId, 0), ToRowKeyBigtableByteString(persistenceId, toSequenceNr)));
            var stream = _BigtableClient.ReadRows(_tableName, rows: rows);
            var deleteEntries = new List<MutateRowsRequest.Types.Entry>();
            using (var enumerator = stream.GetEnumerator())
            {
                while (await enumerator.MoveNext().ConfigureAwait(false))
                {
                    deleteEntries.Add(Mutations.CreateEntry(enumerator.Current.Key, Mutations.DeleteFromRow()));
                }
            }

            if (deleteEntries.Any())
            {
                var last = deleteEntries.LastOrDefault();
                deleteEntries.RemoveAt(deleteEntries.Count - 1);
                deleteEntries.Add(Mutations.CreateEntry(last.RowKey, Mutations.SetCell(Family, PayloadColumnQualifier, ByteString.Empty, new BigtableVersion(-1))));
                await _BigtableClient.MutateRowsAsync(_tableName, deleteEntries);
            }
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var BigtableEntries = messages
                .SelectMany(atomicWrite => (IImmutableList<IPersistentRepresentation>)atomicWrite.Payload)
                .Select(ToBigtableEntry);

            IImmutableList<Exception> exceptions = null;
            try
            {
                await _BigtableClient.MutateRowsAsync(_tableName, BigtableEntries).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                exceptions = ImmutableList.Create(e);
            }

            return exceptions;
        }

        private static long GetSequenceNumber(Row BigtableRow)
        {
            // Note: String.Split(String, Int32) does not exist in .NET Standard 2.0. Converting to char array:
            var parts = BigtableRow.Key.ToStringUtf8().Split(RowKeySeparator.ToArray(), 2);
            return long.Parse(parts[1]);
        }

        private static string ToRowKeyString(string persistenceId, long sequenceNumber)
        { 
            return $"{persistenceId}{RowKeySeparator}{sequenceNumber.ToString("D19")}";
        }

        public static BigtableByteString ToRowKeyBigtableByteString(string persistenceId, long sequenceNumber)
        {
            return new BigtableByteString(ToRowKeyString(persistenceId, sequenceNumber));
        }

        private byte[] PersistentToBytes(IPersistentRepresentation message)
        {
            return _serializer.ToBinary(message);
        }

        private IPersistentRepresentation PersistentFromBytes(byte[] bytes)
        {
            return _serializer.FromBinary<IPersistentRepresentation>(bytes);
        }

        private IPersistentRepresentation ToPersistentRepresentation(Row BigtableRow)
        {
            var columnFamily = BigtableRow.Families.Where(f => f.Name == Family).First();
            var column = columnFamily.Columns.Single(c => c.Qualifier.Equals(PayloadColumnQualifier));
            var byteString = column.Cells.First().Value;

            if (ByteString.Empty.Equals(byteString))
            {
                return null;
            }

            return PersistentFromBytes(byteString.ToArray());
        }

        private MutateRowsRequest.Types.Entry ToBigtableEntry(IPersistentRepresentation persistent)
        {
            var entry = new MutateRowsRequest.Types.Entry();
            entry.RowKey = ByteString.CopyFromUtf8(ToRowKeyString(persistent.PersistenceId, persistent.SequenceNr));
            var payload = PersistentToBytes(persistent);
            entry.Mutations.Add(Mutations.SetCell(Family, PayloadColumnQualifier, payload, new BigtableVersion(-1)));
            return entry;
        }
    }
}