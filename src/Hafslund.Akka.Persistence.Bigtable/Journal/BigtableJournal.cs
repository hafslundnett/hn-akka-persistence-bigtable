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
using Akka.Event;
using Akka.Pattern;

namespace Hafslund.Akka.Persistence.Bigtable.Journal
{


    public class BigtableJournal : AsyncWriteJournal
    {
        private static readonly Type PersistentRepresentationType = typeof(IPersistentRepresentation);
        private static readonly ByteString PayloadColumnQualifier = ByteString.CopyFromUtf8("p");
        private static readonly char RowKeySeparator = '#';
        private readonly string _family;
        private readonly BigtableClient _bigtableClient;
        private readonly TableName _tableName;
        private readonly Serializer _serializer;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly Address _transportSerializationFallbackAddress;
        private readonly bool _serializeWithTransport;

        public BigtableJournal() : this(BigtablePersistence.Get(Context.System))
        {
        }

        public BigtableJournal(BigtablePersistence bigtablePersistence) : this(
            bigtablePersistence.JournalSettings,
            bigtablePersistence.TransportSerializationSetttings)
        {
        }

        public BigtableJournal(BigtableJournalSettings settings, BigtableTransportSerializationSettings transportSerializationSettings)
        {
            _log.Info($"{nameof(BigtableJournal)}: constructing, with table name '{settings.TableName}'");
            _tableName = TableName.Parse(settings.TableName);
            _family = settings.FamilyName;
            _bigtableClient = BigtableClient.Create();
            _serializer = Context.System.Serialization.FindSerializerForType(PersistentRepresentationType);
            _transportSerializationFallbackAddress = transportSerializationSettings.GetFallbackAddress(Context);
            _serializeWithTransport = settings.EnableSerializationWithTransport;
            _transportSerializationFallbackAddress = _serializeWithTransport ? transportSerializationSettings.GetFallbackAddress(Context) : null;
            _log.Info($"EnableSerializationWithTransport: {_serializeWithTransport}");
            _log.Info($"TransportSerializationFallbackAddress: {_transportSerializationFallbackAddress}");
        }

        protected override void PreStart()
        {
            _log.Info("Initializing Bigtable Journal Storage...");
            base.PreStart();
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var rowRange = RowRange.ClosedOpen(
                ToRowKeyBigtableByteString(persistenceId, fromSequenceNr),
                ToRowKeyBigtableByteString(persistenceId, long.MaxValue));
            var rows = RowSet.FromRowRanges(rowRange);
            var stream = _bigtableClient.ReadRows(_tableName, rows: rows);
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

            var stream = _bigtableClient.ReadRows(_tableName, rows: rowSet, rowsLimit: max);

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
            var stream = _bigtableClient.ReadRows(_tableName, rows: rows);
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
                deleteEntries.Add(Mutations.CreateEntry(last.RowKey, Mutations.SetCell(_family, PayloadColumnQualifier, ByteString.Empty, new BigtableVersion(-1))));
                await _bigtableClient.MutateRowsAsync(_tableName, deleteEntries);
            }
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var actorSystem = Context.System;
            int loopCounter = 0;
            foreach (var atomicWrite in messages)
            {
                var request = ToBigtableWriteRequest(atomicWrite, actorSystem);
                var response = await _bigtableClient.CheckAndMutateRowAsync(request).ConfigureAwait(false);
                if (response.PredicateMatched) // row already existed
                {
                    var msg = $"The journal event already exists: {atomicWrite.PersistenceId}-{atomicWrite.LowestSequenceNr}";
                    _log.Warning(msg);
                    var exception = new IllegalActorStateException(msg);
                    return Enumerable
                        .Concat(
                            Enumerable.Repeat<Exception>(null, loopCounter), 
                            Enumerable.Repeat(exception, messages.Count() - loopCounter))
                        .ToImmutableList();
                }
                loopCounter++;
            }
            return null;
        }


        private CheckAndMutateRowRequest ToBigtableWriteRequest(AtomicWrite atomicWrite, ActorSystem actorSystem)
        {
            if (atomicWrite.HighestSequenceNr != atomicWrite.LowestSequenceNr)
            {
                throw new NotSupportedException("Journal does not support multiple events in a single atomic write");
            }

            var persistent = ((IImmutableList<IPersistentRepresentation>)atomicWrite.Payload).Single();

            return ToMutateRowIfNotExistsRequest(persistent, actorSystem);
        }

        private static long GetSequenceNumber(Row bigtableRow)
        {
            var rowKeyString = bigtableRow.Key.ToStringUtf8();
            var from = rowKeyString.LastIndexOf(RowKeySeparator) + 1;
            return long.Parse(rowKeyString.Substring(from));
        }

        private static string ToRowKeyString(string persistenceId, long sequenceNumber)
        {
            return $"{persistenceId}{RowKeySeparator}{sequenceNumber.ToString("D19")}";
        }

        public static BigtableByteString ToRowKeyBigtableByteString(string persistenceId, long sequenceNumber)
        {
            return new BigtableByteString(ToRowKeyString(persistenceId, sequenceNumber));
        }

        private byte[] PersistentToBytes(IPersistentRepresentation message, ActorSystem system)
        {
            if (_serializeWithTransport)
            {
                return Serialization.SerializeWithTransport(system, _transportSerializationFallbackAddress, () => _serializer.ToBinary(message));
            }
            else
            {
                return _serializer.ToBinary(message);
            }
        }

        private IPersistentRepresentation PersistentFromBytes(byte[] bytes)
        {
            return _serializer.FromBinary<IPersistentRepresentation>(bytes);
        }

        private IPersistentRepresentation ToPersistentRepresentation(Row BigtableRow)
        {
            var columnFamily = BigtableRow.Families.First(f => f.Name == _family);
            var column = columnFamily.Columns.Single(c => c.Qualifier.Equals(PayloadColumnQualifier));
            var byteString = column.Cells.First().Value;

            if (ByteString.Empty.Equals(byteString))
            {
                return null;
            }

            return PersistentFromBytes(byteString.ToArray());
        }

        private CheckAndMutateRowRequest ToMutateRowIfNotExistsRequest(IPersistentRepresentation persistent, ActorSystem system)
        {
            var request = new CheckAndMutateRowRequest();
            request.TableNameAsTableName = _tableName;
            var payload = PersistentToBytes(persistent, system);
            request.PredicateFilter = RowFilters.PassAllFilter();
            request.RowKey = ByteString.CopyFromUtf8(ToRowKeyString(persistent.PersistenceId, persistent.SequenceNr));
            request.FalseMutations.Add(Mutations.SetCell(_family, PayloadColumnQualifier, payload, new BigtableVersion(-1)));
            return request;
        }
    }
}