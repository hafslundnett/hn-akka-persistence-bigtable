using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence;
using Akka.Persistence.TCK.Journal;
using Akka.Serialization;
using Akka.TestKit;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Hafslund.Akka.Persistence.Bigtable.IntegrationTests;
using Microsoft.Extensions.Configuration;
using Moq;
using System;
using System.Collections.Generic;
using Xunit;

namespace Hafslund.Akka.Persistence.Bigtable.Tests.Integration.Journal
{
    public class BigtableJournalSpec : JournalSpec
    {
        private readonly static string TableName;
        private static readonly Config SpecConfig;
        public static IConfigurationRoot ReadConfig()
        {
            return new ConfigurationBuilder()
                .AddJsonFile("appsettings.Development.json", optional: true)
                .AddEnvironmentVariables()
                .Build();
        }

        static BigtableJournalSpec()
        {
            var config = ReadConfig();

            var timeFactor = int.Parse(config.GetValue("INTEGRATION_TEST_TIME_FACTOR", "1"));
            Console.WriteLine($"BigtableSnapshotStoreSpec timefactor: {timeFactor}");

            TableName = config.GetValue("INTEGRATION_TEST_JOURNAL_TABLE", "NOT_SET");
            Console.WriteLine($"BigtableJournalSpec bigtable table: {TableName}");

            SpecConfig = ConfigurationFactory.ParseString($"akka.test.timefactor={timeFactor}")
                .WithFallback(ConfigurationFactory.ParseString(@"
                akka {
                    actor {
                        serialize-messages = on
                        serializers {
                            my-event-serializer = ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.MyEventSerializer, Hafslund.Akka.Persistence.Bigtable.IntegrationTests""
                        }
                        serialization-bindings {
                            ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.MyEvent, Hafslund.Akka.Persistence.Bigtable.IntegrationTests"" = my-event-serializer
                        }
                        serialization-identifiers {
                            ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.MyEventSerializer, Hafslund.Akka.Persistence.Bigtable.IntegrationTests"" = 9999
                        }
                    }
   
                    persistence {
                        transport-serialization {
                            bigtable {
                                hostname = ""localhost""
                                transport-protocol = akka.tcp
                                port = 2552
                            }
                        }
                        publish-plugin-commands = on
                        journal {
                            plugin = ""akka.persistence.journal.bigtable""
                            bigtable {
                                enable-serialization-with-transport = true
                                class = ""Hafslund.Akka.Persistence.Bigtable.Journal.BigtableJournal, Hafslund.Akka.Persistence.Bigtable""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + TableName + @"""
                                auto-initialize = on
                            }
                        }
                    }
                }"));
        }
        public BigtableJournalSpec() : base(SpecConfig)
        {
            Initialize();
        }

        protected override void PreparePersistenceId(string pid)
        {
            var rowRange = RowRange.Closed(new BigtableByteString($"{pid}"), new BigtableByteString($"{pid}~"));
            BigtableTestUtils.DeleteRows(TableName, rowRange);
        }


        [Fact]
        public void Journal_should_reject_event_if_it_already_exists()
        {
            //Given
            var testProbe1 = CreateTestProbe();
            var testProbe2 = CreateTestProbe();

            //Then
            Journal.Tell(new WriteMessages(new List<IPersistentEnvelope>() { new AtomicWrite(new Persistent("event1", persistenceId: Pid)) }, testProbe1.Ref, ActorInstanceId), testProbe1.Ref);
            testProbe1.ExpectMsg<WriteMessagesSuccessful>();
            testProbe1.ExpectMsg<WriteMessageSuccess>();
            Journal.Tell(new WriteMessages(
                new List<IPersistentEnvelope>(){
                    new AtomicWrite(new Persistent("event2", persistenceId:Pid, sequenceNr:0)),
                    new AtomicWrite(new Persistent("event3", persistenceId:Pid, sequenceNr:1))
                }, testProbe2.Ref, ActorInstanceId), testProbe2.Ref);
            testProbe2.ExpectMsg<WriteMessagesSuccessful>();
            testProbe2.ExpectMsg<WriteMessageRejected>(msg =>
            {
                var sequenceNr = msg.Persistent.SequenceNr;
                Assert.True(true);
            });
            testProbe2.ExpectMsg<WriteMessageRejected>(msg =>
            {
                var sequenceNr = msg.Persistent.SequenceNr;
                Assert.True(true);
            });
        }

        [Fact]
        public void Journal_should_serialize_with_transport_if_enabled()
        {
            //Given
            var ser = Sys.Serialization.FindSerializerForType(typeof(ActorRefWrapper));
            var receiver = CreateTestProbe();
            var testActor = CreateTestActor("test-actor");

            var myEvent = new ActorRefWrapper
            {
                ActorRef = testActor
            };

            var messages = new List<IPersistentEnvelope>()
            {
                new AtomicWrite(new Persistent(myEvent, 0, Pid, string.Empty, false, testActor, WriterGuid))
            };

            //When
            Journal.Tell(new WriteMessages(messages, receiver, ActorInstanceId), testActor);
            receiver.ExpectMsg<WriteMessagesSuccessful>();
            receiver.ExpectMsg<WriteMessageSuccess>();
            Journal.Tell(new ReplayMessages(0, 1, 1, Pid, receiver.Ref));

            //Then
            receiver.ExpectMsg<ReplayedMessage>(msg =>
            {
                var e = (ActorRefWrapper)msg.Persistent.Payload;
                Assert.True(e.IsSerializedWithTransport, "event should be serialized with transport");
            });
        }

        protected override bool SupportsRejectingNonSerializableObjects => true;

        protected override bool SupportsAtomicPersistAllOfSeveralEvents => false;
    }
}