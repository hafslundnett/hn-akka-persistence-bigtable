using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence;
using Akka.Persistence.TCK.Journal;
using System;
using System.Collections.Generic;
using Xunit;

namespace Hafslund.Akka.Persistence.Bigtable.IntegrationTests.Journal
{
    /// <summary>
    /// To run tests locally, first run: gcloud beta emulators bigtable start --host-port localhost:8090
    /// </summary>
    public class BigtableJournalSpec : JournalSpec
    {
        public static readonly string Host = GetEnvOrDefault("BIGTABLE_EMULATOR_HOST", "localhost:8090");

        private const string ProjectId = "my-project";
        private const string InstanceId = "my-instance";
        private static readonly string TableName = "JournalSpec";
        private static readonly Config SpecConfig;

        private static string GetTablePath()
        {
            return $"projects/{ProjectId}/instances/{InstanceId}/tables/{TableName}";
        }

        static BigtableJournalSpec()
        {
            Console.WriteLine($"BigtableJournalSpec bigtable table: {TableName}");

            SpecConfig = ConfigurationFactory.ParseString($"akka.test.timefactor=10")
                .WithFallback(ConfigurationFactory.ParseString(@"
                akka {
                    actor {
                        serializers {
                            messagepack = ""Akka.Serialization.MessagePack.MsgPackSerializer, Akka.Serialization.MessagePack""
                            actor-ref-wrapper-serializer = ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.ActorRefWrapperSerializer, Hafslund.Akka.Persistence.Bigtable.IntegrationTests""
                        }
                        serialization-bindings {
                            ""System.Object"" = messagepack
                            ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.ActorRefWrapper, Hafslund.Akka.Persistence.Bigtable.IntegrationTests"" = actor-ref-wrapper-serializer
                        }
                        serialization-identifiers {
                            ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.ActorRefWrapperSerializer, Hafslund.Akka.Persistence.Bigtable.IntegrationTests"" = 9999
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
                                class = ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.Journal.BigtableJournalTester, Hafslund.Akka.Persistence.Bigtable.IntegrationTests""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + GetTablePath() + @"""
                                auto-initialize = on
                            }
                        }
                    }
                }"));
        }
        public BigtableJournalSpec() : base(SpecConfig)
        {
            BigtableTestUtils.InitializeWithEmulator(Host, ProjectId, InstanceId, TableName);
            Initialize();
        }

        protected override void PreparePersistenceId(string pid)
        {
            BigtableTestUtils.InitializeWithEmulator(Host, ProjectId, InstanceId, TableName);
        }

        private static string GetEnvOrDefault(string name, string defaultValue)
        {
            return Environment.GetEnvironmentVariable(name) ?? defaultValue;
        }

        [Fact]
        public void Journal_fail_to_write_event_if_the_sequence_number_is_already_used()
        {
            var testProbe = CreateTestProbe();

            Journal.Tell(new WriteMessages(
                new List<IPersistentEnvelope>() {
                    new AtomicWrite(new Persistent("event1", persistenceId: Pid, sequenceNr:0))
                    }, testProbe.Ref, 2));

            testProbe.ExpectMsg<WriteMessagesSuccessful>();
            testProbe.ExpectMsg<WriteMessageSuccess>();

            Journal.Tell(new WriteMessages(
                 new List<IPersistentEnvelope>(){
                    new AtomicWrite(new Persistent("event2", persistenceId:Pid, sequenceNr:0)),
                    new AtomicWrite(new Persistent("event3", persistenceId:Pid, sequenceNr:1))
                }, testProbe.Ref, 2));

            testProbe.ExpectMsg<WriteMessagesFailed>();
            testProbe.ExpectMsg<WriteMessageFailure>();
            testProbe.ExpectMsg<WriteMessageFailure>();
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