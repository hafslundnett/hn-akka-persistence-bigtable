using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal;
using NBench;
using NBench.Util;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests
{
    /// <summary>
    /// To run tests locally, first run: gcloud beta emulators bigtable start --host-port localhost:8091
    /// </summary>
    public class BigtablePluginPerfSpec
    {
        public static readonly string Host = GetEnvOrDefault("BIGTABLE_EMULATOR_HOST", "localhost:8091");
        private const string ProjectId = "my-project";
        private const string InstanceId = "my-instance";

        public string SnapshotStoreTable = "SnapshotStorePerfSpec";
        public string JournalTable = "JournalPerfSpec";

        private readonly Config _specConfig;

        protected BigtablePluginPerfSpec()
        {
            _specConfig = GetSpecConfig(SnapshotStoreTable, JournalTable);
        }

        private static Config GetSpecConfig(string snapshotTable, string journalTable)
        {
            var specConfig = ConfigurationFactory.ParseString(@"
                akka {
                    actor {
                        serializers {
                            messagepack = ""Akka.Serialization.MessagePack.MsgPackSerializer, Akka.Serialization.MessagePack""
                        }

                        serialization-bindings {
                            ""System.Object"" = messagepack
                        }
                    }

                    persistence {
                        publish-plugin-commands = on
                        snapshot-store {
                            plugin = ""akka.persistence.snapshot-store.bigtable""
                            bigtable {
                                class = ""Hafslund.Akka.Persistence.Bigtable.PerformanceTests.BigtableSnapshotStoreTester, Hafslund.Akka.Persistence.Bigtable.PerformanceTests""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + GetTablePath(snapshotTable) + @"""
                                auto-initialize = on
                                default-serializer = messagepack
                            }
                        }

                        journal {
                            plugin = ""akka.persistence.journal.bigtable""
                            bigtable {
                                class = ""Hafslund.Akka.Persistence.Bigtable.PerformanceTests.BigtableJournalTester, Hafslund.Akka.Persistence.Bigtable.PerformanceTests""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + GetTablePath(journalTable) + @"""
                                auto-initialize = on
                                default-serializer = messagepack
                            }
                        }
                    }
                }");

            return specConfig;
        }
        
        private static string GetTablePath(string tableName)
        {
            return $"projects/{ProjectId}/instances/{InstanceId}/tables/{tableName}";
        }
        
        public static AtomicCounter TableVersionCounter = new AtomicCounter(0);       

        protected virtual int PersistentActorCount { get; } = 200;
        
        public static readonly TimeSpan MaxTimeout = TimeSpan.FromMinutes(6);
        
        protected ActorSystem ActorSystem { get; set; }
        
        protected IActorRef Supervisor { get; set; }
        
        private readonly List<string> _persistentActorIds = new List<string>();

        protected List<string> PersistentActorIds => _persistentActorIds;

        private static string GetEnvOrDefault(string name, string defaultValue)
        {
            return Environment.GetEnvironmentVariable(name) ?? defaultValue;
        }

        [PerfSetup]
        public virtual void Setup(BenchmarkContext context)
        {
            ActorSystem = ActorSystem.Create(GetType().Name + TableVersionCounter.Current, _specConfig);

            Supervisor = ActorSystem.ActorOf<BenchmarkActorSupervisor>("supervisor");

            foreach (var i in Enumerable.Range(0, PersistentActorCount))
            {
                var id = "persistent" + Guid.NewGuid();
                _persistentActorIds.Add(id);
            }

            // force the system to initialize
            WakeUpAllActors();
        }


        protected void WakeUpAllActors()
        {
            Task.WaitAll(_persistentActorIds.Select(id => Supervisor.Ask<PersistentBenchmarkMsgs.Done>(new BenchmarkActorMessage(id, PersistentBenchmarkMsgs.Init.Instance))).Cast<Task>().ToArray());
        }

        protected void TerminateAllActors()
        {
            Supervisor.Ask<AllTerminated>(new TerminateAll(), TimeSpan.FromMinutes(2)).GetAwaiter().GetResult();
        }

        protected void ReInitializeTable(string tableName)
        {
            BigtableTestUtils.InitializeWithEmulator(Host, ProjectId, InstanceId, tableName);
        }
    }
}