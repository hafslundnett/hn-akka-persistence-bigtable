namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal
{
    /// <summary>
    /// Messages used for working with benchmark actors
    /// </summary>
    public static class PersistentBenchmarkMsgs
    {
        public sealed class Init
        {
            public static readonly Init Instance = new Init();
            private Init() { }
        }

        public sealed class Finish
        {
            public static readonly Finish Instance = new Finish();
            private Finish() { }
        }
        public sealed class Done
        {
            public static readonly Done Instance = new Done();
            private Done() { }
        }
        public sealed class Finished
        {
            public readonly long State;

            public Finished(long state)
            {
                State = state;
            }
        }

        public sealed class Store
        {
            public readonly int Value;

            public Store(int value)
            {
                Value = value;
            }
        }

        public sealed class Stored
        {
            public readonly int Value;

            public Stored(int value)
            {
                Value = value;
            }
        }
        public sealed class TakeSnapshot
        {
            public readonly object Data;

            public TakeSnapshot(object data)
            {
                Data = data;
            }
        }

        public sealed class TookSnapshot
        {
            public static readonly TookSnapshot Instance = new TookSnapshot();
        }

        public sealed class SnapshotFailed
        {
            public static readonly SnapshotFailed Instance = new SnapshotFailed();
        }
    }
}