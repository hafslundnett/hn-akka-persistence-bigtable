using System;
using System.Threading;
using Google.Api.Gax.Grpc;
using Google.Cloud.Bigtable.Admin.V2;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Grpc.Core;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests
{
    public static class BigtableTestUtils
    {
        private const string Family = "f";

        private static string ProjectId { get; set; }
        private static string InstanceId { get; set; }
        public static BigtableClient Client { get; private set; }
        private static BigtableTableAdminClient AdminClient { get; set; }

        private static TableName GetTableName(string tableName)
        {
            return new TableName(ProjectId, InstanceId, tableName);
        }

        public static void InitializeWithEmulator(string host, string projectId, string instanceId, string tableName)
        {
            var channel = new Channel(host, ChannelCredentials.Insecure);
            Client = BigtableClient.Create(BigtableServiceApiClient.Create(channel));
            AdminClient = BigtableTableAdminClient.Create(channel);
            ProjectId = projectId;
            InstanceId = instanceId;
            
            CreateTable(GetTableName(tableName));            
        }

        private static void CreateTable(TableName tableName)
        {
            Table table = null;

            try
            {
                table = AdminClient.GetTable(tableName,
                    CallSettings.FromCancellationToken(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token));
            }
            catch (RpcException e) 
            {   
                if (e.StatusCode != StatusCode.NotFound) // StatusCode.NotFound means that the table does not exist
                {
                    throw new AggregateException("Operation cancelled. Probably the bigtable emulator is not running. Run: 'gcloud beta emulators bigtable start'", e);
                }
            }

            if (table != null)
            {
                AdminClient.DeleteTable(tableName);
            }

            var createTableRequest = new CreateTableRequest
            {
                TableId = tableName.TableId,
                Table = new Table(),
                ParentAsInstanceName = new InstanceName(tableName.ProjectId, tableName.InstanceId)
            };

            createTableRequest.Table.ColumnFamilies.Add(Family, new ColumnFamily());
            AdminClient.CreateTable(createTableRequest);
        }
    }
}