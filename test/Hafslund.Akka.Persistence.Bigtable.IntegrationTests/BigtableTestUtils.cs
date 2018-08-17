using System.Collections.Generic;
using System.Linq;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;

namespace Hafslund.Akka.Persistence.Bigtable.IntegrationTests
{
    public static class BigtableTestUtils
    {
        public static void DeleteRows(string tableName, RowRange rowRange)
        {
            var bigtableClient = BigtableClient.Create();
            var stream = bigtableClient.ReadRows(Google.Cloud.Bigtable.Common.V2.TableName.Parse(tableName), RowSet.FromRowRanges(rowRange));

            var deleteRows = new List<MutateRowsRequest.Types.Entry>();

            using (var enumerator = stream.GetEnumerator())
            {
                while (enumerator.MoveNext().GetAwaiter().GetResult())
                {
                    deleteRows.Add(Mutations.CreateEntry(enumerator.Current.Key, Mutations.DeleteFromRow()));
                }
            }

            if (deleteRows.Any())
            {
                bigtableClient.MutateRows(Google.Cloud.Bigtable.Common.V2.TableName.Parse(tableName), deleteRows);
            }
        }
    }
}