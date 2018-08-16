using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;

namespace Hafslund.Akka.Persistence
{
    public static class BigtableExtensions
    {
        public static async Task<IEnumerable<Row>> ReadClosedRowRangeAsync(this BigtableClient BigtableClient, TableName tableName, BigtableByteString from, BigtableByteString to)
        {
            var result = new List<Row>();

            if (from > to)
            {
                return result;
            }

            RowSet rowSet;
            if (from.Equals(to))
            {
                rowSet = RowSet.FromRowKey(from);
            }
            else
            {
                rowSet = RowSet.FromRowRanges(RowRange.Closed(from, to));
            }

            var stream = BigtableClient.ReadRows(tableName, rows: rowSet);

            using (var enumerator = stream.GetEnumerator())
            {
                while (await enumerator.MoveNext().ConfigureAwait(false))
                {
                    result.Add(enumerator.Current);
                }
            }

            return result;
        }
    }
}