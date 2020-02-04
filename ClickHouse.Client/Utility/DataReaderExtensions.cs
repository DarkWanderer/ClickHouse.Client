using System.Collections.Generic;
using System.Data;

namespace ClickHouse.Client.Utility
{
    public static class DataReaderExtensions
    {
        public static IReadOnlyCollection<string> GetColumnNames(this IDataReader reader)
        {
            var count = reader.FieldCount;
            var names = new string[count];
            for (int i = 0; i < count; i++)
                names[i] = reader.GetName(i);
            return names;
        }
    }
}
