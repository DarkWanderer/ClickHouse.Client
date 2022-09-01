using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Properties;
using ClickHouse.Client.Types;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Copy
{
    public class ClickHouseBulkCopySerializationException : Exception
    {
        public ClickHouseBulkCopySerializationException(object[] row, int index, Exception innerException)
            : base("Error when serializing data", innerException)
        {
            Row = row;
            Index = index;
        }

        /// <summary>
        /// Gets row at which exception happened
        /// </summary>
        public object[] Row { get; }

        /// <summary>
        /// Gets index of bad value in row
        /// </summary>
        public int Index { get; }
    }
}
