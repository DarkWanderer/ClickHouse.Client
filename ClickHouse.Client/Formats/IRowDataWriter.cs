using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Client.Formats
{
    interface IRowDataWriter
    {
        void WriteRow(params object[] row);
    }
}
