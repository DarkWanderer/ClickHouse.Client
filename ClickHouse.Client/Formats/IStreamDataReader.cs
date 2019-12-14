using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.Types;

namespace ClickHouse.Client.Formats
{

    interface IStreamDataReader
    {
        object ReadValue(ClickHouseType type);
    }
}
