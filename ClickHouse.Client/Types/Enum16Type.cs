using System;

namespace ClickHouse.Client.Types
{
    internal class Enum16Type : EnumType
    {
        public override string Name => "Enum16";

        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Enum16;
    }
}
