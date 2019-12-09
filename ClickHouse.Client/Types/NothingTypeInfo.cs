using System;

namespace ClickHouse.Client.Types
{
    internal class NothingTypeInfo : ClickHouseTypeInfo
    {
        public override ClickHouseTypeCode DataType => ClickHouseTypeCode.Nothing;

        public override Type EquivalentType => typeof(DBNull);

        public override string ToString() => "Nothing";
    }
}
