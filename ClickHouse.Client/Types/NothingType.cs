using System;

namespace ClickHouse.Client.Types
{
    internal class NothingType : ClickHouseType
    {
        public override ClickHouseTypeCode TypeCode => ClickHouseTypeCode.Nothing;

        public override Type FrameworkType => typeof(DBNull);
        
        public override string ToStringParameter(object value) => $"null";

        public override string ToString() => "Nothing";
    }
}
