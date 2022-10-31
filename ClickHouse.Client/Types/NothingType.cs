using System;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types;

internal class NothingType : ClickHouseType
{
    public override Type FrameworkType => typeof(DBNull);

    public override object Read(ExtendedBinaryReader reader) => DBNull.Value;

    public override string ToString() => "Nothing";

    public override void Write(ExtendedBinaryWriter writer, object value) { }
}
