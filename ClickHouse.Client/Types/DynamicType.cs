using System;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types;

internal class DynamicType : ClickHouseType
{
    public override Type FrameworkType => typeof(object);

    public override string ToString() => "Dynamic";

    public override object Read(ExtendedBinaryReader reader) =>
        TypeConverter.
            FromByteCode(reader).
            Read(reader);

    public override void Write(ExtendedBinaryWriter writer, object value) =>
        throw new NotImplementedException();
}