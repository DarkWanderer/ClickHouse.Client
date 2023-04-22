using System;
using System.Text.Json;
using ClickHouse.Client.Formats;

namespace ClickHouse.Client.Types;

internal class JsonType : ClickHouseType
{
    public override Type FrameworkType => typeof(string);

    public override object Read(ExtendedBinaryReader reader) => reader.ReadString();

    public override string ToString() => "Json";

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is null) throw new ArgumentNullException(nameof(value));
        switch (value)
        {
            case string s:
                writer.Write(s); break;
            case JsonElement e:
                writer.Write(e.ToString()); break;
            default:
                throw new NotImplementedException($"Cannot convert {value.GetType()} to Json");
        }
    }
}
