using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Nodes;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Types.Grammar;

namespace ClickHouse.Client.Types;

internal class JsonType : ParameterizedType
{
    public override Type FrameworkType => typeof(string);

    public override string Name => "Json";

    public override object Read(ExtendedBinaryReader reader)
    {
        var value = new JsonObject();
        var nfields = reader.Read7BitEncodedInt();

        for (int i = 0; i < nfields; i++)
        {
            var fieldName = reader.ReadString();
            // See https://github.com/ClickHouse/ClickHouse/blob/b618fe03bf96e64bea1a1bdec01adc1c00cd61fb/src/DataTypes/DataTypesBinaryEncoding.cpp#L48
            // https://clickhouse.com/docs/en/sql-reference/data-types/data-types-binary-encoding
            // TODO: add type codes within types themselves
            var typeCode = reader.Read7BitEncodedInt();
            object fieldValue = typeCode switch
            {
                0x07 => reader.ReadByte(),
                0x08 => reader.ReadInt16(),
                0x09 => reader.ReadInt32(),
                0x0a => reader.ReadInt64(),
                0x0e => reader.ReadDouble(),
                0x15 => reader.ReadString(),
                _ => throw new ArgumentException(fieldName, $"Unknown type code: {typeCode:X}")
            };
            value[fieldName] = JsonValue.Create(fieldValue);
        }
        return value.ToJsonString();
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        JsonElement element;
        if (value is string stringValue)
        {
            element = JsonDocument.Parse(stringValue).RootElement;
        }
        else
        {
            element = JsonSerializer.SerializeToElement(value);
        }

        writer.Write7BitEncodedInt(element.EnumerateObject().Count());
        foreach (var field in element.EnumerateObject())
        {
            writer.Write(field.Name);
            switch (field.Value.ValueKind)
            {
                case JsonValueKind.Number:
                    if (field.Value.TryGetInt64(out var int64Value))
                    {
                        writer.Write((byte)0x0a);
                        writer.Write(int64Value);
                    }
                    else
                    {
                        writer.Write((byte)0x0e);
                        writer.Write(field.Value.GetDouble());
                    }
                    break;
                case JsonValueKind.String:
                    writer.Write((byte)0x15);
                    writer.Write(field.Value.GetString());
                    break;
                case JsonValueKind.True:
                    writer.Write((byte)0x07);
                    writer.Write((byte)1);
                    break;
                case JsonValueKind.False:
                    writer.Write((byte)0x07);
                    writer.Write((byte)0);
                    break;
                default:
                    throw new ArgumentException(field.Name, $"Unknown JSON value kind: {field.Value.ValueKind}");
            }
        }
    }

    public override string ToString() => Name;

    public override ParameterizedType Parse(SyntaxTreeNode typeName, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        if (typeName.ChildNodes.Any())
        {
            throw new SerializationException("JSON type does not accept parameters");
        }

        return this;
    }
}
