using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using ClickHouse.Client.Formats;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Types;

internal class JsonType : ClickHouseType
{
    public override Type FrameworkType => typeof(JsonObject);

    public override string ToString() => "Json";

    public override object Read(ExtendedBinaryReader reader)
    {
        JsonObject root = new();

        var nfields = reader.Read7BitEncodedInt();
        for (int i = 0; i < nfields; i++)
        {
            var current = root;
            var name = reader.ReadString();

            var pathParts = name.Split('.');
            foreach (var part in pathParts.SkipLast1(1))
            {
                if (current.ContainsKey(part))
                {
                    current = (JsonObject)current[part];
                }
                else
                {
                    var newCurrent = new JsonObject();
                    current.Add(part, newCurrent);
                    current = newCurrent;
                }
            }
            current[pathParts.Last()] = ReadJsonNode(reader);
        }
        return root;
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        JsonObject rootObject;
        if (value is string inputString)
        {
            rootObject = (JsonObject)JsonNode.Parse(inputString);
        }
        else if (value is JsonObject inputObject)
        {
            rootObject = inputObject;
        }
        else
        {
            rootObject = (JsonObject)JsonSerializer.SerializeToNode(value);
        }


        // Simple depth-first search to flatten the JSON object into a dictionary
        WriteJsonObject(writer, rootObject);
    }

    internal static void WriteJsonObject(ExtendedBinaryWriter writer, JsonObject rootObject)
    {
        Dictionary<string, JsonNode> fields = new();
        StringBuilder currentPath = new();
        FlattenJson(rootObject, ref currentPath, ref fields);

        writer.Write7BitEncodedInt(fields.Count);
        foreach (var field in fields)
        {
            writer.Write(field.Key);
            WriteJsonNode(writer, field.Value);
        }
    }

    internal static void FlattenJson(JsonObject parent, ref StringBuilder currentPath, ref Dictionary<string, JsonNode> fields)
    {
        foreach (var property in parent)
        {
            var pathLengthBefore = currentPath.Length;
            if (currentPath.Length > 0)
                currentPath.Append('.');
            currentPath.Append(property.Key);

            if (property.Value is JsonObject jObject)
            {
                FlattenJson(jObject, ref currentPath, ref fields);
            }
            else if (property.Value is null || property.Value.GetValueKind() == JsonValueKind.Null)
            {
                fields[currentPath.ToString()] = null;
            }
            else
            {
                fields[currentPath.ToString()] = property.Value;
            }
            currentPath.Length = pathLengthBefore;
        }
    }

    internal static IEnumerable<JsonNode> LeafNodes(JsonNode node)
    {
        if (node is JsonObject jObject)
        {
            foreach (var property in jObject)
            {
                if (property.Value is JsonObject)
                {
                    foreach (var child in LeafNodes(property.Value))
                        yield return child;
                }
                else
                {
                    yield return property.Value;
                }
            }
        }
        else if (node is JsonArray jArray)
        {
            yield return jArray;
        }
        else
        {
            yield break;
        }
    }

    internal static JsonNode ReadJsonNode(ExtendedBinaryReader reader)
    {
        var type = TypeConverter.FromByteCode(reader);
        if (type is ArrayType at)
        {
            var count = reader.Read7BitEncodedInt();
            var array = new JsonArray();
            for (int i = 0; i < count; i++)
            {
                array.Add(at.UnderlyingType.Read(reader));
            }
            return array;
        }
        else
        {
            var value = type.Read(reader);
            if (value is DBNull)
                value = null;
            return JsonValue.Create(JsonSerializer.SerializeToElement(value));
        }
    }

    internal static void WriteJsonNode(ExtendedBinaryWriter writer, JsonNode node)
    {
        switch (node)
        {
            case JsonArray array:
                WriteJsonArray(writer, array);
                break;
            case JsonValue value:
                WriteJsonValue(writer, value);
                break;
            case null:
                writer.Write((byte)0x00);
                break;
            default:
                throw new SerializationException($"Unsupported JSON node type: {node.GetType()}");
        }
    }

    internal static void WriteJsonArray(ExtendedBinaryWriter writer, JsonArray array)
    {
        writer.Write((byte)0x1E);

        var kind = array.Count > 0 ? array[0].GetValueKind() : JsonValueKind.Null;

        // Step 1: Write binary tag for array element type
        switch (kind)
        {
            case JsonValueKind.Undefined:
            case JsonValueKind.String:
                writer.Write((byte)0x15);
                break;
            case JsonValueKind.Number:
                writer.Write((byte)0x0E);
                break;
            case JsonValueKind.False:
            case JsonValueKind.True:
                writer.Write((byte)0x01);
                break;
            case JsonValueKind.Null:
                writer.Write((byte)0x00);
                break;
            case JsonValueKind.Object:
                writer.Write((byte)0x30);
                writer.Write((byte)0);
                writer.Write7BitEncodedInt(256);
                writer.Write((int)16);
                break;
            default:
                throw new SerializationException($"Unsupported JSON value kind: {kind}");
        }

        // Step 2: Write array length
        writer.Write7BitEncodedInt(array.Count);

        // Step 3: Write array elements
        foreach (var value in array)
        {
            if (value.GetValueKind() != kind)
            {
                throw new SerializationException("Array contains mixed value types");
            }

            switch (kind)
            {
                case JsonValueKind.Undefined:
                case JsonValueKind.String:
                    writer.Write(value.ToString());
                    break;
                case JsonValueKind.Number:
                    writer.Write(value.GetValue<double>());
                    break;
                case JsonValueKind.False:
                case JsonValueKind.True:
                    writer.Write(value.GetValue<bool>());
                    break;
                case JsonValueKind.Null:
                    writer.Write((byte)0x00);
                    break;
                case JsonValueKind.Object:
                    WriteJsonObject(writer, (JsonObject)value);
                    break;
                default:
                    throw new SerializationException($"Unsupported JSON value kind: {value.GetValueKind()}");
            }
        }
    }

    internal static void WriteJsonValue(ExtendedBinaryWriter writer, JsonValue value)
    {
        switch (value.GetValueKind())
        {
            case JsonValueKind.Undefined:
            case JsonValueKind.String:
                writer.Write((byte)0x15);
                writer.Write(value.ToString());
                break;
            case JsonValueKind.Number:
                writer.Write((byte)0x0E);
                writer.Write(value.GetValue<double>());
                break;
            case JsonValueKind.False:
            case JsonValueKind.True:
                writer.Write((byte)0x2D);
                writer.Write(value.GetValue<bool>());
                break;
            case JsonValueKind.Null:
                writer.Write((byte)0x00);
                break;
            default:
                throw new SerializationException($"Unsupported JSON value kind: {value.GetValueKind()}");
        }
    }
}
