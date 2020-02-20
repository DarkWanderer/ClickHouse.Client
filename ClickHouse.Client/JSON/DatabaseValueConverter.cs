using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

namespace ClickHouse.Client.JSON
{
    internal class DatabaseValueConverter : CustomCreationConverter<IDictionary<string, object>>
    {
        public override IDictionary<string, object> Create(Type objectType) => new Dictionary<string, object>();

        public override bool CanConvert(Type objectType)
        {
            // in addition to handling IDictionary<string, object>
            // we want to handle the deserialization of dict value
            // which is of type object
            return objectType == typeof(object) || base.CanConvert(objectType);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            switch (reader.TokenType)
            {
                case JsonToken.Null:
                    return DBNull.Value;
                case JsonToken.StartObject:
                    return base.ReadJson(reader, objectType, existingValue, serializer);
                case JsonToken.StartArray:
                    return JToken.ReadFrom(reader).ToObject<object[]>();
                case JsonToken.String:
                    return JToken.ReadFrom(reader).ToString();
            }

            // if the next token is not an object
            // then fall back on standard deserializer (strings, numbers etc.)
            return serializer.Deserialize(reader);
        }
    }
}