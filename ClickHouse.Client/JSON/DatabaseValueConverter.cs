using System;
using System.Collections.Generic;
using System.Globalization;
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
            if (reader.TokenType == JsonToken.Null)
                return DBNull.Value;

            if (reader.TokenType == JsonToken.StartObject)
                return base.ReadJson(reader, objectType, existingValue, serializer);

            if (reader.TokenType == JsonToken.StartArray)
            {
                var array = JToken.ReadFrom(reader).ToObject<object[]>();
                for (var i = 0; i < array.Length; i++) // Hack, TODO: rewrite properly
                {
                    if (array[i] == null)
                        array[i] = DBNull.Value;
                }

                return array;
            }

            if (reader.TokenType == JsonToken.String)
            {
                var token = JToken.ReadFrom(reader).ToString();
                if (Guid.TryParse(token, out var guid))
                    return guid;
                if (DateTime.TryParseExact(token, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateTime))
                    return dateTime;
                if (DateTime.TryParseExact(token, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
                    return date;
                return token;
            }

            // if the next token is not an object
            // then fall back on standard deserializer (strings, numbers etc.)
            return serializer.Deserialize(reader);
        }
    }
}