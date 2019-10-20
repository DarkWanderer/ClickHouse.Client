using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

namespace ClickHouse.Client.Readers
{
    internal class ClickHouseJsonReader : ClickHouseDataReader
    {
        private readonly StreamReader textReader;
        private readonly JsonTextReader jsonReader;
        private readonly JsonSerializer serializer = new JsonSerializer();
        private Type[] FieldTypes;
        private bool skipAdvancing = false;

        public ClickHouseJsonReader(HttpResponseMessage httpResponse) : base(httpResponse)
        {
            textReader = new StreamReader(httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult());
            jsonReader = new JsonTextReader(textReader) { SupportMultipleContent = true, CloseInput = false };
            serializer.Converters.Add(new DatabaseValueConverter());
            ReadHeaders();
        }

        public override bool HasRows => skipAdvancing || textReader.Peek() != -1;

        public override Type GetFieldType(int ordinal) => FieldTypes[ordinal];

        public override int FieldCount => FieldTypes.Length;

        public override bool Read()
        {
            if (skipAdvancing)
            {
                skipAdvancing = false;
                return true;
            }

            if (!jsonReader.Read())
                return false;

            var dictionary = serializer.Deserialize<Dictionary<string, object>>(jsonReader);
            CurrentRow = MapDictionaryToFields(dictionary);

            return true;
        }

        private void ReadHeaders()
        {
            if (!jsonReader.Read())
                return;
            var dictionary = serializer.Deserialize<Dictionary<string, object>>(jsonReader);
            FieldNames = dictionary.Keys.ToArray();

            CurrentRow = MapDictionaryToFields(dictionary);
            FieldTypes = CurrentRow.Select(v => v?.GetType() ?? typeof(object)).ToArray();
            skipAdvancing = true;
        }

        private object[] MapDictionaryToFields(IDictionary<string,object> dictionary)
        {
            var data = new object[FieldNames.Length];
            for (int i = 0; i < data.Length; i++)
            {
                var key = FieldNames[i];
                if (dictionary.ContainsKey(key))
                    data[i] = dictionary[key];
                else
                    data[i] = null;
            }
            return data;
        }

        private class DatabaseValueConverter : CustomCreationConverter<IDictionary<string, object>>
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

                // if the next token is not an object
                // then fall back on standard deserializer (strings, numbers etc.)
                return serializer.Deserialize(reader);
            }
        }
    }
}