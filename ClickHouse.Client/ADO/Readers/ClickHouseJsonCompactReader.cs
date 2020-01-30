﻿using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using ClickHouse.Client.JSON;
using ClickHouse.Client.Types;
using Newtonsoft.Json;

namespace ClickHouse.Client.ADO.Readers
{
    internal class ClickHouseJsonCompactReader : ClickHouseDataReader
    {
        private readonly StreamReader textReader;
        private readonly JsonTextReader jsonReader;
        private readonly JsonSerializer serializer = new JsonSerializer();
        private bool hasMore = false;

        public ClickHouseJsonCompactReader(HttpResponseMessage httpResponse) : base(httpResponse)
        {
            textReader = new StreamReader(httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult());
            jsonReader = new JsonTextReader(textReader) { SupportMultipleContent = true, CloseInput = false };
            serializer.Converters.Add(new DatabaseValueConverter());
            serializer.DateFormatString = "";
            ReadHeaders();
        }

        private void ReadHeaders()
        {
            // Read starting tag
            AssertEquals(true, jsonReader.Read());
            AssertEquals(JsonToken.StartObject, jsonReader.TokenType);

            // Read 'meta' property
            AssertEquals(true, jsonReader.Read());
            AssertEquals(JsonToken.PropertyName, jsonReader.TokenType);
            AssertEquals("meta", jsonReader.Value);
            AssertEquals(true, jsonReader.Read());
            var columns = serializer.Deserialize<JsonColumnRecord[]>(jsonReader);

            FieldNames = new string[columns.Length];
            RawTypes = new ClickHouseType[columns.Length];

            for (var i = 0; i < columns.Length; i++)
            {
                FieldNames[i] = columns[i].Name;
                RawTypes[i] = TypeConverter.ParseClickHouseType(columns[i].Type);
            }

            // Read start of 'data' property
            AssertEquals(true, jsonReader.Read());
            AssertEquals(JsonToken.PropertyName, jsonReader.TokenType);
            AssertEquals("data", jsonReader.Value);

            // Read start of data array tag
            AssertEquals(true, jsonReader.Read());
            AssertEquals(JsonToken.StartArray, jsonReader.TokenType);
            AssertEquals(true, jsonReader.Read());
            hasMore = jsonReader.TokenType == JsonToken.StartArray;
        }

        [JsonObject]
        private class JsonColumnRecord
        {
            [JsonProperty("name")]
            public readonly string Name;

            [JsonProperty("type")]
            public readonly string Type;

            [JsonConstructor]
            public JsonColumnRecord(string name, string type)
            {
                Name = name;
                Type = type;
            }
        }

        private void AssertEquals<T>(T expected, T actual)
        {
            Debug.Assert(Equals(expected, actual));
            if (!Equals(expected, actual))
                throw new InvalidOperationException($"Error: expected {expected}, got {actual}");
        }

        /// <summary>
        /// Streams 
        /// </summary>
        /// <returns>Whether read was successful</returns>
        public override bool Read()
        {
            if (!hasMore)
                return false;
            if (jsonReader.TokenType == JsonToken.EndArray)
                return (hasMore = false);
            AssertEquals(jsonReader.TokenType, JsonToken.StartArray);
            CurrentRow = serializer.Deserialize<object[]>(jsonReader);
            AssertEquals(JsonToken.EndArray, jsonReader.TokenType);
            AssertEquals(true, jsonReader.Read());
            hasMore = jsonReader.TokenType != JsonToken.EndArray;

            // Convert arrays to tuples
            //for (int i = 0; i < FieldCount; i++)
            //{
            //    if (RawTypes[i].TypeCode == ClickHouseTypeCode.Tuple)
            //    {
            //        var tt = (TupleType)RawTypes[i];
            //        CurrentRow[i] = tt.MakeTuple((object[])CurrentRow[i]);
            //    }
            //}

            return true;
        }
    }
}