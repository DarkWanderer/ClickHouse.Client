using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ClickHouse.Client
{
    internal class ClickHouseJsonReader : ClickHouseDataReader
    {
        private readonly StreamReader textReader;
        private readonly JsonTextReader jsonReader;
        private readonly JsonSerializer serializer = new JsonSerializer();
        private bool skipAdvancing = false;

        public ClickHouseJsonReader(HttpResponseMessage httpResponse) : base(httpResponse) 
        {
            textReader = new StreamReader(httpResponse.Content.ReadAsStreamAsync().GetAwaiter().GetResult());
            jsonReader = new JsonTextReader(textReader) { SupportMultipleContent = true, CloseInput = false };
            ReadHeaders();
        }

        public override bool HasRows => skipAdvancing || textReader.Peek() != -1;

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
            CurrentRow = FieldNames.Select(fn => dictionary.GetValueOrDefault(fn, null)).ToArray();

            return true;
        }

        private void ReadHeaders()
        {
            if (!jsonReader.Read())
                return;
            var dictionary = serializer.Deserialize<Dictionary<string, object>>(jsonReader);
            FieldNames = dictionary.Keys.ToArray();

            CurrentRow = FieldNames.Select(fn => dictionary.GetValueOrDefault(fn, null)).ToArray();
            FieldTypes = CurrentRow.Select(v => v?.GetType() ?? typeof(object)).ToArray();
            skipAdvancing = true;
        }

    }
}