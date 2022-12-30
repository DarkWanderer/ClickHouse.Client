using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace ClickHouse.Client.Utility
{
    internal static class JsonSettings
    {
        public static readonly JsonSerializerSettings DefaultSerializerSettings = new();

        public static readonly JsonSerializerSettings SnakeCaseSerializerSettings = new()
        {
            ContractResolver = new DefaultContractResolver
            {
                NamingStrategy = new SnakeCaseNamingStrategy(),
            },
        };

        public static readonly JsonSerializer DefaultSerializer = JsonSerializer.Create(DefaultSerializerSettings);

        public static readonly JsonSerializer SnakeCaseSerializer = JsonSerializer.Create(SnakeCaseSerializerSettings);

    }
}
