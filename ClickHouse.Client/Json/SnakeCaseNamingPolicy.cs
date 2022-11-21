using System.Text.Json;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.Json;

internal class SnakeCaseNamingPolicy : JsonNamingPolicy
{
    public static SnakeCaseNamingPolicy Instance { get; } = new SnakeCaseNamingPolicy();

    public override string ConvertName(string name)
    {
        return name.ToSnakeCase();
    }
}
