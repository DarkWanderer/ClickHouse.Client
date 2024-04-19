using System;
using System.Collections.Concurrent;

namespace ClickHouse.Client.Utility;
internal static class EnumToLowercaseStringCached<T>
    where T : Enum
{
    private static readonly ConcurrentDictionary<T, string> Values = new ConcurrentDictionary<T, string>();

    public static string ToString(T value)
    {
        return Values.GetOrAdd(value, (v) => v.ToString().ToLowerInvariant());
    }
}
