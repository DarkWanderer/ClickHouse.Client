using System.Collections.Generic;
using System.Collections.Specialized;

namespace ClickHouse.Client.Utility;

public static class DictionaryExtensions
{
    public static bool TryAdd<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue value)
    {
        if (dictionary.ContainsKey(key))
            return false;
        dictionary.Add(key, value);
        return true;
    }

    public static void Set<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue value)
    {
        if (dictionary.ContainsKey(key))
            dictionary[key] = value;
        else
            dictionary.Add(key, value);
    }

    public static void SetOrRemove(this IDictionary<string, string> dictionary, string key, string value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            dictionary.Set(key, value);
        }
        else
        {
            dictionary.Remove(key);
        }
    }
}
