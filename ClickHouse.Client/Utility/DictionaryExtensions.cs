using System.Collections.Generic;

namespace ClickHouse.Client.Utility
{
    public static class DictionaryExtensions
    {
        public static bool TryAdd<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue value)
        {
            if (dictionary.ContainsKey(key))
                return false;
            dictionary.Add(key, value);
            return true;
        }
    }
}
