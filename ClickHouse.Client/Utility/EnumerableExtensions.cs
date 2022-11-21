using System;
using System.Collections.Generic;

namespace ClickHouse.Client.Utility;

public static class EnumerableExtensions
{
    public static void Deconstruct<T>(this IList<T> list, out T first, out T second)
    {
        if (list.Count != 2)
            throw new ArgumentException($"Expected 2 elements in list, got {list.Count}");
        first = list[0];
        second = list[1];
    }

    public static void Deconstruct<T>(this IList<T> list, out T first, out T second, out T third)
    {
        if (list.Count != 3)
            throw new ArgumentException($"Expected 3 elements in list, got {list.Count}");
        first = list[0];
        second = list[1];
        third = list[2];
    }
}
