using System;
using System.Buffers;
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

    public static IEnumerable<(T[], int)> BatchRented<T>(this IEnumerable<T> enumerable, int batchSize)
    {
        var array = ArrayPool<T>.Shared.Rent(batchSize);
        int counter = 0;

        foreach (var item in enumerable)
        {
            array[counter++] = item;

            if (counter >= batchSize)
            {
                yield return (array, counter);
                counter = 0;
                array = ArrayPool<T>.Shared.Rent(batchSize);
            }
        }

        if (counter > 0)
        {
            yield return (array, counter);
        }

        if (counter == 0)
        {
            ArrayPool<T>.Shared.Return(array);
        }
    }

    internal static IEnumerable<T> SkipLast1<T>(this IEnumerable<T> source, int count)
    {
        var queue = new Queue<T>();

        using (var e = source.GetEnumerator())
        {
            while (e.MoveNext())
            {
                if (queue.Count == count)
                {
                    do
                    {
                        yield return queue.Dequeue();
                        queue.Enqueue(e.Current);
                    } while (e.MoveNext());
                }
                else
                {
                    queue.Enqueue(e.Current);
                }
            }
        }
    }
}
