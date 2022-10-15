using System;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Client.Utility
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<IList<T>> Batch<T>(this IEnumerable<T> source, int size)
        {
            T[] bucket = null;
            var count = 0;

            foreach (var item in source)
            {
                bucket ??= new T[size];

                bucket[count++] = item;

                if (count != size)
                {
                    continue;
                }

                yield return bucket;

                bucket = null;
                count = 0;
            }

            // Return the last bucket with all remaining elements
            if (bucket != null && count > 0)
            {
                Array.Resize(ref bucket, count);
                yield return bucket;
            }
        }

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
}
