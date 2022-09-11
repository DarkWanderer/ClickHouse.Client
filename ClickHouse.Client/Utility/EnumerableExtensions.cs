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
                if (bucket == null)
                {
                    bucket = new T[size];
                }

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

        public static void Deconstruct<T>(this IEnumerable<T> enumerable, out T first, out IEnumerable<T> rest)
        {
            first = enumerable.FirstOrDefault();
            rest = enumerable.Skip(1);
        }
    }
}
