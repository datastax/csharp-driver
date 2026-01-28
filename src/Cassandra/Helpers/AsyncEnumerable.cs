// Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

#if !NETFRAMEWORK && !NET10_OR_GREATER // These methods are implemented in .NET 10.
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace System.Linq
{
    internal static class AsyncEnumerable
    {
        public static async ValueTask<TSource> FirstAsync<TSource>(
            this IAsyncEnumerable<TSource> source,
            CancellationToken cancellationToken = default)
        {
            await using var e = source.GetAsyncEnumerator(cancellationToken);

            if (!await e.MoveNextAsync())
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            return e.Current;
        }

        public static async ValueTask<TSource> FirstOrDefaultAsync<TSource>(
            this IAsyncEnumerable<TSource> source,
            CancellationToken cancellationToken = default)
        {
            await using var e = source.GetAsyncEnumerator(cancellationToken);
            return await e.MoveNextAsync() ? e.Current : default;
        }

        public static async ValueTask<TSource> SingleAsync<TSource>(
            this IAsyncEnumerable<TSource> source,
            CancellationToken cancellationToken = default)
        {
            await using var e = source.GetAsyncEnumerator(cancellationToken);

            if (!await e.MoveNextAsync())
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            TSource result = e.Current;
            if (await e.MoveNextAsync())
            {
                throw new InvalidOperationException("Sequence contains more than one element");
            }

            return result;
        }

        public static async ValueTask<TSource> SingleOrDefaultAsync<TSource>(
            this IAsyncEnumerable<TSource> source,
            CancellationToken cancellationToken = default)
        {
            await using var e = source.GetAsyncEnumerator(cancellationToken);

            if (!await e.MoveNextAsync())
            {
                return default;
            }

            TSource result = e.Current;
            if (await e.MoveNextAsync())
            {
                throw new InvalidOperationException("Sequence contains more than one element");
            }

            return result;
        }

        public static async IAsyncEnumerable<TResult> Select<TSource, TResult>(
            this IAsyncEnumerable<TSource> source,
            Func<TSource, TResult> selector)
        {
            await foreach (TSource element in source)
            {
                yield return selector(element);
            }
        }

        public static async ValueTask<List<TSource>> ToListAsync<TSource>(
            this IAsyncEnumerable<TSource> source,
            CancellationToken cancellationToken = default)
        {
            var list = new List<TSource>();
            await foreach (TSource element in source.WithCancellation(cancellationToken))
            {
                list.Add(element);
            }

            return list;
        }
    }
}
#endif
