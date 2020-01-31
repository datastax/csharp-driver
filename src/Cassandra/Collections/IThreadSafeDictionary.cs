//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;

namespace Cassandra.Collections
{
    internal interface IThreadSafeDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        TValue GetOrAdd(TKey key, TValue value);

        bool TryRemove(TKey key, out TValue value);

        TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory);

        TValue AddOrUpdate(
            TKey key,
            Func<TKey, TValue> addValueFactory,
            Func<TKey, TValue, TValue> updateValueFactory);

        /// <summary>
        /// Calls <paramref name="updateValueFactory"/> and updates an existing key
        /// only if <paramref name="compareFunc"/> returns true.
        /// </summary>
        /// <param name="key">Key used to fetch/update values in the dictionary.</param>
        /// <param name="compareFunc">Parameters are the key and the existing value. Returned bool determines
        /// whether <paramref name="updateValueFactory"/> is called and the map is updated.</param>
        /// <param name="updateValueFactory">Factory that will be invoked to modify the existing value.
        /// The existing value will be replaced with the output from this factory.</param>
        TValue CompareAndUpdate(
            TKey key,
            Func<TKey, TValue, bool> compareFunc,
            Func<TKey, TValue, TValue> updateValueFactory);
    }
}