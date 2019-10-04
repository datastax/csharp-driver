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

using System.Collections.Generic;

namespace Cassandra.Helpers
{
    internal static class CollectionHelpers
    {
        internal static void CreateOrAdd<TKey, TElement>(
            this IDictionary<TKey, ICollection<TElement>> dictionary, TKey key, TElement elementToAdd)
        {
            if (!dictionary.TryGetValue(key, out var collection))
            {
                collection = new List<TElement>();
                dictionary.Add(new KeyValuePair<TKey, ICollection<TElement>>(key, collection));
            }

            collection.Add(elementToAdd);
        }
        
        internal static void CreateIfDoesNotExist<TKey, TElement>(
            this IDictionary<TKey, ICollection<TElement>> dictionary, TKey key)
        {
            if (!dictionary.TryGetValue(key, out var collection))
            {
                collection = new List<TElement>();
                dictionary.Add(new KeyValuePair<TKey, ICollection<TElement>>(key, collection));
            }
        }
    }
}