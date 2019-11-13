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
using System.Collections.ObjectModel;

namespace Cassandra.Mapping.Utils
{
    /// <summary>
    /// A concrete implementation of System.Collections.ObjectModel.KeyedCollection which acts as a List/Dictionary hybrid where
    /// the Dictionary key is embedded in the item and list order is preserved.
    /// </summary>
    internal class LookupKeyedCollection<TKey, TItem> : KeyedCollection<TKey, TItem>
    {
        private readonly Func<TItem, TKey> _getKeyFunc;

        public LookupKeyedCollection(Func<TItem, TKey> getKeyFunc)
        {
            _getKeyFunc = getKeyFunc;
        }

        public LookupKeyedCollection(Func<TItem, TKey> getKeyFunc, IEqualityComparer<TKey> keyComparer) 
            : base(keyComparer)
        {
            _getKeyFunc = getKeyFunc;
        }

        protected override TKey GetKeyForItem(TItem item)
        {
            return _getKeyFunc(item);
        }

        public bool TryGetItem(TKey key, out TItem item)
        {
            if (Dictionary == null)
            {
                item = default(TItem);
                return false;
            }

            return Dictionary.TryGetValue(key, out item);
        }
    }
}