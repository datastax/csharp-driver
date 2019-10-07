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

using System.Collections;
using System.Collections.Generic;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Contains methods to use as Linq operators.
    /// </summary>
    public static class CqlOperator
    {
        /// <summary>
        /// Represents the CQL add assign (+=) operator for collections
        /// </summary>
        public static T Append<T>(T value) where T: IEnumerable
        {
            return default(T);   
        }

        /// <summary>
        /// Represents the CQL prepend operator for collections (col1 = ? + col1)
        /// </summary>
        public static T Prepend<T>(T value) where T: IEnumerable
        {
            return default(T);
        }

        /// <summary>
        /// Represents the CQL operator to remove an item from lists and sets (col1 = col1 - ?).
        /// </summary>
        public static T SubstractAssign<T>(T value) where T: IEnumerable
        {
            return default(T);
        }

        /// <summary>
        /// Represents the CQL operator to remove an item from a map (col1 = col1 - ?).
        /// </summary>
        public static Dictionary<TKey, TValue> SubstractAssign<TKey, TValue>(this Dictionary<TKey, TValue> map,
                                                                             params TKey[] value)
        {
            return null;
        }

        /// <summary>
        /// Represents the CQL operator to remove an item from a map (col1 = col1 - ?).
        /// </summary>
        public static IDictionary<TKey, TValue> SubstractAssign<TKey, TValue>(this IDictionary<TKey, TValue> map,
                                                                              params TKey[] value)
        {
            return null;
        }

        /// <summary>
        /// Represents the CQL operator to remove an item from a map (col1 = col1 - ?).
        /// </summary>
        public static SortedDictionary<TKey, TValue> SubstractAssign<TKey, TValue>(
            this SortedDictionary<TKey, TValue> map, params TKey[] value)
        {
            return null;
        }
    }
}
