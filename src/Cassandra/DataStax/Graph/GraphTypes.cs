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

namespace Cassandra.DataStax.Graph
{
    public static class GraphTypes
    {
        public static TinkerpopTimestamp AsTinkerpopTimestamp(DateTimeOffset dt)
        {
            return new TinkerpopTimestamp(dt);
        }
        
        public static TinkerpopDate AsTinkerpopDate(DateTimeOffset dateTimeOffset)
        {
            return new TinkerpopDate(dateTimeOffset);
        }

        /// <summary>
        /// Note that T should implement Equals and GetHashCode so that the set implementation
        /// can detect which elements are duplicates.
        /// </summary>
        public static ISet<T> AsSet<T>(IEnumerable<T> enumerable)
        {
            return new HashSet<T>(enumerable);
        }
    }
}