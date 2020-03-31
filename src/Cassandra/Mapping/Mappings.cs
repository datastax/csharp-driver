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
using Cassandra.Mapping.Utils;

namespace Cassandra.Mapping
{
    /// <summary>
    /// A class for defining how to map multiple POCOs via a fluent-style interface.  Inheritors should use the 
    /// <see cref="For{TPoco}"/> method inside their constructor to define mappings.
    /// </summary>
    public abstract class Mappings
    {
        internal LookupKeyedCollection<Type, ITypeDefinition> Definitions;

        /// <summary>
        /// Creates a new collection of mappings.  Inheritors should define all their mappings in the constructor of the sub-class.
        /// </summary>
        protected Mappings()
        {
            Definitions = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
        }

        /// <summary>
        /// Adds a mapping for the Poco type specified (TPoco).
        /// </summary>
        public Map<TPoco> For<TPoco>()
        {
            if (Definitions.TryGetItem(typeof(TPoco), out ITypeDefinition map) == false)
            {
                map = new Map<TPoco>();
                Definitions.Add(map);
            }

            return (Map<TPoco>) map;
        }
    }
}