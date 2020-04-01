//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Dse.Mapping.Utils;

namespace Dse.Mapping
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