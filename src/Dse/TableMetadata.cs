//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//


using System.Collections.Generic;
using System.Linq;

namespace Dse
{
    /// <summary>
    /// Describes a Cassandra table
    /// </summary>
    public class TableMetadata: DataCollectionMetadata
    {
        /// <summary>
        /// Gets the table indexes by name
        /// </summary>
        public IDictionary<string, IndexMetadata> Indexes { get; protected set; }

        protected TableMetadata()
        {
            
        }

        internal TableMetadata(string name, IDictionary<string, IndexMetadata> indexes)
        {
            Name = name;
            Indexes = indexes;
        }
    }
}
