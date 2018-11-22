//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Dse
{
    /// <summary>
    /// Describes a Cassandra table
    /// </summary>
    public class TableMetadata: DataCollectionMetadata
    {
        private static readonly IDictionary<string, IndexMetadata> EmptyIndexes =
            new ReadOnlyDictionary<string, IndexMetadata>(new Dictionary<string, IndexMetadata>());

        /// <summary>
        /// Gets the table indexes by name
        /// </summary>
        public IDictionary<string, IndexMetadata> Indexes { get; protected set; }

        /// <summary>
        /// Determines whether the table is a virtual table or not.
        /// </summary>
        public bool IsVirtual { get; protected set; }

        protected TableMetadata()
        {
            
        }

        internal TableMetadata(string name, IDictionary<string, IndexMetadata> indexes, bool isVirtual = false)
        {
            Name = name;
            Indexes = indexes ?? EmptyIndexes;
            IsVirtual = isVirtual;
        }
    }
}
