//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra
{
    /// <summary>
    /// Describes a materialized view in Cassandra. 
    /// </summary>
    public class MaterializedViewMetadata : DataCollectionMetadata
    {
        /// <summary>
        /// Gets the view where clause
        /// </summary>
        public string WhereClause { get; protected set; }

        protected MaterializedViewMetadata()
        {
            
        }

        internal MaterializedViewMetadata(string name, string whereClause)
        {
            Name = name;
            WhereClause = whereClause;
        }
    }
}