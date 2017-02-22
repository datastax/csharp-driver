//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    internal class SchemaChangeEventArgs : CassandraEventArgs
    {
        public enum Reason
        {
            Created,
            Updated,
            Dropped
        };

        /// <summary>
        /// The keyspace affected
        /// </summary>
        public string Keyspace { get; set; }

        /// <summary>
        /// The table affected
        /// </summary>
        public string Table { get; set; }

        /// <summary>
        /// The type of change in the schema object
        /// </summary>
        public Reason What { get; set; }

        /// <summary>
        /// The custom type affected
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// Name of the Cql function affected
        /// </summary>
        public string FunctionName { get; set; }

        /// <summary>
        /// Name of the aggregate affected
        /// </summary>
        public string AggregateName { get; set; }

        /// <summary>
        /// Signature of the function or aggregate
        /// </summary>
        public string[] Signature { get; set; }
    }
}