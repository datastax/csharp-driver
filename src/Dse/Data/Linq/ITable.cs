//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Linq;

namespace Cassandra.Data.Linq
{
    public interface ITable : IQueryProvider
    {
        void Create();
        Type GetEntityType();
        /// <summary>
        /// Gets the table name in Cassandra
        /// </summary>
        string Name { get; }
        /// <summary>
        /// Gets the name of the keyspace used. If null, it uses the active session keyspace.
        /// </summary>
        string KeyspaceName { get; }
        ISession GetSession();
        TableType GetTableType();
    }
}