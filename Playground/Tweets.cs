using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data;
using Cassandra;
using Cassandra.Native;

namespace Playground
{
    public class Tweets
    {
        [PartitionKey]
        public Guid tweet_id;

        [SecondaryIndex]
        public string author;

        public HashSet<string> body;
    }

    public class TweetsContext : CqlContext
    {
        public TweetsContext(CassandraSession session, CqlConsistencyLevel ReadCqlConsistencyLevel, CqlConsistencyLevel WriteCqlConsistencyLevel)
            :base(session,ReadCqlConsistencyLevel,WriteCqlConsistencyLevel)
        {
            AddTable<Tweets>();
            CreateTablesIfNotExist();
        }

        //public TweetsContext(string connectionString, string keyspaceName = null)
        //    : base(connectionString, keyspaceName, false)
        //{
        //    try
        //    {
        //        Connect();
        //    }
        //    catch (CassandraConnectionException ex)
        //    {
        //        if (ex.InnerException is CassandraClusterInvalidException)
        //        {
        //            using (var creatorContext = new CqlContext(connectionString, "", true))
        //                creatorContext.CreateKeyspaceIfNotExists(Keyspace);
        //            Connect();
        //        }
        //        else
        //            throw;
        //    }

        //    AddTable<Tweets>();
        //    CreateTablesIfNotExist();
        //}

    }
}
