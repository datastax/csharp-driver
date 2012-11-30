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

        public string body;
    }

    public class TweetsContext : CqlContext
    {
        public TweetsContext(CassandraCluster cluster, string keyspaceName, CqlConsistencyLevel ReadCqlConsistencyLevel, CqlConsistencyLevel WriteCqlConsistencyLevel)
        {
            try
            {
                Initialize(cluster.connect(keyspaceName), true, ReadCqlConsistencyLevel, WriteCqlConsistencyLevel);
            }
            catch (CassandraConnectionException ex)
            {
                if (ex.InnerException is CassandraClusterInvalidException)
                {
                    using (var creatorContext = new CqlContext(cluster.connect(), true, CqlConsistencyLevel.IGNORE, CqlConsistencyLevel.IGNORE))
                        creatorContext.CreateKeyspaceIfNotExists(keyspaceName);
                    Initialize(cluster.connect(keyspaceName), true, ReadCqlConsistencyLevel, WriteCqlConsistencyLevel);
                }
                else
                    throw;
            }

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

        public void Drop()
        {
            DeleteKeyspaceIfExists(this.Keyspace);
        }

    }
}
