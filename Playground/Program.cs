using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Native;
using System.Net;
using System.Threading;
using System.Globalization;
using Cassandra.Data;
using Cassandra;

namespace Playground
{
    class Program
    {
        static void Main(string[] args)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");

            CassandraCluster cluster = CassandraCluster.builder().addContactPoint("168.63.107.22").withPort(9042).build();

            //CqlConnectionStringBuilder csb = new CqlConnectionStringBuilder(
            //    Keyspace: "test"+Guid.NewGuid().ToString("N"),
            //    ContactPoints: new IPEndPoint[] { 
            //        new IPEndPoint(IPAddress.Parse("168.63.107.22"), 9042) 
            //    },
            //    ReadCqlConsistencyLevel: CqlConsistencyLevel.ONE,
            //    WriteCqlConsistencyLevel: CqlConsistencyLevel.ANY,
            //    ConnectionTimeout: 1000000,
            //    CompressionType: CassandraCompressionType.NoCompression,
            //    MaxPoolSize: 100,
            //    Username: "guest",
            //    Password: "guest"
            //    );

            var session = cluster.connect();

            var keyspaceName = "test" + Guid.NewGuid().ToString("N");

            try
            {
                session.ChangeKeyspace(keyspaceName);
            }
            catch (CassandraClusterInvalidException ex)
            {
                session.CreateKeyspaceIfNotExists(keyspaceName);
                session.ChangeKeyspace(keyspaceName);
            }


            TweetsContext tweets = new TweetsContext(session, CqlConsistencyLevel.ONE, CqlConsistencyLevel.ANY);

            var table = tweets.GetTable<Tweets>();

            int RowsNo = 2000;
            List<Tweets> entL = new List<Tweets>();
            for (int i = 0; i < RowsNo; i++)
            {
                var ent = new Tweets() { tweet_id = Guid.NewGuid(), author = "test" + i.ToString(), body = new HashSet<string>() { "body" + i.ToString() } };
                table.AddNew(ent, CqlEntityTrackingMode.KeepAtachedAfterSave);
                entL.Add(ent);
            }
            tweets.SaveChanges(CqlSaveChangesMode.Batch);

            var cnt = table.Count().Execute();


            foreach (var auth in (from r in table select r.author).Execute())
            {
                foreach (var auth2 in (from r in table select r.author).Execute())
                {
                    Console.WriteLine(auth);
                }
            }

            foreach (var ent in entL)
                table.Delete(ent);

            tweets.SaveChanges(CqlSaveChangesMode.Batch);

            var cnt2 = table.Count().Execute();

            session.DeleteKeyspaceIfExists(keyspaceName);

            Console.WriteLine("Done!");
            Console.ReadKey();
        }
    }
}
