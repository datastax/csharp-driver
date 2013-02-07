using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra;

namespace LinqSamples
{
    class Program
    {
        [AllowFiltering]
        public class NerdMovie
        {
            [ClusteringKey(1)]
            public string Director;

            public string MainActor;

            [PartitionKey]
            public string Movie;

            public int Year;
        }
        
        static void Main(string[] args)
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("cassi.cloudapp.net").WithoutRowSetBuffering().Build();

            using (var session = cluster.Connect())
            {
                const string keyspaceName = "Excelsior";

                try
                {
                    session.ChangeKeyspace(keyspaceName);
                }
                catch (InvalidException)
                {
                    session.CreateKeyspaceIfNotExists(keyspaceName);
                    session.ChangeKeyspace(keyspaceName);
                }


                var context = new Context(session);
                context.AddTable<NerdMovie>();
                context.CreateTablesIfNotExist();

                var movies = new List<NerdMovie>()
                {
                    new NerdMovie(){ Movie = "Serenity", Director = "Joss Whedon", MainActor = "Nathan Fillion", Year = 2005},
                    new NerdMovie(){ Movie = "Pulp Fiction", Director = "Quentin Tarantino", MainActor = "Bruce Willis", Year = 2001},
                };

                var ins = context.GetTable<NerdMovie>().Insert(new NerdMovie() { Movie = "Serenity", Director = "Joss Whedon", MainActor = "Nathan Fillion", Year = 2005 });

                context.AppendCommand(ins);
            }
        }
    }
}