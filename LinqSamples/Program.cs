using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
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
            Cluster cluster = Cluster.Builder().WithConnectionString("Contact Points=cassi.cloudapp.net;Port=9042").Build();

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

                var table = session.GetTable<NerdMovie>();
                table.CreateIfNotExists();


                {
                    var batch = session.CreateBatch();

                    var movies = new List<NerdMovie>()
                    {
                        new NerdMovie(){ Movie = "Serenity", Director = "Joss Whedon", MainActor = "Nathan Fillion", Year = 2005},
                        new NerdMovie(){ Movie = "Pulp Fiction", Director = "Quentin Tarantino", MainActor = "John Travolta", Year = 1994},
                    };

                    batch.Append(from m in movies select table.Insert(m));

                    batch.Execute();
                }

                var testmovie = new NerdMovie { Year = 2005, Director = "Quentin Tarantino", Movie = "Pulp Fiction" };
                table.Where(m => m.Movie == testmovie.Movie && m.Director == testmovie.Director).Select(m => new NerdMovie { Year = testmovie.Year }).Update().Execute();


                var anonMovie = new { Director = "Quentin Tarantino", Year = 2005 };
                table.Where(m => m.Movie == "Pulp Fiction" && m.Director == anonMovie.Director).Select(m => new NerdMovie { Year = anonMovie.Year, MainActor = "George Clooney" }).Update().Execute();

                var nm1 = (from m in table where m.Director == "Quentin Tarantino" select new { MA = m.MainActor, Y = m.Year }).Execute().ToList();


                (from m in table where m.Movie.Equals("Pulp Fiction") && m.Director == "Quentin Tarantino" select new NerdMovie { Year = 1994 }).Update().Execute();

                table.Where((m) => m.Movie == "Pulp Fiction" && m.Director == "Quentin Tarantino").Select((m) => new NerdMovie { Year = 1994 }).Update().Execute();

                var nm2 = table.Where((m) => m.Director == "Quentin Tarantino").Select((m) => new { MA = m.MainActor, Y = m.Year }).Execute().ToList();

                (from m in table where m.Movie == "Pulp Fiction" && m.Director == "Quentin Tarantino" select m).Delete().Execute();

                var nm3 = (from m in table where m.Director == "Quentin Tarantino" select new { MA = m.MainActor, Y = m.Year }).Execute().ToList();

                session.DeleteKeyspaceIfExists(keyspaceName);
            }

            cluster.Shutdown();
        }
    }
}