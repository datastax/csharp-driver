using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;

namespace TPLSample.NerdMoviesLinqSample
{
    public class NerdMoviesLinqSample
    {
        public static void Run()
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithoutRowSetBuffering().Build();

            using (Session session = cluster.Connect())
            {
                const string keyspaceName = "Excelsior";
                Console.WriteLine("============================================================");
                Console.WriteLine("Creating keyspace...");

                try
                {
                    session.ChangeKeyspace(keyspaceName);
                }
                catch (InvalidQueryException)
                {
                    session.CreateKeyspaceIfNotExists(keyspaceName);
                    session.ChangeKeyspace(keyspaceName);
                }

                Console.WriteLine("============================================================");
                Console.WriteLine("Creating tables...");
                var context = new Context(session);
                context.AddTable<NerdMovie>();
                context.CreateTablesIfNotExist();
                Console.WriteLine("============================================================");

                context.GetTable<NerdMovie>()
                       .AddNew(new NerdMovie {Movie = "Serenity", Director = "Joss Whedon", MainActor = "Nathan Fillion", Year = 2005});
                Task taskSaveMovies = Task.Factory.FromAsync(context.BeginSaveChangesBatch, context.EndSaveChangesBatch, TableType.Standard,
                                                             session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), null);

                taskSaveMovies.Wait();

                ContextTable<NerdMovie> selectNerdMovies = context.GetTable<NerdMovie>(); //select everything from table


                Task taskSelectStartMovies =
                    Task<IEnumerable<NerdMovie>>.Factory.FromAsync(selectNerdMovies.BeginExecute,
                                                                   selectNerdMovies.EndExecute, null)
                                                .ContinueWith(res => DisplayMovies(res.Result));


                taskSelectStartMovies.Wait();

                CqlQuery<NerdMovie> selectAllFromWhere = from m in context.GetTable<NerdMovie>() where m.Director == "Joss Whedon" select m;

                Task taskselectAllFromWhere =
                    Task<IEnumerable<NerdMovie>>.Factory.FromAsync(selectAllFromWhere.BeginExecute,
                                                                   selectAllFromWhere.EndExecute, null)
                                                .ContinueWith(res => DisplayMovies(res.Result));

                taskselectAllFromWhere.Wait();

                Task<List<NerdMovie>> taskselectAllFromWhereWithFuture =
                    Task<IEnumerable<NerdMovie>>.Factory.FromAsync(selectAllFromWhere.BeginExecute,
                                                                   selectAllFromWhere.EndExecute, null)
                                                .ContinueWith(a => a.Result.ToList());

                DisplayMovies(taskselectAllFromWhereWithFuture.Result);

                session.DeleteKeyspaceIfExists(keyspaceName);
            }

            cluster.Shutdown();
        }

        private static void DisplayMovies(IEnumerable<NerdMovie> result)
        {
            foreach (NerdMovie resMovie in result)
            {
                Console.WriteLine("Movie={0} Director={1} MainActor={2}, Year={3}",
                                  resMovie.Movie, resMovie.Director, resMovie.MainActor, resMovie.Year);
            }
            Console.WriteLine();
            Console.WriteLine();
        }
    }
}