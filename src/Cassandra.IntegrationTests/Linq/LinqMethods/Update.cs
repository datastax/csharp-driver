using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Update : TestGlobals
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var table = _session.GetTable<Movie>();
            table.Create();

            //Insert some data
            foreach (var movie in _movieList)
                table.Insert(movie).Execute();
        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        [Test]
        [NUnit.Framework.Ignore("TBD"), Explicit("TBD")]
        public void LinqUpdate_BatchUpdate()
        {
            Table<Movie> table = _session.GetTable<Movie>();
            table.CreateIfNotExists();


            {
                Batch batch = _session.CreateBatch();

                var movies = new List<Movie>
                {
                    new Movie
                    {
                        Title = "Serenity",
                        MovieMaker = "20CentFox",
                        Director = "Joss Whedon",
                        MainActor = "Nathan Fillion",
                        Year = 2005,
                        ExampleSet = new List<string> {"x", "y"}
                    },
                    new Movie
                    {
                        Title = "Pulp Fiction",
                        MovieMaker = "Pixar",
                        Director = "Quentin Tarantino",
                        MainActor = "John Travolta",
                        Year = 1994,
                        ExampleSet = new List<string> {"1", "2", "3"}
                    },
                };

                batch.Append(from m in movies select table.Insert(m));

                batch.Execute();
            }

            var testmovie = new Movie { Year = 2005, Director = "Quentin Tarantino", Title = "Pulp Fiction", MovieMaker = "Pixar" };
            table.Where(m => m.Title == testmovie.Title && m.MovieMaker == testmovie.MovieMaker && m.Director == testmovie.Director)
                 .Select(m => new Movie { Year = testmovie.Year })
                 .Update()
                 .Execute();


            var anonMovie = new { Director = "Quentin Tarantino", Year = 2005 };
            table.Where(m => m.Title == "Pulp Fiction" && m.MovieMaker == "Pixar" && m.Director == anonMovie.Director)
                 .Select(m => new Movie { Year = anonMovie.Year, MainActor = "George Clooney" })
                 .Update()
                 .Execute();

            List<Movie> all2 = table.Where(m => CqlToken.Create(m.Title, m.MovieMaker) > CqlToken.Create("Pulp Fiction", "Pixar")).Execute().ToList();
            List<Movie> all =
                (from m in table where CqlToken.Create(m.Title, m.MovieMaker) > CqlToken.Create("Pulp Fiction", "Pixar") select m).Execute().ToList();

            List<ExtMovie> nmT =
                (from m in table
                 where m.Director == "Quentin Tarantino"
                 select new ExtMovie { TheDirector = m.MainActor, Size = all.Count, TheMaker = m.Director }).Execute().ToList();
            var nm1 = (from m in table where m.Director == "Quentin Tarantino" select new { MA = m.MainActor, Z = 10, Y = m.Year }).Execute().ToList();

            var nmX = (from m in table where m.Director == "Quentin Tarantino" select new { m.MainActor, Z = 10, m.Year }).Execute().ToList();

            (from m in table
             where m.Title.Equals("Pulp Fiction") && m.MovieMaker.Equals("Pixar") && m.Director == "Quentin Tarantino"
             select new Movie { Year = 1994 }).Update().Execute();

            table.Where(m => m.Title == "Pulp Fiction" && m.MovieMaker == "Pixar" && m.Director == "Quentin Tarantino")
                 .Select(m => new Movie { Year = 1994 })
                 .Update()
                 .Execute();

            var nm2 = table.Where(m => m.Director == "Quentin Tarantino").Select(m => new { MA = m.MainActor, Y = m.Year }).Execute().ToList();

            (from m in table where m.Title == "Pulp Fiction" && m.MovieMaker == "Pixar" && m.Director == "Quentin Tarantino" select m).Delete().Execute();

            var nm3 = (from m in table where m.Director == "Quentin Tarantino" select new { MA = m.MainActor, Y = m.Year }).Execute().ToList();
        }


        public class ExtMovie
        {
            public int Size;
            public string TheDirector;
            public string TheMaker;
        }


    }
}
