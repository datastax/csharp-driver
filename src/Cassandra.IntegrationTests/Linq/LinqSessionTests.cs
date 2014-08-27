//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using NUnit.Framework;
using System.Diagnostics;

namespace Cassandra.IntegrationTests.Linq
{
    [Category("short")]
    public class LinqSessionTests : SingleNodeClusterTest
    {
        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            var table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();
            //Insert some data
            var ps = Session.Prepare("INSERT INTO \"nerdiStuff\" " +
                            "(\"movieTile\", \"movieMaker\", \"diri\", \"mainGuy\") VALUES " +
                            "(?, ?, ?, ?)");
            //dont mind the schema, it does not make much sense
            Session.Execute(ps.Bind("title1", "maker1", "director1", "actor1"));
            Session.Execute(ps.Bind("title2", "maker2", "director2", "actor2"));
            Session.Execute(ps.Bind("title3", "maker3", "director3", null));
            Session.Execute(ps.Bind("title4", "maker4", "director4a", null));
            Session.Execute(ps.Bind("title4", "maker4", "director4b", null));
        }

        [Test]
        public void InsertAndSelectExecuteAsync()
        {
            var table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();
            var movie = new NerdMovie
            {
                Movie = "Life of Brian",
                Director = "Terry Jones",
                MainActor = "Terry Gilliam",
                Maker = "HandMade Films",
                Year = 1979
            };
            var taskInsert = table.Insert(movie).ExecuteAsync();
            var rs = taskInsert.Result;
            Assert.AreEqual(0, rs.Count());

            var taskSelect = table.Where(m => m.Director == "Terry Jones").ExecuteAsync();
            var movies = taskSelect.Result.ToArray();
            var count = movies.Length;
            var resultMovie = movies.First();
            Assert.AreEqual(movie.Maker, resultMovie.Maker);
            Assert.AreEqual(movie.Director, resultMovie.Director);
            Assert.AreEqual(movie.MainActor, resultMovie.MainActor);
            Assert.AreEqual(movie.Year, resultMovie.Year);

            //Fetch from Cassandra the count
            var countQueryResult = table
                .Where(m => m.Director == resultMovie.Director && m.Movie == resultMovie.Movie && m.Maker == resultMovie.Maker)
                .Count()
                .ExecuteAsync()
                .Result;

            var countQuerySync = table
                .Where(m => m.Director == resultMovie.Director && m.Movie == resultMovie.Movie && m.Maker == resultMovie.Maker)
                .Count()
                .Execute();

            Assert.AreEqual(count, countQueryResult);
            Assert.AreEqual(count, countQuerySync);

            var first = table.
                First(m => m.Director == movie.Director).Execute();
            Assert.AreEqual(movie.Maker, first.Maker);
            Assert.AreEqual(movie.Director, first.Director);
            Assert.AreEqual(movie.MainActor, first.MainActor);
            Assert.AreEqual(movie.Year, first.Year);
        }

        [Test]
        public void FirstOrDefaultTest()
        {
            var table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();
            var first = table.FirstOrDefault(m => m.Director == "whatever").Execute();
            Assert.IsNull(first);

            //sync
            first = table.FirstOrDefault(m => m.Director == "director1" && m.Movie == "title1" && m.Maker == "maker1").Execute();
            Assert.IsNotNull(first);
            Assert.AreEqual("maker1", first.Maker);

            //async
            first = table.FirstOrDefault(m => m.Director == "director2" && m.Movie == "title2" && m.Maker == "maker2").ExecuteAsync().Result;
            Assert.IsNotNull(first);
            Assert.AreEqual("actor2", first.MainActor);
        }

        [Test]
        public void FirstTest()
        {
            var table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();
            //sync
            var first = table.First(m => m.Director == "director1" && m.Movie == "title1" && m.Maker == "maker1").Execute();
            Assert.IsNotNull(first);
            Assert.AreEqual("actor1", first.MainActor);
            //async
            first = table.First(m => m.Director == "director2" && m.Movie == "title2" && m.Maker == "maker2").ExecuteAsync().Result;
            Assert.IsNotNull(first);
            Assert.AreEqual("maker2", first.Maker);
        }

        [Test]
        public void CountTest()
        {
            var table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();
            //global count
            var count = table.Count().Execute();
            Assert.Greater(count, 0);

            count = table.Where(m => m.Movie == "title2" && m.Maker == "maker2").Count().Execute();
            Assert.AreEqual(1, count);
            count = table.Where(m => m.Movie == "title3" && m.Maker == "maker3").Count().ExecuteAsync().Result;
            Assert.AreEqual(1, count);
        }

        [Test]
        public void TakeTest()
        {
            var table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();
            //with where clause
            var results = table
                .Where(m => m.Director == "director1" && m.Movie == "title1" && m.Maker == "maker1")
                .Take(1)
                .Execute();
            Assert.AreEqual(1, results.Count());
            //without where clause
            results = table
                .Take(2)
                .ExecuteAsync()
                .Result;
            Assert.AreEqual(2, results.Count());
            results = table
                .Take(10000)
                .Execute();
            Assert.Greater(results.Count(), 2);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void UpdateIfTest()
        {
            var table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();
            var movie = new NerdMovie()
            {
                Movie = "Dead Poets Society",
                Year = 1989,
                MainActor = "Robin Williams",
                Director = "Peter Weir",
                Maker = "Touchstone"
            };
            table
                .Insert(movie)
                .SetConsistencyLevel(ConsistencyLevel.Quorum)
                .Execute();

            var retrievedMovie = table
                .FirstOrDefault(m => m.Movie == "Dead Poets Society" && m.Maker == "Touchstone")
                .Execute();
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual(1989, retrievedMovie.Year);
            Assert.AreEqual("Robin Williams", retrievedMovie.MainActor);

            table
                .Where(m => m.Movie == "Dead Poets Society" && m.Maker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new NerdMovie {MainActor = "Robin McLaurin Williams"})
                .UpdateIf(m => m.Year == 1989)
                .Execute();

            retrievedMovie = table
                .FirstOrDefault(m => m.Movie == "Dead Poets Society" && m.Maker == "Touchstone")
                .Execute();
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual(1989, retrievedMovie.Year);
            Assert.AreEqual("Robin McLaurin Williams", retrievedMovie.MainActor);

            //Should not update as the if clause is not satisfied
            table
                .Where(m => m.Movie == "Dead Poets Society" && m.Maker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new NerdMovie { MainActor = "WHOEVER" })
                .UpdateIf(m => m.Year == 1500)
                .Execute();
            retrievedMovie = table
                .FirstOrDefault(m => m.Movie == "Dead Poets Society" && m.Maker == "Touchstone")
                .Execute();
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual("Robin McLaurin Williams", retrievedMovie.MainActor);
        }

        [Test]
        public void OrderByTest()
        {
            var table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();

            var results = table
                .Where(m => m.Movie == "title1" && m.Maker == "maker1")
                .OrderBy(m => m.Director)
                .Execute();
            Assert.AreEqual(1, results.Count());

            results = table
                .Where(m => m.Movie == "title4" && m.Maker == "maker4")
                .OrderBy(m => m.Director)
                .ExecuteAsync()
                .Result;
            var resultOrder1 = results.ToList();

            results = table
                .Where(m => m.Movie == "title4" && m.Maker == "maker4")
                .OrderByDescending(m => m.Director)
                .Execute();
            var resultOrder2 = results.ToList();
            Assert.AreEqual(2, resultOrder1.Count);
            Assert.AreEqual(2, resultOrder2.Count);
            Assert.AreNotEqual(resultOrder1.First().Director, resultOrder2.First().Director);
        }

        [Test]
        public void CqlQueryExceptiosnTest()
        {
            var table = Session.GetTable<NerdMovie>();
            //No translation in CQL
            Assert.Throws<SyntaxError>(() => table.Where(m => m.Year is int).Execute());
            //No partition key in Query
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.MainActor == null).Execute());
            //No execute
            Assert.Throws<InvalidOperationException>(() => table.Where(m => m.Maker == "dum").GetEnumerator());

            //Wrong consistency level
            Assert.Throws<RequestInvalidException>(() => table.Where(m => m.Maker == "dum").SetConsistencyLevel(ConsistencyLevel.Serial).Execute());
        }

        [Test]
        public void CqlQuerySingleElementExceptionsTest()
        {
            var table = Session.GetTable<NerdMovie>();
            //No translation in CQL
            Assert.Throws<SyntaxError>(() => table.First(m => m.Year is int).Execute());
            //No partition key in Query
            Assert.Throws<InvalidQueryException>(() => table.First(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => table.First(m => m.MainActor == null).Execute());
            //Wrong consistency level
            Assert.Throws<RequestInvalidException>(() => table.First(m => m.Maker == "dum").SetConsistencyLevel(ConsistencyLevel.Serial).Execute());
        }

        [Test]
        public void CqlScalarExceptionsTest()
        {
            var table = Session.GetTable<NerdMovie>();
            //No translation in CQL
            Assert.Throws<SyntaxError>(() => table.Where(m => m.Year is int).Count().Execute());
            //No partition key in Query
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.Year == 100).Count().Execute());
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.MainActor == null).Count().Execute());
            //Wrong consistency level
            Assert.Throws<RequestInvalidException>(() => table.Where(m => m.Maker == "dum").Count().SetConsistencyLevel(ConsistencyLevel.LocalSerial).Execute());
        }

        [Test]
        public void CqlCommandExceptionsTest()
        {
            var table = Session.GetTable<NerdMovie>();
            //No translation in CQL
            Assert.Throws<SyntaxError>(() => table
                .Where(m => m.Year is int)
                .Select(m => new NerdMovie {Year = 1})
                .Update().Execute());
            //Delete: No partition key in Query
            Assert.Throws<InvalidQueryException>(() => table
                .Where(m => m.Year == 1999)
                .Delete()
                .Execute());
            //Insert: No partition key in Query
            Assert.Throws<InvalidQueryException>(() => table
                .Insert(new NerdMovie() { MainActor = "Dolph Lundgren" })
                .Execute());
            //Wrong consistency level
            Assert.Throws<RequestInvalidException>(() => table
                .Where(m => m.Movie == "title1" && m.Maker == "maker1")
                .SetConsistencyLevel(ConsistencyLevel.LocalSerial)
                .Delete()
                .Execute());
        }

        [Test]
        public void LinqBatchInsertUpdateSelectTest()
        {
            Table<NerdMovie> table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();


            {
                Batch batch = Session.CreateBatch();

                var movies = new List<NerdMovie>
                {
                    new NerdMovie
                    {
                        Movie = "Serenity",
                        Maker = "20CentFox",
                        Director = "Joss Whedon",
                        MainActor = "Nathan Fillion",
                        Year = 2005,
                        exampleSet = new List<string> {"x", "y"}
                    },
                    new NerdMovie
                    {
                        Movie = "Pulp Fiction",
                        Maker = "Pixar",
                        Director = "Quentin Tarantino",
                        MainActor = "John Travolta",
                        Year = 1994,
                        exampleSet = new List<string> {"1", "2", "3"}
                    },
                };

                batch.Append(from m in movies select table.Insert(m));

                batch.Execute();
            }

            var testmovie = new NerdMovie {Year = 2005, Director = "Quentin Tarantino", Movie = "Pulp Fiction", Maker = "Pixar"};
            table.Where(m => m.Movie == testmovie.Movie && m.Maker == testmovie.Maker && m.Director == testmovie.Director)
                 .Select(m => new NerdMovie {Year = testmovie.Year})
                 .Update()
                 .Execute();


            var anonMovie = new {Director = "Quentin Tarantino", Year = 2005};
            table.Where(m => m.Movie == "Pulp Fiction" && m.Maker == "Pixar" && m.Director == anonMovie.Director)
                 .Select(m => new NerdMovie {Year = anonMovie.Year, MainActor = "George Clooney"})
                 .Update()
                 .Execute();

            List<NerdMovie> all2 = table.Where(m => CqlToken.Create(m.Movie, m.Maker) > CqlToken.Create("Pulp Fiction", "Pixar")).Execute().ToList();
            List<NerdMovie> all =
                (from m in table where CqlToken.Create(m.Movie, m.Maker) > CqlToken.Create("Pulp Fiction", "Pixar") select m).Execute().ToList();

            List<ExtMovie> nmT =
                (from m in table
                 where m.Director == "Quentin Tarantino"
                 select new ExtMovie {TheDirector = m.MainActor, Size = all.Count, TheMaker = m.Director}).Execute().ToList();
            var nm1 = (from m in table where m.Director == "Quentin Tarantino" select new {MA = m.MainActor, Z = 10, Y = m.Year}).Execute().ToList();

            var nmX = (from m in table where m.Director == "Quentin Tarantino" select new {m.MainActor, Z = 10, m.Year}).Execute().ToList();

            (from m in table
             where m.Movie.Equals("Pulp Fiction") && m.Maker.Equals("Pixar") && m.Director == "Quentin Tarantino"
             select new NerdMovie {Year = 1994}).Update().Execute();

            table.Where(m => m.Movie == "Pulp Fiction" && m.Maker == "Pixar" && m.Director == "Quentin Tarantino")
                 .Select(m => new NerdMovie {Year = 1994})
                 .Update()
                 .Execute();

            var nm2 = table.Where(m => m.Director == "Quentin Tarantino").Select(m => new {MA = m.MainActor, Y = m.Year}).Execute().ToList();

            (from m in table where m.Movie == "Pulp Fiction" && m.Maker == "Pixar" && m.Director == "Quentin Tarantino" select m).Delete().Execute();

            var nm3 = (from m in table where m.Director == "Quentin Tarantino" select new {MA = m.MainActor, Y = m.Year}).Execute().ToList();
        }

        [Test]
        public void LinqBatchInsertAndSelectTestTpl()
        {
            Table<NerdMovie> table = Session.GetTable<NerdMovie>();
            table.CreateIfNotExists();

            {
                Batch batch = Session.CreateBatch();

                var movies = new List<NerdMovie>
                {
                    new NerdMovie
                    {
                        Movie = "Serenity",
                        Maker = "20CentFox",
                        Director = "Joss Whedon",
                        MainActor = "Nathan Fillion",
                        Year = 2005,
                        exampleSet = new List<string> {"x", "y"}
                    },
                    new NerdMovie
                    {
                        Movie = "Pulp Fiction",
                        Maker = "Pixar",
                        Director = "Quentin Tarantino",
                        MainActor = "John Travolta",
                        Year = 1994,
                        exampleSet = new List<string> {"1", "2", "3"}
                    },
                };

                batch.Append(from m in movies select table.Insert(m));

                Task taskSaveMovies = Task.Factory.FromAsync(batch.BeginExecute, batch.EndExecute, null);
                taskSaveMovies.Wait();
            }

            Table<NerdMovie> selectNerdMovies = table; //select everything from table


            Task taskSelectStartMovies =
                Task<IEnumerable<NerdMovie>>.Factory.FromAsync(selectNerdMovies.BeginExecute,
                                                               selectNerdMovies.EndExecute, null)
                                            .ContinueWith(res => DisplayMovies(res.Result));


            taskSelectStartMovies.Wait();

            CqlQuery<NerdMovie> selectAllFromWhere = from m in table where m.Director == "Joss Whedon" select m;

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
        }

        private static void DisplayMovies(IEnumerable<NerdMovie> result)
        {
            foreach (NerdMovie resMovie in result)
            {
                Trace.TraceInformation("Movie={0} Director={1} MainActor={2}, Year={3}",
                                  resMovie.Movie, resMovie.Director, resMovie.MainActor, resMovie.Year);
            }
        }

        public class ExtMovie
        {
            public int Size;
            public string TheDirector;
            public string TheMaker;
        }

        [AllowFiltering]
        [Table("nerdiStuff")]
        public class NerdMovie
        {
            [Column("mainGuy")] public string MainActor;

            [PartitionKey(5)] [Column("movieMaker")] public string Maker;
            [PartitionKey(1)] [Column("movieTile")] public string Movie;

            [Column("List")] public List<string> exampleSet = new List<string>();

            [ClusteringKey(1)]
            [Column("diri")]
            public string Director { get; set; }

            [Column("When-Made")]
            public int? Year { get; set; }
        }
    }
}