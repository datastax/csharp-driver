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
        [Test]
        public void LinqBatchInsertAndSelectTest()
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