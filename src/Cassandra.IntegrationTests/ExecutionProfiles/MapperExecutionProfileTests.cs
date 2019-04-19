//
//       Copyright (C) 2019 DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System.Collections.Generic;
using System.Linq;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Mapping;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.ExecutionProfiles
{
    [TestFixture]
    [Category("short")]
    public class MapperExecutionProfileTests
    {
        private Map<Movie> _config;
        private ISession _session;
        private string _keyspace;
        private MappingConfiguration _mappingConfig;
        private List<Movie> _movieList;
        private SimulacronCluster _simulacronCluster;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            _keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();

            _simulacronCluster = SimulacronCluster.CreateNew(3);
            _session = Cluster.Builder()
                              .AddContactPoint(_simulacronCluster.InitialContactPoint)
                              .WithExecutionProfiles(opts => opts
                                  .WithProfile("testProfile", profile => profile
                                      .WithConsistencyLevel(ConsistencyLevel.Two))
                                  .WithDerivedProfile("testDerivedProfile", "testProfile", profile => profile
                                      .WithConsistencyLevel(ConsistencyLevel.One)))
                              .Build().Connect(_keyspace);

            _config = new Map<Movie>().PartitionKey(c => c.Title).PartitionKey(c => c.MovieMaker).KeyspaceName(_keyspace);
            _mappingConfig = new MappingConfiguration().Define(_config);

            _movieList = Movie.GetDefaultMovieList();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _session.Cluster.Dispose();
            _simulacronCluster.Remove().Wait();
        }

        private object CreatePrimeObject(Movie movie)
        {
            return new
            {
                MainActor = movie.MainActor,
                MovieMaker = movie.MovieMaker,
                Title = movie.Title,
                ExampleSet = movie.ExampleSet.ToArray(),
                Director = movie.Director,
                Year = movie.Year
            };
        }

        private object CreateThenForPrime(IEnumerable<Movie> movies)
        {
            return new
            {
                result = "success",
                delay_in_ms = 0,
                rows = movies.Select(CreatePrimeObject).ToArray(),
                column_types = new
                {
                    MainActor = "ascii",
                    MovieMaker = "ascii",
                    Title = "ascii",
                    ExampleSet = "list<ascii>",
                    Director = "ascii",
                    Year = "int"
                },
                ignore_on_prepare = true
            };
        }

        private void PrimeSelect(IEnumerable<Movie> movies, string consistencyLevel, string query = null)
        {
            var primeQuery = new
            {
                when = new
                {
                    query = (query ?? "SELECT MainActor, MovieMaker, Title, ExampleSet, Director, Year") + $" FROM {_keyspace}.Movie",
                    consistency_level = new[] { consistencyLevel }
                },
                then = CreateThenForPrime(movies)
            };
            _simulacronCluster.Prime(primeQuery);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFetchWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            PrimeSelect(_movieList, "TWO", $"SELECT MainActor");
            var mapper = new Mapper(_session, _mappingConfig);

            var movies = async
                ? mapper.FetchAsync<Movie>(Cql.New($"SELECT MainActor FROM {_keyspace}.Movie").WithExecutionProfile("testProfile")).Result.ToList()
                : mapper.Fetch<Movie>(Cql.New($"SELECT MainActor FROM {_keyspace}.Movie").WithExecutionProfile("testProfile")).ToList();

            CollectionAssert.AreEqual(_movieList, movies, new MovieComparer());
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFetchPageWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            PrimeSelect(new List<Movie> { _movieList.First() }, "ONE");
            var mapper = new Mapper(_session, _mappingConfig);

            var movies = async
                ? mapper.FetchPageAsync<Movie>(Cql.New().WithExecutionProfile("testDerivedProfile")).Result.ToList()
                : mapper.FetchPage<Movie>(Cql.New().WithExecutionProfile("testDerivedProfile")).ToList();

            Assert.AreEqual(1, movies.Count);
            Assert.IsTrue(new MovieComparer().Compare(_movieList.First(), movies.Single()) == 0);
        }

        //[Test]
        //public void First_Async()
        //{
        //    var actualMovie = _mapper.FirstAsync<Movie>(_selectAllDefaultCql).Result;
        //    Movie.AssertListContains(_movieList, actualMovie);
        //}

        //[Test]
        //public void METHOD()
        //{
        //}
    }
}