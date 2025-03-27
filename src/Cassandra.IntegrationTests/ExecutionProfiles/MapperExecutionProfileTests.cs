//
//      Copyright (C) DataStax Inc.
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
using System.Linq;
using System.Threading.Tasks;

using Cassandra.Mapping;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using Cassandra.Tests.Mapping.Pocos;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.ExecutionProfiles
{
    [TestFixture]
    [Category(TestCategory.Short)]
    public class MapperExecutionProfileTests : TestGlobals
    {
        private ISession _session;
        private string _keyspace;
        private MappingConfiguration _mappingConfig;
        private List<Movie> _movieList;
        private SimulacronCluster _simulacronCluster;
        private IMapper _mapper;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            _keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();

            _simulacronCluster = SimulacronCluster.CreateNew(3);
            _session = ClusterBuilder()
                              .AddContactPoint(_simulacronCluster.InitialContactPoint)
                              .WithExecutionProfiles(opts => opts
                                  .WithProfile("testProfile", profile => profile
                                      .WithConsistencyLevel(ConsistencyLevel.Two))
                                  .WithDerivedProfile("testDerivedProfile", "testProfile", profile => profile
                                      .WithConsistencyLevel(ConsistencyLevel.One)))
                              .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.Any))
                              .Build().Connect(_keyspace);

            _mappingConfig = new MappingConfiguration()
                .Define(new Map<Movie>().PartitionKey(c => c.Title).PartitionKey(c => c.MovieMaker).KeyspaceName(_keyspace))
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song_insert").KeyspaceName(_keyspace));

            _movieList = Movie.GetDefaultMovieList();
            _mapper = new Mapper(_session, _mappingConfig);
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _session.Cluster.Dispose();
            _simulacronCluster.RemoveAsync().Wait();
        }

        private object[] CreatePrimeObject(Movie movie)
        {
            return new object[]
            {
                movie.MainActor,
                movie.MovieMaker,
                movie.Title,
                movie.ExampleSet.ToArray(),
                movie.Director,
                movie.Year
            };
        }

        private IThenFluent CreateThenForPrimeSelect(IWhenFluent when, IEnumerable<Movie> movies)
        {
            return when.ThenRowsSuccess(
                new[]
                {
                    ("MainActor", DataType.Ascii),
                    ("MovieMaker", DataType.Ascii),
                    ("Title", DataType.Ascii),
                    ("ExampleSet", DataType.List(DataType.Ascii)),
                    ("Director", DataType.Ascii),
                    ("Year", DataType.Int)
                },
                rows => rows.WithRows(movies.Select(CreatePrimeObject).ToArray())).WithIgnoreOnPrepare(true);
        }

        private void PrimeSelect(IEnumerable<Movie> movies, ConsistencyLevel consistencyLevel, string query = null)
        {
            var primeQuery =
                CreateThenForPrimeSelect(SimulacronBase
                    .PrimeBuilder()
                    .WhenQuery(
                        (query ?? "SELECT MainActor, MovieMaker, Title, ExampleSet, Director, Year") + $" FROM {_keyspace}.Movie",
                        when => when.WithConsistency(consistencyLevel)),
                movies).BuildRequest();
            _simulacronCluster.Prime(primeQuery);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFetchWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            PrimeSelect(_movieList, ConsistencyLevel.Two, "SELECT MainActor");

            var movies = async
                ? _mapper.FetchAsync<Movie>(Cql.New($"SELECT MainActor FROM {_keyspace}.Movie").WithExecutionProfile("testProfile")).Result.ToList()
                : _mapper.Fetch<Movie>(Cql.New($"SELECT MainActor FROM {_keyspace}.Movie").WithExecutionProfile("testProfile")).ToList();

            CollectionAssert.AreEqual(_movieList, movies, new MovieComparer());
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFetchPageWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            PrimeSelect(new List<Movie> { _movieList.First() }, ConsistencyLevel.One, "SELECT MovieMaker");

            var movies = async
                ? _mapper.FetchPageAsync<Movie>(Cql.New($"SELECT MovieMaker FROM {_keyspace}.Movie").WithExecutionProfile("testDerivedProfile")).Result.ToList()
                : _mapper.FetchPage<Movie>(Cql.New($"SELECT MovieMaker FROM {_keyspace}.Movie").WithExecutionProfile("testDerivedProfile")).ToList();

            Assert.AreEqual(1, movies.Count);
            Assert.IsTrue(new MovieComparer().Compare(_movieList.First(), movies.Single()) == 0);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFirstWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            PrimeSelect(new List<Movie> { _movieList.Skip(1).First() }, ConsistencyLevel.Two, "SELECT Title");

            var movie = async
                ? _mapper.FirstAsync<Movie>(Cql.New($"SELECT Title FROM {_keyspace}.Movie").WithExecutionProfile("testProfile")).Result
                : _mapper.First<Movie>(Cql.New($"SELECT Title FROM {_keyspace}.Movie").WithExecutionProfile("testProfile"));

            Assert.IsNotNull(movie);
            Assert.IsTrue(new MovieComparer().Compare(_movieList.Skip(1).First(), movie) == 0);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFirstOrDefaultWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            PrimeSelect(new List<Movie> { _movieList.First() }, ConsistencyLevel.Two, "SELECT Year");

            var movie = async
                ? _mapper.FirstOrDefaultAsync<Movie>(Cql.New($"SELECT Year FROM {_keyspace}.Movie").WithExecutionProfile("testProfile")).Result
                : _mapper.FirstOrDefault<Movie>(Cql.New($"SELECT Year FROM {_keyspace}.Movie").WithExecutionProfile("testProfile"));

            Assert.IsNotNull(movie);
            Assert.IsTrue(new MovieComparer().Compare(_movieList.First(), movie) == 0);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteSingletWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            PrimeSelect(new List<Movie> { _movieList.Skip(1).First() }, ConsistencyLevel.Two, "SELECT ExampleSet");

            var movie = async
                ? _mapper.SingleAsync<Movie>(Cql.New($"SELECT ExampleSet FROM {_keyspace}.Movie").WithExecutionProfile("testProfile")).Result
                : _mapper.Single<Movie>(Cql.New($"SELECT ExampleSet FROM {_keyspace}.Movie").WithExecutionProfile("testProfile"));

            Assert.IsNotNull(movie);
            Assert.IsTrue(new MovieComparer().Compare(_movieList.Skip(1).First(), movie) == 0);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteSingleOrDefaultWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            PrimeSelect(new List<Movie> { _movieList.Skip(2).First() }, ConsistencyLevel.Two, "SELECT Director");

            var movie = async
                ? _mapper.SingleOrDefaultAsync<Movie>(Cql.New($"SELECT Director FROM {_keyspace}.Movie").WithExecutionProfile("testProfile")).Result
                : _mapper.SingleOrDefault<Movie>(Cql.New($"SELECT Director FROM {_keyspace}.Movie").WithExecutionProfile("testProfile"));

            Assert.IsNotNull(movie);
            Assert.IsTrue(new MovieComparer().Compare(_movieList.Skip(2).First(), movie) == 0);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteInsertWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            var insert = $"INSERT INTO {_keyspace}.song_insert (Artist, Id, ReleaseDate, Title) VALUES (?, ?, ?, ?)";
            var queries = _simulacronCluster.GetQueries(insert, QueryType.Execute);

            if (async)
            {
                await _mapper.InsertAsync(song, "testProfile", true, null).ConfigureAwait(false);
                await _mapper.InsertAsync(song, "testProfile").ConfigureAwait(false);
                await _mapper.InsertAsync(song, "testProfile", true).ConfigureAwait(false);
            }
            else
            {
                _mapper.Insert(song, "testProfile", true, null);
                _mapper.Insert(song, "testProfile");
                _mapper.Insert(song, "testProfile", true);
            }
            var newQueries = _simulacronCluster.GetQueries(insert, QueryType.Execute);
            Assert.AreEqual(queries.Count + 3, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteInsertIfNotExistsWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            var insertIfNotExists = $"INSERT INTO {_keyspace}.song_insert (Artist, Id, ReleaseDate, Title) VALUES (?, ?, ?, ?) IF NOT EXISTS";
            var queries = _simulacronCluster.GetQueries(insertIfNotExists, QueryType.Execute);

            if (async)
            {
                await _mapper.InsertIfNotExistsAsync(song, "testProfile", true, null).ConfigureAwait(false);
                await _mapper.InsertIfNotExistsAsync(song, "testProfile").ConfigureAwait(false);
                await _mapper.InsertIfNotExistsAsync(song, "testProfile", true).ConfigureAwait(false);
            }
            else
            {
                _mapper.InsertIfNotExists(song, "testProfile", true, null);
                _mapper.InsertIfNotExists(song, "testProfile");
                _mapper.InsertIfNotExists(song, "testProfile", true);
            }

            var newQueries = _simulacronCluster.GetQueries(insertIfNotExists, QueryType.Execute);
            Assert.AreEqual(queries.Count + 3, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteDeleteWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            var delete = $"DELETE FROM {_keyspace}.song_insert WHERE Id = ?";
            var queries = _simulacronCluster.GetQueries(delete, QueryType.Execute);

            if (async)
            {
                await _mapper.DeleteAsync(song, "testProfile").ConfigureAwait(false);
                await _mapper.DeleteAsync<Song>(Cql.New("WHERE Id = ?", song.Id).WithExecutionProfile("testProfile")).ConfigureAwait(false);
            }
            else
            {
                _mapper.Delete(song, "testProfile");
                _mapper.Delete<Song>(Cql.New("WHERE Id = ?", song.Id).WithExecutionProfile("testProfile"));
            }

            var newQueries = _simulacronCluster.GetQueries(delete, QueryType.Execute);
            Assert.AreEqual(queries.Count + 2, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteDeleteIfWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            var delete = $"DELETE FROM {_keyspace}.song_insert WHERE Id = ? IF EXISTS";
            var queries = _simulacronCluster.GetQueries(delete, QueryType.Execute);

            if (async)
            {
                await _mapper.DeleteIfAsync<Song>(Cql.New("WHERE Id = ? IF EXISTS", song.Id).WithExecutionProfile("testProfile")).ConfigureAwait(false);
            }
            else
            {
                _mapper.DeleteIf<Song>(Cql.New("WHERE Id = ? IF EXISTS", song.Id).WithExecutionProfile("testProfile"));
            }

            var newQueries = _simulacronCluster.GetQueries(delete, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteUpdateWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            var update = $"UPDATE {_keyspace}.song_insert SET Artist = ?, ReleaseDate = ?, Title = ? WHERE Id = ?";
            var queries = _simulacronCluster.GetQueries(update, QueryType.Execute);

            if (async)
            {
                await _mapper.UpdateAsync(song, "testProfile").ConfigureAwait(false);
                await _mapper.UpdateAsync<Song>(
                    Cql.New("SET Artist = ?, ReleaseDate = ?, Title = ? WHERE Id = ?", song.Title, song.Artist, song.ReleaseDate, song.Id)
                       .WithExecutionProfile("testProfile")).ConfigureAwait(false);
            }
            else
            {
                _mapper.Update(song, "testProfile");
                _mapper.Update<Song>(
                    Cql.New("SET Artist = ?, ReleaseDate = ?, Title = ? WHERE Id = ?", song.Title, song.Artist, song.ReleaseDate, song.Id)
                       .WithExecutionProfile("testProfile"));
            }
            var newQueries = _simulacronCluster.GetQueries(update, QueryType.Execute);
            Assert.AreEqual(queries.Count + 2, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteUpdateIfWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            var update = $"UPDATE {_keyspace}.song_insert SET Title = ?, Artist = ?, ReleaseDate = ? WHERE Id = ? IF EXISTS";
            var queries = _simulacronCluster.GetQueries(update, QueryType.Execute);

            if (async)
            {
                await _mapper.UpdateIfAsync<Song>(
                    Cql.New("SET Title = ?, Artist = ?, ReleaseDate = ? WHERE Id = ? IF EXISTS", song.Title, song.Artist, song.ReleaseDate, song.Id)
                       .WithExecutionProfile("testProfile")).ConfigureAwait(false);
            }
            else
            {
                _mapper.UpdateIf<Song>(
                    Cql.New("SET Title = ?, Artist = ?, ReleaseDate = ? WHERE Id = ? IF EXISTS", song.Title, song.Artist, song.ReleaseDate, song.Id)
                       .WithExecutionProfile("testProfile"));
            }
            var newQueries = _simulacronCluster.GetQueries(update, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteBatchWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            var queries = _simulacronCluster.GetQueries(null, QueryType.Batch);
            var batch = _mapper.CreateBatch();
            batch.InsertIfNotExists(song);

            if (async)
            {
                await _mapper.ExecuteAsync(batch, "testProfile").ConfigureAwait(false);
            }
            else
            {
                _mapper.Execute(batch, "testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(null, QueryType.Batch);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.AreEqual(ConsistencyLevel.Two, newQueries.Last().Frame.GetBatchMessage().ConsistencyLevel);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteConditionalBatchWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            var queries = _simulacronCluster.GetQueries(null, QueryType.Batch);
            var batch = _mapper.CreateBatch();
            batch.InsertIfNotExists(song);

            if (async)
            {
                await _mapper.ExecuteConditionalAsync<Song>(batch, "testProfile").ConfigureAwait(false);
            }
            else
            {
                _mapper.ExecuteConditional<Song>(batch, "testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(null, QueryType.Batch);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.AreEqual(ConsistencyLevel.Two, newQueries.Last().Frame.GetBatchMessage().ConsistencyLevel);
        }
    }
}