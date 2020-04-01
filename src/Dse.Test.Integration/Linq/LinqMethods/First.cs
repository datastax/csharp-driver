//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using Dse.Test.Integration.SimulacronAPI;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder.Then;
using Newtonsoft.Json;
using NUnit.Framework;

#pragma warning disable 612

namespace Dse.Test.Integration.Linq.LinqMethods
{
    [TestCassandraVersion(2, 0)]
    public class First : SimulacronTest
    {
        private readonly List<Movie> _movieList = Movie.GetDefaultMovieList();
        private Table<Movie> _movieTable;

        public override void SetUp()
        {
            base.SetUp();
            
            MappingConfiguration movieMappingConfig = new MappingConfiguration();
            movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            _movieTable = new Table<Movie>(Session, movieMappingConfig);
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void First_ExecuteAsync(bool async)
        {
            try
            {
                var expectedMovie = _movieList.First();
                TestCluster.PrimeFluent(
                    b => b.WhenQuery(
                              "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                              $"FROM \"{Movie.TableName}\" WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? LIMIT ? ALLOW FILTERING",
                              rows => rows.WithParams(expectedMovie.Title, expectedMovie.MovieMaker, 1))
                          .ThenRowsSuccess(expectedMovie.CreateRowsResult()));

                var actualMovieQuery =
                    _movieTable.First(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker);

                var actualMovie = async ? actualMovieQuery.ExecuteAsync().Result : actualMovieQuery.Execute();
                Movie.AssertEquals(expectedMovie, actualMovie);
            }
            catch (Exception ex)
            {
                Assert.Fail(
                    string.Join(",", typeof(Movie).GetFields().Select(f => f.Name)
                                 .Union(typeof(Movie).GetProperties().Select(p => p.Name))) + 
                                   ex + Environment.NewLine + JsonConvert.SerializeObject(TestCluster.GetLogs()));
            }
        }

        [Test]
        public void First_NoSuchRecord()
        {
            Movie existingMovie = _movieList.Last();
            string randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? LIMIT ? ALLOW FILTERING",
                          rows => rows.WithParams(existingMovie.Title, randomStr, 1))
                      .ThenRowsSuccess(Movie.GetColumns()));

            Movie foundMovie = _movieTable.First(m => m.Title == existingMovie.Title && m.MovieMaker == randomStr).Execute();
            Assert.Null(foundMovie);
            
            VerifyBoundStatement(
                "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                $"FROM \"{Movie.TableName}\" WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? LIMIT ? ALLOW FILTERING",
                1,
                existingMovie.Title, randomStr, 1);
        }

        ///////////////////////////////////////////////
        /// Exceptions
        ///////////////////////////////////////////////

        [Test]
        public void First_NoTranslationFromLinqToCql()
        {
            //No translation in CQL
            Assert.Throws<CqlLinqNotSupportedException>(() => _movieTable.First(m => m.Year is int).Execute());
        }

        [Test]
        [TestCassandraVersion(3, 0, Comparison.LessThan)]
        public void First_NoPartitionKey()
        {
            //No partition key in Query
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"yearMade\" = ? LIMIT ? ALLOW FILTERING",
                          rows => rows.WithParams(100, 1))
                      .ThenServerError(ServerError.Invalid, "msg"));
            var ex = Assert.Throws<InvalidQueryException>(() => _movieTable.First(m => m.Year == 100).Execute());
            Assert.AreEqual("msg", ex.Message);
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"mainGuy\" = ? LIMIT ? ALLOW FILTERING",
                          rows => rows.WithParam(DataType.Ascii, null).WithParam(DataType.Int, 1))
                      .ThenServerError(ServerError.Invalid, "msg"));
            ex = Assert.Throws<InvalidQueryException>(() => _movieTable.First(m => m.MainActor == null).Execute());
            Assert.AreEqual("msg", ex.Message);
        }

        [Test]
        public void First_With_Serial_ConsistencyLevel()
        {
            var expectedMovie = _movieList.First();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" LIMIT ? ALLOW FILTERING",
                          rows => rows.WithParams(1))
                      .ThenRowsSuccess(expectedMovie.CreateRowsResult()));
            var actualMovie = _movieTable.First().SetConsistencyLevel(ConsistencyLevel.Serial).Execute();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        /// <summary>
        /// Test if driver throws exception when query without all partition keys.
        /// Since Cassandra 3.10 it is not expected to throw InvalidException.
        ///
        /// @expected_errors InvalidQueryException
        /// @jira_ticket CASSANDRA-11031
        /// @test_assumptions
        ///     - Cassandra version less than 3.10
        /// </summary>
        [Test]
        [TestCassandraVersion(3, 9, Comparison.LessThan)]
        public void First_MissingPartitionKey()
        {
            string expectedErrMsg = "Partition key part(s:)? movie_maker must be restricted (since preceding part is|as other parts are)";
            var randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"unique_movie_title\" = ? LIMIT ? ALLOW FILTERING",
                          rows => rows.WithParams(randomStr, 1))
                      .ThenServerError(ServerError.Invalid, expectedErrMsg));

            try
            {
                _movieTable.First(m => m.Title == randomStr).Execute();
                Assert.Fail("expected exception was not thrown!");
            }
            catch (InvalidQueryException e)
            {
                Assert.AreEqual(expectedErrMsg, e.Message);
            }
        }
    }
}