//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Linq;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using NUnit.Framework;

#pragma warning disable 612

namespace Dse.Test.Integration.Linq.LinqMethods
{
    public class Update : SimulacronTest
    {
        private readonly List<Movie> _movieList = Movie.GetDefaultMovieList();
        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

        public override void SetUp()
        {
            base.SetUp();
            Session.ChangeKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Successfully update multiple records using a single (non-batch) update
        /// </summary>
        [TestCase(true), TestCase(false)]
        [Test]
        public void LinqUpdate_Single(bool async)
        {
            // Setup
            var table = new Table<Movie>(Session, new MappingConfiguration());
            var movieToUpdate = _movieList[1];

            var expectedMovie = new Movie(movieToUpdate.Title, movieToUpdate.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate.MovieMaker, 1212);
            var updateQuery =
                table.Where(m => m.Title == movieToUpdate.Title && m.MovieMaker == movieToUpdate.MovieMaker && m.Director == movieToUpdate.Director)
                     .Select(m => new Movie { Year = expectedMovie.Year, MainActor = expectedMovie.MainActor })
                     .Update();

            if (async)
            {
                updateQuery.ExecuteAsync().GetAwaiter().GetResult();
            }
            else
            {
                updateQuery.Execute();
            }

            VerifyBoundStatement(
                $"UPDATE \"{Movie.TableName}\" " +
                "SET \"yearMade\" = ?, \"mainGuy\" = ? " +
                "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ?",
                1,
                expectedMovie.Year,
                expectedMovie.MainActor,
                movieToUpdate.Title,
                movieToUpdate.MovieMaker,
                movieToUpdate.Director);
        }

        /// <summary>
        /// Try to update a non existing record
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void LinqUpdate_IfExists()
        {
            // Setup
            var table = new Table<Movie>(Session, new MappingConfiguration());

            var unexistingMovie = new Movie("Unexisting movie title", "Unexisting movie director", "Unexisting movie actor", "Unexisting movie maker", 1212);

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"UPDATE \"{Movie.TableName}\" " +
                          "SET \"yearMade\" = ?, \"mainGuy\" = ? " +
                          "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ? IF EXISTS",
                          when => when.WithParams(unexistingMovie.Year, unexistingMovie.MainActor, unexistingMovie.Title, unexistingMovie.MovieMaker,
                              unexistingMovie.Director))
                      .ThenRowsSuccess(Movie.CreateAppliedInfoRowsResultWithoutMovie(false)));

            var cql = table.Where(m => m.Title == unexistingMovie.Title && m.MovieMaker == unexistingMovie.MovieMaker && m.Director == unexistingMovie.Director)
                           .Select(m => new Movie { Year = unexistingMovie.Year, MainActor = unexistingMovie.MainActor })
                           .UpdateIfExists();
            var appliedInfo = cql.Execute();

            Assert.IsFalse(appliedInfo.Applied);

            VerifyBoundStatement(
                $"UPDATE \"{Movie.TableName}\" " +
                "SET \"yearMade\" = ?, \"mainGuy\" = ? " +
                "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ? IF EXISTS",
                1,
                unexistingMovie.Year, unexistingMovie.MainActor, unexistingMovie.Title, unexistingMovie.MovieMaker, unexistingMovie.Director);
        }

        /// <summary>
        /// Successfully update multiple records using a Batch update
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void LinqUpdate_Batch()
        {
            // Setup
            var table = new Table<Movie>(Session, new MappingConfiguration());
            var movieToUpdate1 = _movieList[1];
            var movieToUpdate2 = _movieList[2];

            var batch = new BatchStatement();

            var expectedMovie1 = new Movie(movieToUpdate1.Title, movieToUpdate1.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate1.MovieMaker, 1212);
            var update1 = table.Where(m => m.Title == movieToUpdate1.Title && m.MovieMaker == movieToUpdate1.MovieMaker && m.Director == movieToUpdate1.Director)
                 .Select(m => new Movie { Year = expectedMovie1.Year, MainActor = expectedMovie1.MainActor })
                 .Update();
            batch.Add(update1);

            var expectedMovie2 = new Movie(movieToUpdate2.Title, movieToUpdate2.Director, "also_something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate2.MovieMaker, 1212);
            var update2 = table.Where(m => m.Title == movieToUpdate2.Title && m.MovieMaker == movieToUpdate2.MovieMaker && m.Director == movieToUpdate2.Director)
                 .Select(m => new Movie { Year = expectedMovie2.Year, MainActor = expectedMovie2.MainActor })
                 .Update();
            batch.Add(update2);

            table.GetSession().Execute(batch);

            VerifyBatchStatement(
                1,
                new[]
                {
                    $"UPDATE \"{Movie.TableName}\" " +
                        "SET \"yearMade\" = ?, \"mainGuy\" = ? " +
                        "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ?",
                    $"UPDATE \"{Movie.TableName}\" " +
                        "SET \"yearMade\" = ?, \"mainGuy\" = ? " +
                        "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ?"
                },
                new[]
                {
                    new object[] { expectedMovie1.Year, expectedMovie1.MainActor, movieToUpdate1.Title, movieToUpdate1.MovieMaker, movieToUpdate1.Director },
                    new object[] { expectedMovie2.Year, expectedMovie2.MainActor, movieToUpdate2.Title, movieToUpdate2.MovieMaker, movieToUpdate2.Director }
                });
        }

        [TestCase(BatchType.Unlogged)]
        [TestCase(BatchType.Logged)]
        [TestCase(default(BatchType))]
        [TestCassandraVersion(2, 0)]
        public void LinqUpdate_UpdateBatchType(BatchType batchType)
        {
            // Setup
            var table = new Table<Movie>(Session, new MappingConfiguration());
            var movieToUpdate1 = _movieList[1];
            var movieToUpdate2 = _movieList[2];

            var batch = table.GetSession().CreateBatch(batchType);

            var expectedMovie1 = new Movie(movieToUpdate1.Title, movieToUpdate1.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate1.MovieMaker, 1212);
            var update1 = table.Where(m => m.Title == movieToUpdate1.Title && m.MovieMaker == movieToUpdate1.MovieMaker && m.Director == movieToUpdate1.Director)
                               .Select(m => new Movie { Year = expectedMovie1.Year, MainActor = expectedMovie1.MainActor })
                               .Update();
            batch.Append(update1);

            var expectedMovie2 = new Movie(movieToUpdate2.Title, movieToUpdate2.Director, "also_something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate2.MovieMaker, 1212);
            var update2 = table.Where(m => m.Title == movieToUpdate2.Title && m.MovieMaker == movieToUpdate2.MovieMaker && m.Director == movieToUpdate2.Director)
                               .Select(m => new Movie { Year = expectedMovie2.Year, MainActor = expectedMovie2.MainActor })
                               .Update();
            batch.Append(update2);

            batch.Execute();

            VerifyBatchStatement(
                1,
                new[]
                {
                    $"UPDATE \"{Movie.TableName}\" " +
                    "SET \"yearMade\" = ?, \"mainGuy\" = ? " +
                    "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ?",
                    $"UPDATE \"{Movie.TableName}\" " +
                    "SET \"yearMade\" = ?, \"mainGuy\" = ? " +
                    "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ?"
                },
                new[]
                {
                    new object[] { expectedMovie1.Year, expectedMovie1.MainActor, movieToUpdate1.Title, movieToUpdate1.MovieMaker, movieToUpdate1.Director },
                    new object[] { expectedMovie2.Year, expectedMovie2.MainActor, movieToUpdate2.Title, movieToUpdate2.MovieMaker, movieToUpdate2.Director }
                });
        }
    }
}