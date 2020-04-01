//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using NUnit.Framework;
#pragma warning disable 612

namespace Dse.Test.Integration.Linq.LinqMethods
{
    public class UpdateIfTests : SimulacronTest
    {
        public override void SetUp()
        {
            base.SetUp();
            var uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            Session.ChangeKeyspace(uniqueKsName);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void LinqTable_UpdateIf_AppliedInfo_Test()
        {
            var movieTable = new Table<Movie>(Session, new MappingConfiguration());

            var movie = new Movie
            {
                Title = "Dead Poets Society",
                Year = 1989,
                MainActor = "Robin Williams",
                Director = "Peter Weir",
                MovieMaker = "Touchstone"
            };
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"UPDATE \"{Movie.TableName}\" " +
                          "SET \"mainGuy\" = ? " +
                          "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ? IF \"yearMade\" = ?",
                          when => when.WithParams("Robin McLaurin Williams", movie.Title, movie.MovieMaker, movie.Director, movie.Year))
                      .ThenRowsSuccess(Movie.CreateAppliedInfoRowsResultWithoutMovie(true)));

            var appliedInfo = movieTable
                .Where(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new Movie { MainActor = "Robin McLaurin Williams" })
                .UpdateIf(m => m.Year == 1989)
                .Execute();
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);

            var existingMovie = new Movie
            {
                Title = "Dead Poets Society",
                Year = 1989,
                MainActor = "Robin McLaurin Williams",
                Director = "Peter Weir",
                MovieMaker = "Touchstone"
            };
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"UPDATE \"{Movie.TableName}\" " +
                          "SET \"mainGuy\" = ? " +
                          "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ? IF \"yearMade\" = ?",
                          when => when.WithParams("WHOEVER", movie.Title, movie.MovieMaker, movie.Director, 1500))
                      .ThenRowsSuccess(existingMovie.CreateAppliedInfoRowsResult()));
            
            //Should not update as the if clause is not satisfied
            var updateIf = movieTable
                .Where(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new Movie { MainActor = "WHOEVER" })
                .UpdateIf(m => m.Year == 1500);

            appliedInfo = updateIf.Execute();
            Assert.False(appliedInfo.Applied);
            Assert.AreEqual(1989, appliedInfo.Existing.Year);
        }
    }
}
