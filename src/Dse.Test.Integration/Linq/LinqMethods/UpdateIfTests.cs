//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class UpdateIfTests : SharedClusterTest
    {
        private ISession _session;
        private Table<Movie> _movieTable;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            var uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(uniqueKsName);
            _session.ChangeKeyspace(uniqueKsName);

            _movieTable = new Table<Movie>(_session, new MappingConfiguration());
            _movieTable.Create();
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void LinqTable_UpdateIf_AppliedInfo_Test()
        {
            _movieTable.CreateIfNotExists();
            var movie = new Movie()
            {
                Title = "Dead Poets Society",
                Year = 1989,
                MainActor = "Robin Williams",
                Director = "Peter Weir",
                MovieMaker = "Touchstone"
            };
            _movieTable.Insert(movie).SetConsistencyLevel(ConsistencyLevel.Quorum).Execute();

            var retrievedMovie = _movieTable
                .FirstOrDefault(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone")
                .Execute();
            Movie.AssertEquals(movie, retrievedMovie);
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual(1989, retrievedMovie.Year);
            Assert.AreEqual("Robin Williams", retrievedMovie.MainActor);

            var appliedInfo = _movieTable
                .Where(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new Movie { MainActor = "Robin McLaurin Williams" })
                .UpdateIf(m => m.Year == 1989)
                .Execute();
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);

            retrievedMovie = _movieTable
                .FirstOrDefault(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone")
                .Execute();
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual(1989, retrievedMovie.Year);
            Assert.AreEqual("Robin McLaurin Williams", retrievedMovie.MainActor);

            //Should not update as the if clause is not satisfied
            var updateIf = _movieTable
                .Where(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new Movie { MainActor = "WHOEVER" })
                .UpdateIf(m => m.Year == 1500);

            appliedInfo = updateIf.Execute();
            Assert.False(appliedInfo.Applied);
            Assert.AreEqual(1989, appliedInfo.Existing.Year);
            retrievedMovie = _movieTable
                .FirstOrDefault(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone")
                .Execute();
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual("Robin McLaurin Williams", retrievedMovie.MainActor);
        }
    }
}
