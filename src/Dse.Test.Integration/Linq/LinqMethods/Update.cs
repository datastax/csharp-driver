//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Update : SharedClusterTest
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            MappingConfiguration movieMappingConfig = new MappingConfiguration();
            movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            _movieTable = new Table<Movie>(_session, movieMappingConfig);
            _movieTable.Create();

            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        /// <summary>
        /// Successfully update multiple records using a single (non-batch) update
        /// </summary>
        [Test]
        public void LinqUpdate_Single()
        {
            // Setup
            Table<Movie> table = new Table<Movie>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            Movie movieToUpdate = _movieList[1];

            var expectedMovie = new Movie(movieToUpdate.Title, movieToUpdate.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate.MovieMaker, 1212);
            table.Where(m => m.Title == movieToUpdate.Title && m.MovieMaker == movieToUpdate.MovieMaker && m.Director == movieToUpdate.Director)
                 .Select(m => new Movie { Year = expectedMovie.Year, MainActor = expectedMovie.MainActor })
                 .Update()
                 .Execute();

            List<Movie> actualMovieList = table.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie));
            Movie.AssertListContains(actualMovieList, expectedMovie);
        }

        /// <summary>
        /// Successfully update multiple records using a single (non-batch) update, using async execute
        /// </summary>
        [Test]
        public void LinqUpdate_Single_Async()
        {
            // Setup
            Table<Movie> table = new Table<Movie>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            Movie movieToUpdate = _movieList[1];

            var expectedMovie = new Movie(movieToUpdate.Title, movieToUpdate.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate.MovieMaker, 1212);
            table.Where(m => m.Title == movieToUpdate.Title && m.MovieMaker == movieToUpdate.MovieMaker && m.Director == movieToUpdate.Director)
                 .Select(m => new Movie { Year = expectedMovie.Year, MainActor = expectedMovie.MainActor })
                 .Update()
                 .Execute();

            List<Movie> actualMovieList = table.ExecuteAsync().Result.ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie));
            Movie.AssertListContains(actualMovieList, expectedMovie);
        }

        /// <summary>
        /// Successfully update multiple records using a Batch update
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void LinqUpdate_Batch()
        {
            // Setup
            Table<Movie> table = new Table<Movie>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            Movie movieToUpdate1 = _movieList[1];
            Movie movieToUpdate2 = _movieList[2];

            BatchStatement batch = new BatchStatement();

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

            List<Movie> actualMovieList = table.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Assert.AreNotEqual(expectedMovie1.MainActor, expectedMovie2.MainActor);
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie1));
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie2));
            Movie.AssertListContains(actualMovieList, expectedMovie1);
            Movie.AssertListContains(actualMovieList, expectedMovie2);
        }




        public class ExtMovie
        {
            public int Size;
            public string TheDirector;
            public string TheMaker;
        }


    }
}
