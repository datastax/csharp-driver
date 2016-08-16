﻿using System;
using System.Collections.Generic;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class First : SharedClusterTest
    {
        ISession _session = null;
        private List<Movie> _movieList;
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;
        private Mapper _mapper;
        private string _selectAllDefaultCql = "SELECT * from movie";

        protected override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var config = new Map<Movie>().PartitionKey(c => c.Title).PartitionKey(c => c.MovieMaker);
            var mappingConfig = new MappingConfiguration().Define(config);
            _mapper = new Mapper(_session, mappingConfig);
            _movieTable = new Table<Movie>(_session, mappingConfig);
            _movieTable.Create();

            //Insert some data
            _movieList = Movie.GetDefaultMovieList();
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        [Test]
        public void First_Sync()
        {
            var actualMovie = _mapper.First<Movie>(_selectAllDefaultCql);
            Movie.AssertListContains(_movieList, actualMovie);
        }

        [Test]
        public void First_Async()
        {
            var actualMovie = _mapper.FirstAsync<Movie>(_selectAllDefaultCql).Result;
            Movie.AssertListContains(_movieList, actualMovie);
        }

        [Test]
        public void First_NoSuchRecord()
        {
            string cqlToFindNothing = _selectAllDefaultCql + " where moviemaker ='" + Randomm.RandomAlphaNum(20) + "'";
            var err = Assert.Throws<InvalidOperationException>(() => _mapper.First<Movie>(cqlToFindNothing));
            Assert.AreEqual("Sequence contains no elements", err.Message);
        }

        [Test]
        public void First_Async_NoSuchRecord()
        {
            string cqlToFindNothing = _selectAllDefaultCql + " where moviemaker ='" + Randomm.RandomAlphaNum(20) + "'";
            try
            {
                _mapper.FirstAsync<Movie>(cqlToFindNothing).Wait();
            }
            catch (AggregateException e)
            {
                Assert.AreEqual("Sequence contains no elements", e.InnerException.Message);
            }
        }

        ///////////////////////////////////////////////
        /// Exceptions
        ///////////////////////////////////////////////

        [Test]
        public void First_InvalidCql()
        {
            string cqlToFindNothing = _selectAllDefaultCql + " where this is invalid cql";
            Assert.Throws<SyntaxError>(() =>_mapper.First<Movie>(cqlToFindNothing));
        }

        [Test]
        public void First_NoPartitionKey()
        {
            string cqlToFindNothing = _selectAllDefaultCql + " where year = 1234";
            Assert.Throws<InvalidQueryException>(() => _mapper.First<Movie>(cqlToFindNothing));
        }

        [Test]
        public void First_MissingPartitionKey()
        {
            string bunkCql = _selectAllDefaultCql + " where title ='doesntmatter'";
            Assert.Throws<InvalidQueryException>(() => _mapper.First<Movie>(bunkCql));
        }


    }
}
