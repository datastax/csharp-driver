using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class Single : SharedClusterTest
    {
        ISession _session = null;
        private List<Movie> _movieList;
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;
        private Mapper _mapper;
        private string _selectAllDefaultCql = "SELECT * from movie";

        protected override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
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

        /// <summary>
        /// Validate the Linq method Single
        /// 
        /// @test_category queries:basic
        /// </summary>
        [Test]
        public void Single_Sync()
        {
            Movie expectedMovie = _movieList.First();

            string cqlStr = _selectAllDefaultCql + " where moviemaker ='" + expectedMovie.MovieMaker + "'";
            Movie actualMovie = _mapper.Single<Movie>(cqlStr);
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        /// <summary>
        /// Validate the Linq method Single, Asynchronous
        /// 
        /// @test_category queries:basic,async
        /// </summary>
        [Test]
        public void Single_Async()
        {
            Movie expectedMovie = _movieList.First();

            string cqlStr = _selectAllDefaultCql + " where moviemaker ='" + expectedMovie.MovieMaker + "'";
            Movie actualMovie = _mapper.SingleAsync<Movie>(cqlStr).Result;
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        /// <summary>
        /// Attempt to use the Linq method Single when there are multiple entities in the data set
        /// 
        /// @test_category queries:basic
        /// @expected_errors InvalidOperationException
        /// </summary>
        [Test]
        public void Single_Sync_SequenceContainsMoreThanOneElement()
        {
            Assert.Throws<InvalidOperationException>(() => _mapper.Single<Movie>(_selectAllDefaultCql));
        }

        /// <summary>
        /// Attempt to use the Linq method Single when there are multiple entities in the data set
        /// using asynchronous execution
        /// 
        /// @test_category queries:basic,async
        /// @expected_errors AggregateException
        /// </summary>
        [Test]
        public void Single_Async_SequenceContainsMoreThanOneElement()
        {
            Assert.Throws<AggregateException>(() => _mapper.SingleAsync<Movie>(_selectAllDefaultCql).Wait());
        }

        /// <summary>
        /// Attempt to use the Linq method Single when no records were returned
        /// 
        /// @test_category queries:basic
        /// @expected_errors InvalidOperationException
        /// </summary>
        [Test]
        public void Single_NoSuchRecord()
        {
            string cqlToFindNothing = _selectAllDefaultCql + " where moviemaker ='" + Randomm.RandomAlphaNum(20) + "'";
            Assert.Throws<InvalidOperationException>(() => _mapper.Single<Movie>(cqlToFindNothing));
        }

        /// <summary>
        /// Attempt to use the Linq method Single when no records were returned,
        /// using asynchronous execution
        /// 
        /// @test_category queries:basic,async
        /// @expected_errors InvalidOperationException
        /// </summary>
        [Test]
        public void Single_Async_NoSuchRecord()
        {
            string cqlToFindNothing = _selectAllDefaultCql + " where moviemaker ='" + Randomm.RandomAlphaNum(20) + "'";
            Assert.Throws<AggregateException>(() => _mapper.SingleAsync<Movie>(cqlToFindNothing).Wait());
        }

        ///////////////////////////////////////////////
        /// Exceptions
        ///////////////////////////////////////////////

        /// <summary>
        /// Attempt to use the Linq method Single, passing in invalid CQL
        /// 
        /// @test_category queries:basic
        /// @expected_errors SyntaxError
        /// </summary>
        [Test]
        public void Single_InvalidCql()
        {
            string cqlToFindNothing = _selectAllDefaultCql + " where this is invalid cql";
            Assert.Throws<SyntaxError>(() => _mapper.Single<Movie>(cqlToFindNothing));
        }

        /// <summary>
        /// Attempt to use the Linq method Single, passing in cql with partition key omitted
        /// 
        /// @test_category queries:basic
        /// @expected_errors InvalidQueryException
        /// </summary>
        [Test]
        public void Single_NoPartitionKey()
        {
            string cqlToFindNothing = _selectAllDefaultCql + " where year = 1234";
            Assert.Throws<InvalidQueryException>(() => _mapper.Single<Movie>(cqlToFindNothing));
        }

        /// <summary>
        /// Attempt to use the Linq method Single, passing in cql with partition key missing
        /// 
        /// @test_category queries:basic
        /// @expected_errors InvalidQueryException
        /// </summary>
        [Test]
        public void Single_MissingPartitionKey()
        {
            string bunkCql = _selectAllDefaultCql + " where title ='doesntmatter'";

            try
            {
                _mapper.Single<Movie>(bunkCql);
                Assert.Fail("expected exception was not thrown!");
            }
            catch (InvalidQueryException e)
            {
                string expectedErrMsg = null;
                if (_session.BinaryProtocolVersion < 3)
                    expectedErrMsg = "No indexed columns present in by-columns clause with Equal operator"; 
                else
                    expectedErrMsg = "No secondary indexes on the restricted columns support the provided operators: ";

                StringAssert.IsMatch(expectedErrMsg, e.Message);
            }
        }


    }
}
