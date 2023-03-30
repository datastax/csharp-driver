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
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class First : SharedClusterTest
    {
        ISession _session;
        private List<Movie> _movieList;
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;
        private Mapper _mapper;
        private string _selectAllDefaultCql = "SELECT * from movie";

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
            
            Session.Execute(string.Format(PocoWithEnumCollections.DefaultCreateTableCql, "tbl_with_enum_collections"));

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
            string cqlToFindNothing = $"{_selectAllDefaultCql} where moviemaker =\'{Randomm.RandomAlphaNum(20)}\'";
            try
            {
                _mapper.FirstAsync<Movie>(cqlToFindNothing).Wait();
            }
            catch (AggregateException e)
            {
                Assert.NotNull(e.InnerException);
                Assert.AreEqual("Sequence contains no elements", e.InnerException.Message);
            }
        }

        [Test]
        public void First_With_Enum_Collections()
        {
            var expectedCollection = new[]{ HairColor.Blonde, HairColor.Gray };
            var expectedMap = new SortedDictionary<HairColor, TimeUuid>
            {
                { HairColor.Brown, TimeUuid.NewId() },
                { HairColor.Red, TimeUuid.NewId() }
            };
            var collectionValues = expectedCollection.Select(x => (int)x).ToArray();
            var mapValues =
                new SortedDictionary<int, Guid>(expectedMap.ToDictionary(kv => (int) kv.Key, kv => (Guid) kv.Value));

            const string insertQuery =
                "INSERT INTO tbl_with_enum_collections (id, list1, list2, array1, set1, set2, set3, map1, map2, map3)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            Session.Execute(new SimpleStatement(insertQuery, 2000L, collectionValues, collectionValues,
                collectionValues, collectionValues, collectionValues, collectionValues, mapValues, mapValues,
                mapValues));
            Session.Execute(new SimpleStatement(insertQuery, 2001L, null, null, null, null, null, null, null, null,
                null));
            
            var config =
                new MappingConfiguration().Define(
                    PocoWithEnumCollections.DefaultMapping.TableName("tbl_with_enum_collections"));
            var mapper = new Mapper(Session, config);
            var result = mapper
                .First<PocoWithEnumCollections>("SELECT * FROM tbl_with_enum_collections WHERE id = ?", 2000L);
            Assert.NotNull(result);
            Assert.AreEqual(2000L, result.Id);
            Assert.AreEqual(expectedCollection, result.List1);
            Assert.AreEqual(expectedCollection, result.List2);
            Assert.AreEqual(expectedCollection, result.Array1);
            Assert.AreEqual(expectedCollection, result.Set1);
            Assert.AreEqual(expectedCollection, result.Set2);
            Assert.AreEqual(expectedCollection, result.Set3);
            Assert.AreEqual(expectedMap, result.Dictionary1);
            Assert.AreEqual(expectedMap, result.Dictionary2);
            Assert.AreEqual(expectedMap, result.Dictionary3);
            
            result = mapper
                .First<PocoWithEnumCollections>("SELECT * FROM tbl_with_enum_collections WHERE id = ?", 2001L);
            Assert.NotNull(result);
            Assert.AreEqual(2001L, result.Id);
            Assert.AreEqual(new HairColor[0], result.List1);
            Assert.AreEqual(new HairColor[0], result.List2);
            Assert.AreEqual(new HairColor[0], result.Array1);
            Assert.AreEqual(new HairColor[0], result.Set1);
            Assert.AreEqual(new HairColor[0], result.Set2);
            Assert.AreEqual(new HairColor[0], result.Set3);
            Assert.AreEqual(new Dictionary<HairColor, TimeUuid>(), result.Dictionary1);
            Assert.AreEqual(new Dictionary<HairColor, TimeUuid>(), result.Dictionary2);
            Assert.AreEqual(new Dictionary<HairColor, TimeUuid>(), result.Dictionary3);
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
