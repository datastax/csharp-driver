//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class InsertTests : SharedClusterTest
    {
        private ISession _session;
        private string _uniqueKsName;

        protected override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            _session = Session;
        }

        [SetUp]
        public void TestSetup()
        {
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping
        /// </summary>
        [Test]
        public void Insert_Sync()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>()
                .TableName(TestUtils.GetUniqueTableName().ToLowerInvariant()).PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            // Insert using Mapper.Insert
            lowercaseclassnamepklowercase privateClassInstance = new lowercaseclassnamepklowercase();
            var mapper = new Mapper(_session, mappingConfig);
            mapper.Insert(privateClassInstance);
            List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>().ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepklowercase defaultInstance = new lowercaseclassnamepklowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping, inserting asynchronously
        /// </summary>
        [Test]
        public void Insert_Async()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>().PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            // Insert using Mapper.Insert
            lowercaseclassnamepklowercase privateClassInstance = new lowercaseclassnamepklowercase();
            var mapper = new Mapper(_session, mappingConfig);
            mapper.InsertAsync(privateClassInstance).Wait();

            // Validate data in C*
            List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>().ToList();
            DateTime futureDateTime = DateTime.Now.AddSeconds(2);
            while (instancesQueried.Count < 1 && futureDateTime > DateTime.Now)
            {
                instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>().ToList();
            }
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepklowercase defaultInstance = new lowercaseclassnamepklowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping,
        /// including every acceptable consistency level
        /// </summary>
        [Test]
        public void Insert_WithConsistency_Success()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>()
                .TableName(TestUtils.GetUniqueTableName().ToLowerInvariant()).PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            table.Create();
            var mapper = new Mapper(_session, mappingConfig);

            // Insert the data
            var consistencyLevels = new []
            {
                ConsistencyLevel.All,
                ConsistencyLevel.Any,
                ConsistencyLevel.EachQuorum,
                ConsistencyLevel.LocalOne,
                ConsistencyLevel.LocalQuorum,
                ConsistencyLevel.Quorum,
            };
            foreach (var consistencyLevel in consistencyLevels)
            {
                lowercaseclassnamepklowercase pocoInstance = new lowercaseclassnamepklowercase();
                pocoInstance.somepartitionkey = Guid.NewGuid().ToString();
                mapper.Insert(pocoInstance, new CqlQueryOptions().SetConsistencyLevel(consistencyLevel));

                // Assert final state of C* data
                string cql = "Select * from " + table.Name + " where somepartitionkey ='" + pocoInstance.somepartitionkey + "'";
                List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>(cql).ToList();
                DateTime futureDateTime = DateTime.Now.AddSeconds(2);
                while (instancesQueried.Count < 1 && futureDateTime > DateTime.Now)
                {
                    instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>(cql).ToList();
                }
                Assert.AreEqual(1, instancesQueried.Count, "Unexpected failure for consistency level: " + consistencyLevel);
                Assert.AreEqual(pocoInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
            }
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping,
        /// including a consistency level of 'Serial'
        /// Validate expected error message
        /// </summary>
        [Test]
        public void Insert_WithConsistency_Serial()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>()
                .TableName(TestUtils.GetUniqueTableName().ToLowerInvariant()).PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            table.Create();

            // Insert the data
            var mapper = new Mapper(_session, mappingConfig);
            lowercaseclassnamepklowercase pocoInstance = new lowercaseclassnamepklowercase();

            // Assert final state of C* data
            var err = Assert.Throws<RequestInvalidException>(
                () => mapper.Insert(pocoInstance, new CqlQueryOptions().SetConsistencyLevel(ConsistencyLevel.Serial)));
            Assert.AreEqual("Serial consistency specified as a non-serial one.", err.Message);
            List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>().ToList();
            Assert.AreEqual(0, instancesQueried.Count);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping,
        /// including consistency levels that will cause the request to fail silently.
        /// </summary>
        [Test]
        public void Insert_WithConsistencyLevel_Fail()
        {
            // Setup
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var mappingConfig =
                new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>().PartitionKey(c => c.somepartitionkey).TableName(tableName).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            table.Create();

            // Insert the data
            var consistencyLevels = new []
            {
                ConsistencyLevel.Three,
                ConsistencyLevel.Two
            };
            var mapper = new Mapper(_session, mappingConfig);
            foreach (var consistencyLevel in consistencyLevels)
            {
                lowercaseclassnamepklowercase privateClassInstance = new lowercaseclassnamepklowercase();
                Assert.Throws<UnavailableException>(() => mapper.Insert(privateClassInstance, new CqlQueryOptions().SetConsistencyLevel(consistencyLevel)));
            }
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping, inserting asynchronously
        /// including a consistency level that one greater than the current node count
        /// </summary>
        [Test]
        public void Insert_Async_WithConsistency_OneMoreCopyThanNodeCount()
        {
            // Setup
            var mappingConfig =
                new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>().PartitionKey(c => c.somepartitionkey).CaseSensitive());

            // Insert the data
            lowercaseclassnamepklowercase privateClassInstance = new lowercaseclassnamepklowercase();
            var mapper = new Mapper(_session, mappingConfig);
            Assert.Throws<AggregateException>(() => mapper.InsertAsync(privateClassInstance, new CqlQueryOptions().SetConsistencyLevel(ConsistencyLevel.Two)).Wait());
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping, using Mapper.Insert
        /// </summary>
        [Test]
        public void Insert_WithMapperInsert_TwoPartitionKeys_OnlyOne()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<ClassWithTwoPartitionKeys>()
                .TableName(typeof (ClassWithTwoPartitionKeys).Name).CaseSensitive()
                .PartitionKey(new string[] {"PartitionKey1", "PartitionKey2" }).CaseSensitive()
                );
            var table = new Table<ClassWithTwoPartitionKeys>(_session, mappingConfig);
            table.Create();

            // Insert the data
            ClassWithTwoPartitionKeys defaultInstance = new ClassWithTwoPartitionKeys();
            ClassWithTwoPartitionKeys instance = new ClassWithTwoPartitionKeys();
            var mapper = new Mapper(_session, mappingConfig);
            mapper.Insert(instance);

            List<ClassWithTwoPartitionKeys> instancesRetrieved = new List<ClassWithTwoPartitionKeys>();
            DateTime futureDateTime = DateTime.Now.AddSeconds(5);
            while (instancesRetrieved.Count < 1 && DateTime.Now < futureDateTime)
                instancesRetrieved = mapper.Fetch<ClassWithTwoPartitionKeys>("SELECT * from \"" + table.Name + "\"").ToList();
            Assert.AreEqual(1, instancesRetrieved.Count);
            Assert.AreEqual(defaultInstance.PartitionKey1, instancesRetrieved[0].PartitionKey1);
            Assert.AreEqual(defaultInstance.PartitionKey2, instancesRetrieved[0].PartitionKey2);
            instancesRetrieved.Clear();

            futureDateTime = DateTime.Now.AddSeconds(5);
            string cqlSelect = "SELECT * from \"" + table.Name + "\" where \"PartitionKey1\" = '" + instance.PartitionKey1 + "' and \"PartitionKey2\" = '" + instance.PartitionKey2 + "'";
            while (instancesRetrieved.Count < 1 && DateTime.Now < futureDateTime)
                instancesRetrieved = mapper.Fetch<ClassWithTwoPartitionKeys>(cqlSelect).ToList();
            Assert.AreEqual(1, instancesRetrieved.Count);
            Assert.AreEqual(defaultInstance.PartitionKey1, instancesRetrieved[0].PartitionKey1);
            Assert.AreEqual(defaultInstance.PartitionKey2, instancesRetrieved[0].PartitionKey2);

            var err = Assert.Throws<InvalidQueryException>(() => mapper.Fetch<ClassWithTwoPartitionKeys>("SELECT * from \"" + table.Name + "\" where \"PartitionKey1\" = '" + instance.PartitionKey1 + "'"));
            string expectedErrMsg = "Partition key part(s:)? PartitionKey2 must be restricted (since preceding part is|as other parts are)";
            StringAssert.IsMatch(expectedErrMsg, err.Message);

            Assert.Throws<InvalidQueryException>(() => mapper.Fetch<ClassWithTwoPartitionKeys>("SELECT * from \"" + table.Name + "\" where \"PartitionKey2\" = '" + instance.PartitionKey2 + "'"));
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping, 
        /// using Session.Execute to insert an Insert object created with table.Insert()
        /// </summary>
        [Test, TestCassandraVersion(2,0)]
        public void Insert_WithSessionExecuteTableInsert()
        {
            // Setup
            string uniqueTableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>().TableName(uniqueTableName).PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            table.Create();

            // Insert the data
            lowercaseclassnamepklowercase defaultPocoInstance = new lowercaseclassnamepklowercase();
            _session.Execute(table.Insert(defaultPocoInstance));
            var mapper = new Mapper(_session, mappingConfig);
            List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepklowercase defaultInstance = new lowercaseclassnamepklowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
        }

        /// <summary>
        /// Attempt to insert a Poco into a nonexistent table
        /// </summary>
        [Test]
        public void Insert_UnconfiguredTable()
        {
            // Setup
            var mapper = new Mapper(_session, new MappingConfiguration());
            ManyDataTypesPoco manyTypesPoco = ManyDataTypesPoco.GetRandomInstance();

            // Validate Error Message
            var e = Assert.Throws<InvalidQueryException>(() => mapper.Insert(manyTypesPoco));
            string expectedErrMsg = "unconfigured (columnfamily|table) " + typeof(ManyDataTypesPoco).Name.ToLower();
            StringAssert.IsMatch(expectedErrMsg, e.Message);
        }

        /// <summary>
        /// By default Linq preserves class param casing, but cqlpoco does not, 
        /// so expect "unconfigured columnfamily" when trying to insert via cqlpoco using default settings
        /// This also validates that a private class can be used by the CqlPoco client
        /// </summary>
        [Test]
        public void Insert_ClassAndPartitionKeyAreCamelCase()
        {
            var mappingConfig = new MappingConfiguration().Define(new Map<PrivateClassWithClassNameCamelCase>().PartitionKey(c => c.SomePartitionKey));
            Table<PrivateClassWithClassNameCamelCase> table = new Table<PrivateClassWithClassNameCamelCase>(_session, mappingConfig);
            Assert.AreNotEqual(table.Name, table.Name.ToLower());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration());
            PrivateClassWithClassNameCamelCase privateClassCamelCase = new PrivateClassWithClassNameCamelCase();
            mapper.Insert(privateClassCamelCase);

            List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepklowercase defaultInstance = new lowercaseclassnamepklowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);

            Assert.Throws<InvalidQueryException>(() => TestUtils.TableExists(_session, _uniqueKsName, typeof (PrivateClassWithClassNameCamelCase).Name, true));
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, typeof(PrivateClassWithClassNameCamelCase).Name.ToLower(), true));
        }

        /// <summary>
        /// Validate that mapped class properties are lower-cased by default
        /// </summary>
        [Test]
        public void Insert_TableNameLowerCase_PartitionKeyCamelCase()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepkcamelcase>().PartitionKey(c => c.SomePartitionKey));
            Table<lowercaseclassnamepkcamelcase> table = new Table<lowercaseclassnamepkcamelcase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();
            var mapper = new Mapper(_session, new MappingConfiguration());
            lowercaseclassnamepkcamelcase privateClassInstance = new lowercaseclassnamepkcamelcase();

            // Validate state of table
            mapper.Insert(privateClassInstance);
            List<lowercaseclassnamepkcamelcase> instancesQueried = mapper.Fetch<lowercaseclassnamepkcamelcase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepkcamelcase defaultPocoInstance = new lowercaseclassnamepkcamelcase();
            Assert.AreEqual(defaultPocoInstance.SomePartitionKey, instancesQueried[0].SomePartitionKey);

            // Attempt to select from Camel Case partition key
            string cqlCamelCasePartitionKey = "SELECT * from " + typeof (lowercaseclassnamepkcamelcase).Name + " where \"SomePartitionKey\" = 'doesntmatter'";
            var ex = Assert.Throws<InvalidQueryException>(() => _session.Execute(cqlCamelCasePartitionKey));
            string expectedErrMsg = "Undefined name SomePartitionKey in where clause";
            StringAssert.Contains(expectedErrMsg, ex.Message);

            // Validate that select on lower case key does not fail
            string cqlLowerCasePartitionKey = "SELECT * from " + typeof(lowercaseclassnamepkcamelcase).Name + " where \"somepartitionkey\" = '" + defaultPocoInstance.SomePartitionKey + "'";
            List<Row> rows = _session.Execute(cqlLowerCasePartitionKey).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(defaultPocoInstance.SomePartitionKey, rows[0].GetValue<string>("somepartitionkey"));
        }

        /// <summary>
        /// Attempting to insert a Poco into a table with a missing column field fails
        /// </summary>
        [Test]
        public void Insert_MislabledClusteringKey()
        {
            string tableName = typeof(PocoWithAdditionalField).Name.ToLower();
            string createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY)";
            _session.Execute(createTableCql);
            var cqlClient = new Mapper(_session, new MappingConfiguration());
            PocoWithAdditionalField pocoWithCustomAttributes = new PocoWithAdditionalField();

            // Validate expected exception
            var ex = Assert.Throws<InvalidQueryException>(() => cqlClient.Insert(pocoWithCustomAttributes));
            StringAssert.Contains("Unknown identifier someotherstring", ex.Message);
        }

        [Test]
        public void InsertIfNotExists_Applied_Test()
        {
            var config = new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song_insert"));
            //Use linq to create the table
            new Table<Song>(_session, config).Create();
            var mapper = new Mapper(_session, config);
            var song = new Song {Id = Guid.NewGuid(), Artist = "Led Zeppelin", Title = "Good Times Bad Times"};
            //It is the first song there, it should apply it
            var appliedInfo = mapper.InsertIfNotExists(song);
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);
            //Following times, it should not apply the mutation as the partition key is the same
            var nextSong = new Song { Id = song.Id, Title = "Communication Breakdown" };
            appliedInfo = mapper.InsertIfNotExists(nextSong);
            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual(song.Title, appliedInfo.Existing.Title);
        }

        [Test]
        public void Insert_Without_Nulls_Test()
        {
            var config = new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song_insert"));
            //Use linq to create the table
            new Table<Song>(_session, config).CreateIfNotExists();
            var mapper = new Mapper(_session, config);
            var song = new Song 
            { 
                Id = Guid.NewGuid(), 
                Artist = "The Who", 
                Title = "Substitute", 
                ReleaseDate = DateTimeOffset.UtcNow
            };
            mapper.Insert(song);
            var storedSong = mapper.First<Song>("WHERE id = ?", song.Id);
            Assert.AreEqual(song.Artist, storedSong.Artist);
            //do NOT insert nulls
            mapper.Insert(new Song { Id = song.Id, Artist = null, Title = "Substitute 2", ReleaseDate = DateTimeOffset.UtcNow}, false);
            //it should have the new title but the artist should still be the same (not null)
            storedSong = mapper.First<Song>("WHERE id = ?", song.Id);
            Assert.NotNull(storedSong.Artist);
            Assert.AreEqual(song.Artist, storedSong.Artist);
            Assert.AreEqual("Substitute 2", storedSong.Title);
            //Now insert nulls
            mapper.Insert(new Song { Id = song.Id, Artist = null, Title = "Substitute 3", ReleaseDate = DateTimeOffset.UtcNow }, true);
            //it should have the new title and the artist should be null
            storedSong = mapper.First<Song>("WHERE id = ?", song.Id);
            Assert.Null(storedSong.Artist);
            Assert.AreEqual("Substitute 3", storedSong.Title);
        }


        /////////////////////////////////////////
        /// Private test classes
        /////////////////////////////////////////

        private class ClassWithTwoPartitionKeys
        {
            public string PartitionKey1 = "somePartitionKey1";
            public string PartitionKey2 = "somePartitionKey2";
        }

        private class PrivateClassWithClassNameCamelCase
        {
            public string SomePartitionKey = "somePartitionKey";
        }

        private class lowercaseclassnamepkcamelcase
        {
            public string SomePartitionKey = "somePartitionKey";
        }

        private class lowercaseclassnamepklowercase
        {
            public string somepartitionkey = "somePartitionKey";
        }

        private class PocoWithAdditionalField
        {
            public string SomeString = "someStringValue";
            public string SomeOtherString = "someOtherStringValue";
        }


    }
}
