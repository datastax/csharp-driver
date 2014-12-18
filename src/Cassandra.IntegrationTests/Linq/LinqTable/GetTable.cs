using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqTable
{
    [Category("short")]
    public class GetTable : TestGlobals
    {
        ISession _session = null;
        private readonly Logger _logger = new Logger(typeof(global::Cassandra.IntegrationTests.Linq.LinqTable.CreateTable));
        string _uniqueKsName;

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            TestUtils.WaitForSchemaAgreement(_session.Cluster);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Get table using GetTable, validate that the resultant table object functions as expected
        /// </summary>
        [Test]
        public void LinqTable_GetTable()
        {
            // Test
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);
        }

        /// <summary>
        /// Get table using GetTable table, validate that the resultant table object functions as expected
        /// </summary>
        [Test]
        public void LinqTable_GetTable_GetTwice()
        {
            // Test
            Table<AllDataTypesEntity> table;
            table = _session.GetTable<AllDataTypesEntity>();
            table = _session.GetTable<AllDataTypesEntity>();
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);
        }

        /// <summary>
        /// When the keyspace is deleted and then a new table is obtained using a new keyspace, 
        /// this causes an infinite loop when doing a select after a prepared statement was bound and executed.
        /// </summary>
        [Test]
        public void LinqTable_GetTable_DeleteKeyspaceThenRepeat()
        {
            // Do the initial write / read test
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);
            _session.DeleteKeyspace(_uniqueKsName);

            // Now recreate the keyspace, repeat the write/read test
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            TestUtils.WaitForSchemaAgreement(_session.Cluster);
            _session.ChangeKeyspace(_uniqueKsName);

            // Test
            table = _session.GetTable<AllDataTypesEntity>();
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);
        }

    }
}
