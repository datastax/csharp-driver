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
    /// <summary>
    /// NOTE: The GetTable() method is deprecated.  These tests may need to be removed.
    /// </summary>
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
        [Test, NUnit.Framework.Ignore("Shouldn't be using the 'GetTable' method")]
        public void LinqTable_GetTable()
        {
            // Test
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);
        }

    }
}
