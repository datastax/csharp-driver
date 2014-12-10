using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.Tests
{
    [Category("short")]
    public class Count : TestGlobals
    {
        ISession _session = null;
        private List<AllDataTypesEntity> _entityList = AllDataTypesEntity.GetDefaultAllDataTypesList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            _entityList = AllDataTypesEntity.SetupDefaultTable(_session);

        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        [Test]
        public void LinqCount_Sync()
        {
            var table = _session.GetTable<AllDataTypesEntity>();
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
        }

        [Test]
        public void LinqCount_Where_Sync()
        {
            var table = _session.GetTable<AllDataTypesEntity>();
            AllDataTypesEntity expectedEntity = _entityList[1];
            long count = table.Where(actualEntity => actualEntity.StringType == expectedEntity.StringType && actualEntity.GuidType == expectedEntity.GuidType).Count().ExecuteAsync().Result;
            Assert.AreEqual(1, count);
        }

        [Test]
        public void LinqCount_Async()
        {
            var table = _session.GetTable<AllDataTypesEntity>();
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
        }

        [Test]
        public void LinqCount_Where_Async()
        {
            var table = _session.GetTable<AllDataTypesEntity>();
            AllDataTypesEntity expectedEntity = _entityList[2];
            long count = table.Where(e => e.StringType == expectedEntity.StringType && e.GuidType == expectedEntity.GuidType).Count().ExecuteAsync().Result;
            Assert.AreEqual(1, count);
        }


    }
}
