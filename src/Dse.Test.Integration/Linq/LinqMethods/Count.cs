//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Linq;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.LinqMethods
{
    [Category("short")]
    public class Count : SharedClusterTest
    {
        ISession _session;
        private List<AllDataTypesEntity> _entityList = AllDataTypesEntity.GetDefaultAllDataTypesList();
        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

        [SetUp]
        public void SetupTest()
        {
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            _entityList = AllDataTypesEntity.SetupDefaultTable(_session);

        }

        [TearDown]
        public void TeardownTest()
        {
            TestUtils.TryToDeleteKeyspace(_session, _uniqueKsName);
        }

        [Test]
        public void LinqCount_Sync()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
        }

        [Test]
        public void LinqCount_Where_Sync()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            AllDataTypesEntity expectedEntity = _entityList[1];
            long count = table.Where(e => e.StringType == expectedEntity.StringType && e.GuidType == expectedEntity.GuidType).Count().Execute();
            Assert.AreEqual(1, count);
        }

        [Test]
        public void LinqCount_Async()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
        }

        [Test]
        public void LinqCount_Where_Async()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            AllDataTypesEntity expectedEntity = _entityList[2];
            long count = table.Where(e => e.StringType == expectedEntity.StringType && e.GuidType == expectedEntity.GuidType).Count().ExecuteAsync().Result;
            Assert.AreEqual(1, count);
        }


    }
}
