//
//  Copyright (C) DataStax, Inc.
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
using Dse.Test.Integration.SimulacronAPI;
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.LinqMethods
{
    public class Count : SimulacronTest
    {
        private readonly List<AllDataTypesEntity> _entityList = AllDataTypesEntity.GetDefaultAllDataTypesList();

        [Test]
        public void LinqCount_Sync()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT count(*) FROM \"{AllDataTypesEntity.TableName}\" ALLOW FILTERING")
                      .ThenRowsSuccess(new[] { ("count", DataType.BigInt) }, rows => rows.WithRow(_entityList.Count)));
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
        }

        [Test]
        public void LinqCount_Where_Sync()
        {
            AllDataTypesEntity expectedEntity = _entityList[1];
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT count(*) FROM \"{AllDataTypesEntity.TableName}\" " +
                          "WHERE \"string_type\" = ? AND \"guid_type\" = ? " +
                          "ALLOW FILTERING",
                          when => when.WithParam(DataType.Ascii, expectedEntity.StringType)
                                      .WithParam(DataType.Uuid, expectedEntity.GuidType))
                      .ThenRowsSuccess(new[] { ("count", DataType.BigInt) }, rows => rows.WithRow(1)));
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            long count = table.Where(e => e.StringType == expectedEntity.StringType && e.GuidType == expectedEntity.GuidType).Count().Execute();
            Assert.AreEqual(1, count);
        }

        [Test, TestCassandraVersion(3, 0)]
        public void LinqCount_Where_Sync_AllowFiltering()
        {
            var mapping = new MappingConfiguration();
            mapping.Define(new Map<Tweet>()
                .ExplicitColumns()
                .Column(t => t.AuthorId)
                .Column(t => t.TweetId)
                .Column(t => t.Body)
                .PartitionKey(t => t.AuthorId)
                .ClusteringKey(t => t.TweetId)
                .TableName("tweets"));
            var table = new Table<Tweet>(Session, mapping);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT count(*) FROM tweets WHERE Body = ? ALLOW FILTERING",
                          when => when.WithParam(DataType.Ascii, "a lot"))
                      .ThenRowsSuccess(new[] { ("count", DataType.BigInt) }, rows => rows.WithRow(3)));

            var count = table.Where(e => e.Body == "a lot").AllowFiltering().Count().Execute();
            Assert.AreEqual(3, count);
        }

        [Test]
        public void LinqCount_Async()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT count(*) FROM \"{AllDataTypesEntity.TableName}\" ALLOW FILTERING")
                      .ThenRowsSuccess(new[] { ("count", DataType.BigInt) }, rows => rows.WithRow(_entityList.Count)));
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
        }

        [Test]
        public void LinqCount_Where_Async()
        {
            AllDataTypesEntity expectedEntity = _entityList[2];
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT count(*) FROM \"{AllDataTypesEntity.TableName}\" " +
                          "WHERE \"string_type\" = ? AND \"guid_type\" = ? " +
                          "ALLOW FILTERING",
                          when => when.WithParam(DataType.Ascii, expectedEntity.StringType)
                                      .WithParam(DataType.Uuid, expectedEntity.GuidType))
                      .ThenRowsSuccess(new[] { ("count", DataType.BigInt) }, rows => rows.WithRow(1)));
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            long count = table.Where(e => e.StringType == expectedEntity.StringType && e.GuidType == expectedEntity.GuidType).Count().ExecuteAsync().Result;
            Assert.AreEqual(1, count);
        }
    }
}