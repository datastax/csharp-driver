using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Cassandra.Tests.TestHelpers;
using Moq;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TabletMapTests
    {
        private TabletMap _tabletMap;
        private Guid _hostId1;
        private Guid _hostId2;

        [SetUp]
        public void SetUp()
        {
            var config = new TestConfigurationBuilder
            {
                ClientOptions = new ClientOptions(false, 12345, null)
            }.Build();
            var metadata = new Metadata(config);
            _tabletMap = new TabletMap(metadata, new ConcurrentDictionary<TabletMap.KeyspaceTableNamePair, TabletMap.TabletSet>(), new Hosts());
            _hostId1 = Guid.NewGuid();
            _hostId2 = Guid.NewGuid();
        }

        private Tablet CreateTablet(long firstToken, long lastToken, Guid hostId)
        {
            var replicas = new List<HostShardPair> { new HostShardPair(hostId, 0) };
            return new Tablet(firstToken, lastToken, replicas);
        }

        [Test]
        public void RemoveTableMappings_RemovesOnlyMatchingTablets()
        {
            var keyspace = "ks";
            var table = "tbl";
            var tablet1 = CreateTablet(0, 100, _hostId1);
            var tablet2 = CreateTablet(101, 200, _hostId2);

            _tabletMap.AddTablet(keyspace, table, tablet1);
            _tabletMap.AddTablet(keyspace, table, tablet2);
            var host = TestHostFactory.Create(_hostId1);
            _tabletMap.RemoveTableMappings(host);

            var mapping = _tabletMap.GetMapping();
            var key = new TabletMap.KeyspaceTableNamePair(keyspace, table);
            Assert.IsTrue(mapping.ContainsKey(key));
            Assert.AreEqual(1, mapping[key].Tablets.Count);
        }

        [Test]
        public void RemoveTableMappings_RemovesMappingIfNoTabletsLeft()
        {
            var keyspace = "ks";
            var table = "tbl";
            var tablet = CreateTablet(0, 100, _hostId1);

            _tabletMap.AddTablet(keyspace, table, tablet);
            var host = TestHostFactory.Create(_hostId1);
            _tabletMap.RemoveTableMappings(host);

            var mapping = _tabletMap.GetMapping();
            var key = new TabletMap.KeyspaceTableNamePair(keyspace, table);
            Assert.IsFalse(mapping.ContainsKey(key));
        }

        [Test]
        public void RemoveTableMappingsByKeyspace_RemovesCorrectTablets()
        {
            var keyspace1 = "ks";
            var keyspace2 = "ks2";
            var table = "tbl";
            var tablet1 = CreateTablet(0, 100, _hostId1);
            var tablet2 = CreateTablet(101, 200, _hostId2);
            var tablet3 = CreateTablet(201, 300, _hostId1);
            var tablet4 = CreateTablet(301, 400, _hostId2);

            _tabletMap.AddTablet(keyspace1, table, tablet1);
            _tabletMap.AddTablet(keyspace1, table, tablet2);
            _tabletMap.AddTablet(keyspace2, table, tablet3);
            _tabletMap.AddTablet(keyspace2, table, tablet4);
            _tabletMap.RemoveTableMappings(keyspace1);

            var mapping = _tabletMap.GetMapping();
            var key = new TabletMap.KeyspaceTableNamePair(keyspace1, table);
            Assert.IsFalse(mapping.ContainsKey(key), "Mapping should be empty after removing all tablets for the keyspace");
            key = new TabletMap.KeyspaceTableNamePair(keyspace2, table);
            Assert.IsTrue(mapping.ContainsKey(key), "Mapping for keyspace2 should still exist after removing keyspace1 tablets.");
            Assert.AreEqual(2, mapping[key].Tablets.Count, "There should be 2 tablets left for keyspace2 after removing keyspace1 tablets.");
        }

        [Test]
        public void RemoveTableMappingsByKeyspaceAndTable_RemovesCorrectTablets()
        {
            var keyspace = "ks";
            var table1 = "tbl";
            var table2 = "tbl2";
            var tablet1 = CreateTablet(0, 100, _hostId1);
            var tablet2 = CreateTablet(101, 200, _hostId2);
            var tablet3 = CreateTablet(201, 300, _hostId1);
            var tablet4 = CreateTablet(301, 400, _hostId2);

            _tabletMap.AddTablet(keyspace, table1, tablet1);
            _tabletMap.AddTablet(keyspace, table1, tablet2);
            _tabletMap.AddTablet(keyspace, table2, tablet3);
            _tabletMap.AddTablet(keyspace, table2, tablet4);
            _tabletMap.RemoveTableMappings(keyspace, table1);

            var mapping = _tabletMap.GetMapping();
            var key = new TabletMap.KeyspaceTableNamePair(keyspace, table1);
            Assert.IsFalse(mapping.ContainsKey(key), "Mapping should be empty after removing all tablets for the keyspace and table1.");
            key = new TabletMap.KeyspaceTableNamePair(keyspace, table2);
            Assert.IsTrue(mapping.ContainsKey(key), "Mapping for table2 should still exist after removing table1 tablets.");
            Assert.AreEqual(2, mapping[key].Tablets.Count, "There should be 2 tablets left for table2 after removing table1 tablets.");
        }
    }
}
