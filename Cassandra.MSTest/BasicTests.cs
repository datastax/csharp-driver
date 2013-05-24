using System;
using System.Collections.Generic;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

#if NET_40_OR_GREATER
using System.Numerics;
#endif  

namespace Cassandra.MSTest
{
    [TestClass]
    public partial class BasicTests
    {
        [TestMethod]        
		[Ignore]//OK
        public void BigInsert()
        {
            BigInsertTest(3000);
        }

        [TestMethod]        
		[Ignore]//OK
        public void creatingSecondaryIndex()
        {
            createSecondaryIndexTest();
        }
        
        [TestMethod]
        public void checkSimpleStrategyKeyspace()
        {
            CreateKeyspaceWithPropertiesTest(ReplicationStrategies.SimpleStrategy);
        }

        [TestMethod]
        public void checkNetworkTopologyStrategyKeyspace()
        {
            CreateKeyspaceWithPropertiesTest(ReplicationStrategies.NetworkTopologyStrategy);
        }

        [TestMethod]        
        public void checkTableMetadata()
        {
            checkMetadata();
        }
        
        [TestMethod]              
        public void checkTableMetadataWithOptions()
        {
            checkMetadata(tableOptions: new TableOptions("Comment",0.5,0.6, true,42,0.01,"ALL",
                new SortedDictionary<string,string>{{"class","org.apache.cassandra.db.compaction.LeveledCompactionStrategy"} , {"sstable_size_in_mb" , "15"}},
                new SortedDictionary<string,string>{{"sstable_compression" , "org.apache.cassandra.io.compress.SnappyCompressor"}, {"chunk_length_kb" , "128"}}));
        }        
        
        [TestMethod]
		[Ignore]//OK
        public void checkKeyspaceMetadata()
        {
            checkKSMetadata();
        }

        [TestMethod]
        public void testCounter()
        {
            testCounters();
        }

        [TestMethod]
        public void testBlob()
        {
            insertingSingleValue(typeof(byte));
        }

        [TestMethod]
        public void testASCII()
        {
            insertingSingleValue(typeof(Char));
        }

        [TestMethod]
        public void testDecimal()
        {
            insertingSingleValue(typeof(Decimal));
        }
#if NET_40_OR_GREATER
        [TestMethod]
        public void testVarInt()
        {
            insertingSingleValue(typeof(BigInteger));
        }
#endif

        [TestMethod]
        public void testBigInt()
        {
            insertingSingleValue(typeof(System.Int64));
        }
        [TestMethod]
        public void testDouble()
        {
            insertingSingleValue(typeof(System.Double));
        }
        [TestMethod]
        public void testFloat()
        {
            insertingSingleValue(typeof(System.Single));
        }
        [TestMethod]
        public void testInt()
        {
            insertingSingleValue(typeof(System.Int32));
        }
        [TestMethod]
        public void testBoolean()
        {
            insertingSingleValue(typeof(System.Boolean));
        }

        [TestMethod]
        public void testUUID()
        {
            insertingSingleValue(typeof(System.Guid));
        }

        [TestMethod]
        public void testTimestamp()
        {
            TimestampTest();
        }

        [TestMethod]
        public void MaxingBoundsOf_INT()
        {
            ExceedingCassandraType(typeof(System.Int32), typeof(System.Int32));
        }
        [TestMethod]
		[Ignore]//OK
        public void MaxingBoundsOf_BIGINT()
        {
            ExceedingCassandraType(typeof(System.Int64), typeof(System.Int64));
        }
        [TestMethod]
		[Ignore]//OK
        public void MaxingBoundsOf_FLOAT()
        {
            ExceedingCassandraType(typeof(System.Single), typeof(System.Single));
        }
        [TestMethod]
		[Ignore]//OK
        public void MaxingBoundsOf_DOUBLE()
        {
            ExceedingCassandraType(typeof(System.Double), typeof(System.Double));
        }

        [TestMethod]
		[Ignore]//OK
        public void ExceedingCassandra_INT()
        {
            ExceedingCassandraType(typeof(System.Int32), typeof(System.Int64), false);
        }

        [TestMethod]
		[Ignore]//OK
        public void ExceedingCassandra_FLOAT()
        {
            ExceedingCassandraType(typeof(System.Single), typeof(System.Double), false);
        }

    }
}
