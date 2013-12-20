using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;
using System.Threading;

#if MYTEST
using MyTest;
#else
using Cassandra.MSTest;
#endif


namespace Cassandra.Data.Linq.MSTest
{
	[TestClass]
	public class FoundBug4Tests
	{
		public class Message
		{
			[PartitionKey]
			public Guid message_id;

			public List<string> line_list;
			public HashSet<string> line_set;
			public Dictionary<string, string> line_map;
		}

        private string KeyspaceName = "test";
        Session Session;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");
            Session.CreateKeyspaceIfNotExists(KeyspaceName);
            Session.ChangeKeyspace(KeyspaceName);
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }

		[TestMethod]
		public void Bug_LargeTextInCollections()
		{
			var table = Session.GetTable<Message>();
			table.CreateIfNotExists();

			string largeString = new string('8', UInt16.MaxValue - 16);

			var message = new Message()
			{
				message_id = Guid.NewGuid(),
				line_list = new List<string>()
				{
					largeString
				},
				line_set = new HashSet<string>()
				{
					largeString
				},
				line_map = new Dictionary<string, string>()
				{
					{ largeString, largeString }
				}
			};

			var batch = Session.CreateBatch();
			batch.Append(table.Insert(message));
			batch.Execute();

			var saved_message = (from m in table select m).Execute().FirstOrDefault();

			Assert.Equal(largeString, saved_message.line_list[0]);
			Assert.Equal(largeString, saved_message.line_set.First());
			Assert.Equal(largeString, saved_message.line_map[largeString]);
		}
	}
}
