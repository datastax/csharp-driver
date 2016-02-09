using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Graph;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    public class DseClusterBuilderTests : BaseUnitTest
    {
        [Test]
        public void Should_Build_A_Cluster_With_Graph_Options()
        {
            var graphOptions = new GraphOptions();
            IDseCluster cluster = DseCluster.Builder()
                .WithGraphOptions(graphOptions)
                .AddContactPoint("192.168.1.159")
                .Build();
            Assert.NotNull(cluster.Configuration);
            Assert.NotNull(cluster.Configuration.CassandraConfiguration);
            Assert.AreSame(graphOptions, cluster.Configuration.GraphOptions);
        }

        [Test]
        public void Should_Build_A_Cluster_With_Default_Graph_Options()
        {
            //without specifying graph options
            IDseCluster cluster = DseCluster.Builder().AddContactPoint("192.168.1.159").Build();
            Assert.NotNull(cluster.Configuration);
            Assert.NotNull(cluster.Configuration.CassandraConfiguration);
            Assert.NotNull(cluster.Configuration.GraphOptions);
        }
    }
}
