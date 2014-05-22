using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    /// Represents a set of tests that reuse an test cluster of 1 node
    /// </summary>
    public abstract class TwoNodesClusterTest : MultipleNodesClusterTest
    {
        public TwoNodesClusterTest()
            : base(2)
        {

        }
    }
}
