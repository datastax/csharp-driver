using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    /// Represents a set of tests that reuse an test cluster of 1 node
    /// </summary>
    public abstract class SingleNodeClusterTest : MultipleNodesClusterTest
    {
        public SingleNodeClusterTest() 
            : base(1)
        {

        }
    }
}
