using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    [TestFixture, Category("unit")]
    public abstract class BaseUnitTest
    {
        /// <summary>
        /// A reconnection policy suitable for unit testing.
        /// </summary>
        protected static readonly IReconnectionPolicy ReconnectionPolicy = new ConstantReconnectionPolicy(1000);

        protected static Task<T> TaskOf<T>(T value)
        {
            var tcs = new TaskCompletionSource<T>();
            tcs.SetResult(value);
            return tcs.Task;
        }

        /// <summary>
        /// A Load balancing suitable for testing that returns 2 hardcoded nodes.
        /// </summary>
        protected class TestLoadBalancingPolicy : ILoadBalancingPolicy
        {
            public void Initialize(ICluster cluster)
            {

            }

            public HostDistance Distance(Host host)
            {
                return HostDistance.Local;
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                return new[]
                {
                    new Host(new IPEndPoint(101L, 9042), ReconnectionPolicy),
                    new Host(new IPEndPoint(102L, 9042), ReconnectionPolicy)
                };
            }
        }
    }
}
