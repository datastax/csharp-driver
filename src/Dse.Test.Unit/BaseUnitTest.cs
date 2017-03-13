//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Dse;
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
        /// Returns the hex string representation in lowercase
        /// </summary>
        protected static string ToHex(byte[] ba)
        {
            var hex = BitConverter.ToString(ba);
            return hex.Replace("-", "").ToLowerInvariant();
        }

        /// <summary>
        /// Returns the byte array representation from a hex string
        /// </summary>
        protected static byte[] FromHex(string hex)
        {
            return Enumerable.Range(0, hex.Length)
                .Where(x => x % 2 == 0)
                .Select(x => Convert.ToByte(hex.Substring(x, 2), 16))
                .ToArray();
        }

        /// <summary>
        /// A Load balancing suitable for testing that returns 2 hardcoded nodes.
        /// </summary>
        protected class TestLoadBalancingPolicy : ILoadBalancingPolicy
        {
            private readonly HostDistance _distance;

            public TestLoadBalancingPolicy(HostDistance distance = HostDistance.Local)
            {
                _distance = distance;
            }

            public void Initialize(ICluster cluster)
            {

            }

            public HostDistance Distance(Host host)
            {
                return _distance;
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
