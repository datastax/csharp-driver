//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Tasks;
using NUnit.Framework;

namespace Cassandra.Tests
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

            public Task InitializeAsync(IMetadataSnapshotProvider metadata)
            {
                return TaskHelper.Completed;
            }

            public HostDistance Distance(ICluster cluster, Host host)
            {
                return _distance;
            }

            public IEnumerable<Host> NewQueryPlan(ICluster cluster, string keyspace, IStatement query)
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
