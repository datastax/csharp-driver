//
//       Copyright (C) 2019 DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using NUnit.Framework;

namespace Cassandra.Tests.ExecutionProfiles
{
    [TestFixture]
    public class ConfigurationTests
    {
        [Test]
        public void Should_MapProfileToOptionsCorrectly_When_AllSettingsAreProvided()
        {
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithExecutionProfiles(opts =>
            {
                opts.AddProfile(
                    "test1",
                    ExecutionProfile
                        .Builder()
                        .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                        .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                        .WithReadTimeoutMillis(9999)
                        .WithLoadBalancingPolicy(lbp)
                        .WithSpeculativeExecutionPolicy(sep)
                        .WithRetryPolicy(rp)
                        .Build());
            }).Build();

            Assert.AreEqual(1, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, options.SerialConsistencyLevel);
            Assert.AreEqual(9999, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sep, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, options.RetryPolicy);
        }

        [Test]
        public void Should_MapProfileToOptionsWithAllSettingsFromCluster_When_NoSettingIsProvided()
        {
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var tg = new AtomicMonotonicTimestampGenerator();
            var cluster = Cluster
                          .Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithQueryOptions(
                              new QueryOptions()
                                  .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                                  .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                  .SetDefaultIdempotence(true)
                                  .SetPageSize(5)
                                  .SetPrepareOnAllHosts(false)
                                  .SetReprepareOnUp(false))
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(9999))
                          .WithLoadBalancingPolicy(lbp)
                          .WithSpeculativeExecutionPolicy(sep)
                          .WithRetryPolicy(rp)
                          .WithExecutionProfiles(opts => { opts.AddProfile("test1", ExecutionProfile.Builder().Build()); })
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .Build();

            Assert.AreEqual(1, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, options.SerialConsistencyLevel);
            Assert.AreEqual(9999, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sep, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, options.RetryPolicy);
            Assert.AreEqual(true, options.DefaultIdempotence);
            Assert.AreEqual(5, options.PageSize);
            Assert.AreEqual(false, options.PrepareOnAllHosts);
            Assert.AreEqual(30, options.QueryAbortTimeout);
            Assert.AreEqual(false, options.ReprepareOnUp);
            Assert.AreSame(tg, options.TimestampGenerator);
        }

        [Test]
        public void Should_MapProfileToOptionsWithSomeSettingsFromCluster_When_SomeSettingAreNotProvided()
        {
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var sepProfile = new ConstantSpeculativeExecutionPolicy(200, 50);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var rpProfile = new LoggingRetryPolicy(new IdempotenceAwareRetryPolicy(new DefaultRetryPolicy()));
            var tg = new AtomicMonotonicTimestampGenerator();
            var cluster = Cluster
                          .Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithQueryOptions(
                              new QueryOptions()
                                  .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                                  .SetSerialConsistencyLevel(ConsistencyLevel.Serial)
                                  .SetDefaultIdempotence(true)
                                  .SetPageSize(5)
                                  .SetPrepareOnAllHosts(false)
                                  .SetReprepareOnUp(false))
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(300))
                          .WithLoadBalancingPolicy(lbp)
                          .WithSpeculativeExecutionPolicy(sep)
                          .WithRetryPolicy(rp)
                          .WithExecutionProfiles(opts =>
                          {
                              opts.AddProfile(
                                  "test1",
                                  ExecutionProfile
                                      .Builder()
                                      .WithConsistencyLevel(ConsistencyLevel.Quorum)
                                      .WithSpeculativeExecutionPolicy(sepProfile)
                                      .WithRetryPolicy(rpProfile)
                                      .Build());
                          })
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .Build();

            Assert.AreEqual(1, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.AreEqual(ConsistencyLevel.Quorum, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.Serial, options.SerialConsistencyLevel);
            Assert.AreEqual(300, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sepProfile, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rpProfile, options.RetryPolicy);
            Assert.AreEqual(true, options.DefaultIdempotence);
            Assert.AreEqual(5, options.PageSize);
            Assert.AreEqual(false, options.PrepareOnAllHosts);
            Assert.AreEqual(30, options.QueryAbortTimeout);
            Assert.AreEqual(false, options.ReprepareOnUp);
            Assert.AreSame(tg, options.TimestampGenerator);
        }

        [Test]
        public void Should_MapProfileToOptionsWithSomeSettingsFromBaseProfile_When_ADerivedProfileIsProvided()
        {
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var sepProfile = new ConstantSpeculativeExecutionPolicy(200, 50);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var rpProfile = new LoggingRetryPolicy(new IdempotenceAwareRetryPolicy(new DefaultRetryPolicy()));
            var tg = new AtomicMonotonicTimestampGenerator();
            var baseProfile = ExecutionProfile
                              .Builder()
                              .WithConsistencyLevel(ConsistencyLevel.Quorum)
                              .WithSpeculativeExecutionPolicy(sepProfile)
                              .WithRetryPolicy(rpProfile)
                              .Build();
            var cluster = Cluster
                          .Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithQueryOptions(
                              new QueryOptions()
                                  .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                                  .SetSerialConsistencyLevel(ConsistencyLevel.Serial)
                                  .SetDefaultIdempotence(true)
                                  .SetPageSize(5)
                                  .SetPrepareOnAllHosts(false)
                                  .SetReprepareOnUp(false))
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(300))
                          .WithLoadBalancingPolicy(lbp)
                          .WithSpeculativeExecutionPolicy(sep)
                          .WithRetryPolicy(rp)
                          .WithExecutionProfiles(opts =>
                          {
                              opts.AddProfile(
                                  "test1",
                                  ExecutionProfile
                                      .Builder()
                                      .DeriveFrom(baseProfile)
                                      .WithConsistencyLevel(ConsistencyLevel.All)
                                      .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                      .Build());
                          })
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .Build();

            Assert.AreEqual(1, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.AreEqual(ConsistencyLevel.All, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, options.SerialConsistencyLevel);
            Assert.AreEqual(300, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sepProfile, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rpProfile, options.RetryPolicy);
            Assert.AreEqual(true, options.DefaultIdempotence);
            Assert.AreEqual(5, options.PageSize);
            Assert.AreEqual(false, options.PrepareOnAllHosts);
            Assert.AreEqual(30, options.QueryAbortTimeout);
            Assert.AreEqual(false, options.ReprepareOnUp);
            Assert.AreSame(tg, options.TimestampGenerator);
        }
    }
}