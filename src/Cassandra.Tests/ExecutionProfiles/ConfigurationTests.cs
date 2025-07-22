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
            var lbpGraph = new RoundRobinPolicy();
            var sepGraph = new ConstantSpeculativeExecutionPolicy(2000, 1);
            var rpGraph = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var cluster =
                Cluster
                    .Builder()
                    .AddContactPoint("127.0.0.1")
                    .WithExecutionProfiles(opts =>
                    {
                        opts.WithProfile("test1", profile => profile
                                .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                .WithReadTimeoutMillis(9999)
                                .WithLoadBalancingPolicy(lbp)
                                .WithSpeculativeExecutionPolicy(sep)
                                .WithRetryPolicy(rp));
                        opts.WithProfile("test1graph", profile => profile
                                .WithConsistencyLevel(ConsistencyLevel.All)
                                .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                                .WithReadTimeoutMillis(5555)
                                .WithLoadBalancingPolicy(lbpGraph)
                                .WithSpeculativeExecutionPolicy(sepGraph)
                                .WithRetryPolicy(rpGraph));
                    })
                    .Build();

            Assert.AreEqual(3, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, options.SerialConsistencyLevel);
            Assert.AreEqual(9999, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sep, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, options.RetryPolicy);

            var graphOptions = cluster.Configuration.RequestOptions["test1graph"];
            Assert.AreEqual(ConsistencyLevel.All, graphOptions.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.Serial, graphOptions.SerialConsistencyLevel);
            Assert.AreEqual(5555, graphOptions.ReadTimeoutMillis);
            Assert.AreSame(lbpGraph, graphOptions.LoadBalancingPolicy);
            Assert.AreSame(sepGraph, graphOptions.SpeculativeExecutionPolicy);
            Assert.AreSame(rpGraph, graphOptions.RetryPolicy);
        }

        [Test]
        public void Should_MapDefaultProfileToDefaultOptionsCorrectly_When_AllSettingsAreProvided()
        {
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var cluster =
                Cluster
                    .Builder()
                    .AddContactPoint("127.0.0.1")
                    .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(3000))
                    .WithExecutionProfiles(opts =>
                    {
                        opts.WithProfile("default", profile => profile
                                .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                .WithReadTimeoutMillis(9999)
                                .WithLoadBalancingPolicy(lbp)
                                .WithSpeculativeExecutionPolicy(sep)
                                .WithRetryPolicy(rp));
                    })
                    .Build();

            Assert.AreEqual(1, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["default"];
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
                          .WithExecutionProfiles(opts =>
                          {
                              opts.WithProfile("test1", profile => { });
                              opts.WithProfile("test1Graph", profile => { });
                          })
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)

                          .Build();

            Assert.AreEqual(3, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, options.SerialConsistencyLevel);
            Assert.AreEqual(9999, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sep, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, options.RetryPolicy);
            Assert.AreEqual(true, options.DefaultIdempotence);
            Assert.AreEqual(5, options.PageSize);
            Assert.AreEqual(30, options.QueryAbortTimeout);
            Assert.AreSame(tg, options.TimestampGenerator);

            var graphOptions = cluster.Configuration.RequestOptions["test1Graph"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, graphOptions.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, graphOptions.SerialConsistencyLevel);
            Assert.AreEqual(9999, graphOptions.ReadTimeoutMillis);
            Assert.AreSame(lbp, graphOptions.LoadBalancingPolicy);
            Assert.AreSame(sep, graphOptions.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, graphOptions.RetryPolicy);
            Assert.AreEqual(true, graphOptions.DefaultIdempotence);
            Assert.AreEqual(5, graphOptions.PageSize);
            Assert.AreEqual(30, graphOptions.QueryAbortTimeout);
            Assert.AreSame(tg, graphOptions.TimestampGenerator);
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
                          .WithExecutionProfiles(opts => opts
                              .WithProfile("test1", profile => profile
                                    .WithConsistencyLevel(ConsistencyLevel.Quorum)
                                    .WithSpeculativeExecutionPolicy(sepProfile)
                                    .WithRetryPolicy(rpProfile))
                              .WithProfile("test1Graph", profile => profile
                                  .WithReadTimeoutMillis(5000)
                                  .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)))
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)

                          .Build();

            Assert.AreEqual(3, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.AreEqual(ConsistencyLevel.Quorum, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.Serial, options.SerialConsistencyLevel);
            Assert.AreEqual(300, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sepProfile, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rpProfile, options.RetryPolicy);
            Assert.AreEqual(true, options.DefaultIdempotence);
            Assert.AreEqual(5, options.PageSize);
            Assert.AreEqual(30, options.QueryAbortTimeout);
            Assert.AreSame(tg, options.TimestampGenerator);

            var graphOptions = cluster.Configuration.RequestOptions["test1Graph"];
            Assert.AreEqual(ConsistencyLevel.LocalQuorum, graphOptions.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.Serial, graphOptions.SerialConsistencyLevel);
            Assert.AreEqual(5000, graphOptions.ReadTimeoutMillis);
            Assert.AreSame(lbp, graphOptions.LoadBalancingPolicy);
            Assert.AreSame(sep, graphOptions.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, graphOptions.RetryPolicy);
            Assert.AreEqual(true, graphOptions.DefaultIdempotence);
            Assert.AreEqual(5, graphOptions.PageSize);
            Assert.AreEqual(30, graphOptions.QueryAbortTimeout);
            Assert.AreSame(tg, graphOptions.TimestampGenerator);
        }

        [Test]
        public void Should_MapProfileToOptionsWithSomeSettingsFromBaseProfile_When_ADerivedProfileIsProvided()
        {
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var sepProfile = new ConstantSpeculativeExecutionPolicy(200, 50);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var rpProfile = new LoggingRetryPolicy(new IdempotenceAwareRetryPolicy(new DefaultRetryPolicy()));
            var rpGraph = new LoggingRetryPolicy(new DefaultRetryPolicy());
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
                          .WithExecutionProfiles(opts => opts
                              .WithProfile("default", profile => profile
                                      .WithReadTimeoutMillis(5))
                              .WithProfile("baseProfile", baseProfile => baseProfile
                                      .WithConsistencyLevel(ConsistencyLevel.Quorum)
                                      .WithSpeculativeExecutionPolicy(sepProfile)
                                      .WithRetryPolicy(rpProfile))
                              .WithDerivedProfile("test1", "baseProfile", profileBuilder => profileBuilder
                                      .WithConsistencyLevel(ConsistencyLevel.All)
                                      .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
                              .WithProfile("baseProfileGraph", baseProfile => baseProfile
                                      .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)
                                      .WithSpeculativeExecutionPolicy(sepProfile)
                                      .WithRetryPolicy(rpProfile)
                                      )
                              .WithDerivedProfile("test1Graph", "baseProfileGraph", profileBuilder => profileBuilder
                                      .WithConsistencyLevel(ConsistencyLevel.Two)
                                      .WithRetryPolicy(rpGraph)))
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .Build();

            Assert.AreEqual(5, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.AreEqual(ConsistencyLevel.All, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, options.SerialConsistencyLevel);
            Assert.AreEqual(5, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sepProfile, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rpProfile, options.RetryPolicy);
            Assert.AreEqual(true, options.DefaultIdempotence);
            Assert.AreEqual(5, options.PageSize);
            Assert.AreEqual(30, options.QueryAbortTimeout);
            Assert.AreSame(tg, options.TimestampGenerator);

            var graphOptions = cluster.Configuration.RequestOptions["test1Graph"];
            Assert.AreEqual(ConsistencyLevel.Two, graphOptions.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.Serial, graphOptions.SerialConsistencyLevel);
            Assert.AreEqual(5, graphOptions.ReadTimeoutMillis);
            Assert.AreSame(lbp, graphOptions.LoadBalancingPolicy);
            Assert.AreSame(sepProfile, graphOptions.SpeculativeExecutionPolicy);
            Assert.AreSame(rpGraph, graphOptions.RetryPolicy);
            Assert.AreEqual(true, graphOptions.DefaultIdempotence);
            Assert.AreEqual(5, graphOptions.PageSize);
            Assert.AreEqual(30, graphOptions.QueryAbortTimeout);
            Assert.AreSame(tg, graphOptions.TimestampGenerator);
        }

        [Test]
        public void Should_MapDefaultProfileToOptionsWithAllSettingsFromCluster_When_NoSettingIsProvided()
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
                          .WithExecutionProfiles(opts => { opts.WithProfile("default", profile => { }); })
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)

                          .Build();

            Assert.AreEqual(1, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["default"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, options.SerialConsistencyLevel);
            Assert.AreEqual(9999, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sep, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, options.RetryPolicy);
            Assert.AreEqual(true, options.DefaultIdempotence);
            Assert.AreEqual(5, options.PageSize);
            Assert.AreEqual(30, options.QueryAbortTimeout);
            Assert.AreSame(tg, options.TimestampGenerator);
        }

        [Test]
        public void Should_MapDefaultProfileToOptionsWithAllSettingsFromCluster_When_NoProfileIsChangedOrAdded()
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
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)

                          .Build();

            Assert.AreEqual(1, cluster.Configuration.RequestOptions.Count);
            var options = cluster.Configuration.RequestOptions["default"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, options.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, options.SerialConsistencyLevel);
            Assert.AreEqual(9999, options.ReadTimeoutMillis);
            Assert.AreSame(lbp, options.LoadBalancingPolicy);
            Assert.AreSame(sep, options.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, options.RetryPolicy);
            Assert.AreEqual(true, options.DefaultIdempotence);
            Assert.AreEqual(5, options.PageSize);
            Assert.AreEqual(30, options.QueryAbortTimeout);
            Assert.AreSame(tg, options.TimestampGenerator);
        }

        [Test]
        public void Should_MapOptionsToProfileCorrectly_When_AllSettingsAreProvided()
        {
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithExecutionProfiles(opts =>
            {
                opts.WithProfile("test1", profile => profile
                                                     .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                                     .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                                     .WithReadTimeoutMillis(9999)
                                                     .WithLoadBalancingPolicy(lbp)
                                                     .WithSpeculativeExecutionPolicy(sep)
                                                     .WithRetryPolicy(rp));
                opts.WithProfile("test1Graph", profile => profile
                                                     .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                                     .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                                     .WithReadTimeoutMillis(9999)
                                                     .WithLoadBalancingPolicy(lbp)
                                                     .WithSpeculativeExecutionPolicy(sep)
                                                     .WithRetryPolicy(rp)
                                                     );
            }).Build();

            var execProfile = cluster.Configuration.ExecutionProfiles["test1"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, execProfile.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, execProfile.SerialConsistencyLevel);
            Assert.AreEqual(9999, execProfile.ReadTimeoutMillis);
            Assert.AreSame(lbp, execProfile.LoadBalancingPolicy);
            Assert.AreSame(sep, execProfile.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, execProfile.RetryPolicy);

            var graphExecProfile = cluster.Configuration.ExecutionProfiles["test1Graph"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, graphExecProfile.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, graphExecProfile.SerialConsistencyLevel);
            Assert.AreEqual(9999, graphExecProfile.ReadTimeoutMillis);
            Assert.AreSame(lbp, graphExecProfile.LoadBalancingPolicy);
            Assert.AreSame(sep, graphExecProfile.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, graphExecProfile.RetryPolicy);
        }

        [Test]
        public void Should_MapDefaultOptionsToDefaultProfileCorrectly_When_AllSettingsAreProvided()
        {
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithExecutionProfiles(opts =>
            {
                opts.WithProfile("default", profile => profile
                                                     .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                                     .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                                     .WithReadTimeoutMillis(9999)
                                                     .WithLoadBalancingPolicy(lbp)
                                                     .WithSpeculativeExecutionPolicy(sep)
                                                     .WithRetryPolicy(rp)
                                                     );
            }).Build();

            var execProfile = cluster.Configuration.ExecutionProfiles["default"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, execProfile.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, execProfile.SerialConsistencyLevel);
            Assert.AreEqual(9999, execProfile.ReadTimeoutMillis);
            Assert.AreSame(lbp, execProfile.LoadBalancingPolicy);
            Assert.AreSame(sep, execProfile.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, execProfile.RetryPolicy);
        }

        [Test]
        public void Should_MapOptionsToProfileWithAllSettingsFromCluster_When_NoProfileIsChangedOrAdded()
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
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)

                          .Build();

            Assert.AreEqual(1, cluster.Configuration.RequestOptions.Count);
            var profile = cluster.Configuration.ExecutionProfiles["default"];
            Assert.AreEqual(ConsistencyLevel.EachQuorum, profile.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, profile.SerialConsistencyLevel);
            Assert.AreEqual(9999, profile.ReadTimeoutMillis);
            Assert.AreSame(lbp, profile.LoadBalancingPolicy);
            Assert.AreSame(sep, profile.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, profile.RetryPolicy);
        }

        [Test]
        public void Should_SetLegacyProperties_When_PoliciesAreProvidedByDefaultProfile()
        {
            var lbp1 = new RoundRobinPolicy();
            var sep1 = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var lbp2 = new RoundRobinPolicy();
            var sep2 = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var retryPolicy = new DefaultRetryPolicy();
            var retryPolicy2 = new DefaultRetryPolicy();
            var cluster =
                Cluster.Builder()
                       .AddContactPoint("127.0.0.1")
                       .WithLoadBalancingPolicy(lbp1)
                       .WithSpeculativeExecutionPolicy(sep1)
                       .WithRetryPolicy(retryPolicy)
                       .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(123))
                       .WithQueryOptions(
                           new QueryOptions()
                               .SetConsistencyLevel(ConsistencyLevel.All)
                               .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
                       .WithExecutionProfiles(opt => opt
                           .WithProfile("default", profile =>
                               profile
                                   .WithLoadBalancingPolicy(lbp2)
                                   .WithSpeculativeExecutionPolicy(sep2)
                                   .WithRetryPolicy(retryPolicy2)
                                   .WithConsistencyLevel(ConsistencyLevel.Quorum)
                                   .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                                   .WithReadTimeoutMillis(4412)))
                       .Build();

            Assert.AreSame(retryPolicy2, cluster.Configuration.Policies.ExtendedRetryPolicy);
            Assert.AreSame(retryPolicy2, cluster.Configuration.Policies.RetryPolicy);
            Assert.AreSame(sep2, cluster.Configuration.Policies.SpeculativeExecutionPolicy);
            Assert.AreSame(lbp2, cluster.Configuration.Policies.LoadBalancingPolicy);
            Assert.AreEqual(4412, cluster.Configuration.SocketOptions.ReadTimeoutMillis);
            Assert.AreEqual(ConsistencyLevel.Quorum, cluster.Configuration.QueryOptions.GetConsistencyLevel());
            Assert.AreEqual(ConsistencyLevel.Serial, cluster.Configuration.QueryOptions.GetSerialConsistencyLevel());
        }
    }
}