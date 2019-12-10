//
//       Copyright (C) DataStax Inc.
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

using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    public class SimulacronTest
    {
        private readonly bool _shared;
        private readonly SimulacronOptions _options;
        private readonly bool _connect;

        public SimulacronTest(bool shared = false, SimulacronOptions options = null, bool connect = true)
        {
            _shared = shared;
            _options = options ?? new SimulacronOptions();
            _connect = connect;
        }

        protected ISession Session { get; private set; }

        protected SimulacronCluster TestCluster { get; private set; }

        [OneTimeSetUp]
        public virtual void OneTimeSetup()
        {
            if (_shared)
            {
                Init();
            }
        }

        [OneTimeTearDown]
        public virtual void OneTimeTearDown()
        {
            if (_shared)
            {
                Dispose();
            }
        }

        [SetUp]
        public virtual void SetupTest()
        {
            if (!_shared)
            {
                Init();
            }
        }

        [TearDown]
        public virtual void TearDown()
        {
            if (!_shared)
            {
                Dispose();
            }
        }

        private void Init()
        {
            TestCluster = SimulacronCluster.CreateNew(_options);
            if (_connect)
            {
                Session = CreateSession();
            }
        }

        private void Dispose()
        {
            Session?.Cluster?.Dispose();
            TestCluster?.Dispose();
        }

        private ISession CreateSession()
        {
            return Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build().Connect();
        }
    }
}