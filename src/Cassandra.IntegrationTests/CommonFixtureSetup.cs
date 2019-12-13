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
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [SetUpFixture]
    public class CommonFixtureSetup
    {
        [OneTimeSetUp]
        public void SetupTestSuite()
        {
            Environment.SetEnvironmentVariable("CASSANDRA_VERSION", "3.11.2");
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            if (Environment.GetEnvironmentVariable("TEST_TRACE")?.ToUpper() == "ON")
            {
                Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
            }
            Trace.TraceInformation("Starting Test Run ...");
        }

        [OneTimeTearDown]
        public void TearDownTestSuite()
        {
            // this method is executed once after all the fixtures have completed execution
            TestClusterManager.TryRemove();
            SimulacronManager.Instance.Stop();
            TestCloudClusterManager.TryRemove();
        }
    }
}
