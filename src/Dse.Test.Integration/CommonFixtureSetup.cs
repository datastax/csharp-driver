//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Diagnostics;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration
{
    [SetUpFixture]
    public class CommonFixtureSetup
    {
        [OneTimeSetUp]
        public void SetupTestSuite()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            if (Environment.GetEnvironmentVariable("TEST_TRACE")?.ToUpper() == "ON")
            {
                Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
            }
            Trace.TraceInformation("Starting Test Run ...");
            SimulacronManager.Instance.Start();
        }

        [OneTimeTearDown]
        public void TearDownTestSuite()
        {
            // this method is executed once after all the fixtures have completed execution
            TestClusterManager.TryRemove();
            SimulacronManager.Instance.Stop();
        }
    }
}
