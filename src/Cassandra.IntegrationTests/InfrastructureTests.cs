using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    /// Represents a set of test that checks the local infrastructure and outputs the result to the standard output.
    /// The tests are not meant to fail, just print the result.
    /// </summary>
    [TestFixture]
    public class InfrastructureTests
    {
        /// <summary>
        /// Checks that python is present in the system
        /// </summary>
        [Test]
        public void PythonTest()
        {
            var output = TestUtils.ExecutePythonCommand("-V");
            if (output.ExitCode == 0)
            {
                Trace.TraceInformation("Python version: " + output.OutputText.ToString());
            }
            else
            {
                Trace.TraceError("Python not found");
                Trace.TraceError(output.ToString());
            }
        }

        /// <summary>
        /// Checks that ccm is present in the user profile path (generally C:\Users\(USERNAME)\)
        /// </summary>
        [Test]
        public void CcmTest()
        {
            var ccmConfigDir = TestUtils.CreateTempDirectory();
            var output = TestUtils.ExecuteLocalCcm("list", ccmConfigDir);
            if (output.ExitCode == 0)
            {
                Trace.TraceInformation("Ccm executed correctly: " + output.OutputText.ToString());
            }
            else
            {
                Trace.TraceError("Ccm not found");
                Trace.TraceError(output.ToString());
            }
        }

        /// <summary>
        /// Checks that ccm is present in the user profile path (generally C:\Users\(USERNAME)\)
        /// </summary>
        [Test]
        public void CcmStartRemove()
        {
            var ccmConfigDir = TestUtils.CreateTempDirectory();
            var output = TestUtils.ExecuteLocalCcmClusterStart(ccmConfigDir, "2.0.6");
            if (output.ExitCode == 0)
            {
                Trace.TraceInformation("Ccm started correctly: " + output.OutputText.ToString());
            }
            else
            {
                Trace.TraceError("Ccm start failed:");
                Trace.TraceError(output.ToString());
            }

            TestUtils.ExecuteLocalCcmClusterRemove(ccmConfigDir);
        }

        /// <summary>
        /// Checks that ccm is present in the user profile path (generally C:\Users\(USERNAME)\)
        /// </summary>
        [Test]
        public void CcmStartRemoveMultipleTimes()
        {
            for (var i = 0; i < 4; i++)
            {
                CcmStartRemove();
            }
        }
    }
}
