using NUnit.Framework;
using System;
using System.Collections.Generic;
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
            string pathToCcm = "-V";
            var output = TestUtils.ExecutePythonCommand(pathToCcm);
            if (output.ExitCode == 0)
            {
                Console.WriteLine("Python version: " + output.OutputText.ToString());
            }
            else
            {
                Console.WriteLine("Python not found");
                Console.WriteLine(output);
            }
        }
    }
}
