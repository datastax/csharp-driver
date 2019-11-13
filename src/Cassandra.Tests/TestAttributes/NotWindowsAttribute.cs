using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Cassandra.Tests.TestAttributes
{
    /// <summary>
    /// Filter the test based on the OS being used to run the test.
    /// </summary>
    public class NotWindowsAttribute : NUnitAttribute, IApplyToTest
    {
        public void ApplyToTest(NUnit.Framework.Internal.Test test)
        {
            if (TestHelper.IsWin)
            {
                test.RunState = RunState.Ignored;
                test.Properties.Set("_SKIPREASON", "Test designed not to run with Windows");
            }
        }
    }
}