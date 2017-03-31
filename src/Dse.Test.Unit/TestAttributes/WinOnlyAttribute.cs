using System;
using System.Diagnostics;
using System.Text.RegularExpressions;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Dse.Test.Unit.TestAttributes
{
    /// <summary>
    /// Filter the test based on the OS being used to run the test.
    /// </summary>
    public class WinOnlyAttribute : NUnitAttribute, IApplyToTest
    {
        private static readonly Regex VersionRegex = new Regex(@"Version (\d+)\.(\d+)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public int Major { get; set; }

        public int Minor { get; set; }

        /// <summary>
        /// Creates an instance of an attribute that filters the test to execute according to the current Win version
        /// being used.
        /// </summary>
        /// <param name="major">Major version</param>
        /// <param name="minor">Minor version</param>
        public WinOnlyAttribute(int major, int minor)
        {
            Major = major;
            Minor = minor;
        }

        /// <summary>
        /// Creates an instance of an attribute that filters the test to execute according to the current OS being used.
        /// </summary>
        public WinOnlyAttribute() : this(0, 0)
        {
            
        }

        public void ApplyToTest(NUnit.Framework.Internal.Test test)
        {
            if (!TestHelper.IsWin)
            {
                test.RunState = RunState.Ignored;
                test.Properties.Set("_SKIPREASON", "Test designed to run with Windows");
                return;
            }
            if (Major > 0)
            {
                // Get windows version without using APIs not supported in .NET Core
                Version winVersion = null;
                using (var ver = new Process
                {
                    StartInfo = new ProcessStartInfo
                    {
                        FileName = "cmd.exe",
                        Arguments = "/c ver",
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        CreateNoWindow = true
                    }
                })
                {
                    ver.Start();
                    var verOutput = ver.StandardOutput.ReadToEnd();
                    var match = VersionRegex.Match(verOutput);
                    if (match.Success)
                    {
                        winVersion = new Version(Convert.ToInt32(match.Groups[1].Value),
                            Convert.ToInt32(match.Groups[2].Value));
                    }
                    ver.WaitForExit();
                }
                string message = null;
                if (winVersion == null)
                {
                    message = string.Format("Test designed to run with Windows version {0}.{1} or above but " +
                                            "current version could not be retrieved",
                                            Major, Minor);
                }
                if (winVersion < new Version(Major, Minor))
                {
                    test.RunState = RunState.Ignored;
                    message = string.Format("Test designed to run with Windows version {0}.{1} or above (running {2})",
                        Major, Minor,
                        winVersion);
                }
                if (message != null)
                {
                    test.Properties.Set("_SKIPREASON", message);
                    test.RunState = RunState.Ignored;
                }
            }
        }
    }
}
