//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Text;
#if NETCORE
using Microsoft.DotNet.InternalAbstractions;
#endif

namespace Dse.Test.Integration.TestBase
{
    /// <summary>
    ///  A number of static fields/methods handy for tests.
    /// </summary>
    internal static class TestUtils
    {

        public static string GetTestClusterNameBasedOnTime()
        {
            return "test_" + (DateTimeOffset.UtcNow.Ticks / TimeSpan.TicksPerSecond);
        }

        public static bool IsWin
        {
            get
            {
#if !NETCORE
                switch (Environment.OSVersion.Platform)
                {
                    case PlatformID.Win32NT:
                    case PlatformID.Win32S:
                    case PlatformID.Win32Windows:
                    case PlatformID.WinCE:
                        return true;
                }
                return false;
#else
                return RuntimeEnvironment.OperatingSystemPlatform == Platform.Windows;
#endif
            }
        }
        

        /// <summary>
        /// Adds double quotes to the path in case it contains spaces.
        /// </summary>
        public static string EscapePath(string path)
        {
            if (path == null)
            {
                throw new ArgumentNullException("path");
            }
            if (path.Contains(" "))
            {
                return "\"" + path + "\"";
            }
            return path;
        }

    }

    /// <summary>
    /// Represents a result from executing an external process.
    /// </summary>
    public class ProcessOutput
    {
        public int ExitCode { get; set; }

        public StringBuilder OutputText { get; set; }

        public ProcessOutput()
        {
            OutputText = new StringBuilder();
            ExitCode = Int32.MinValue;
        }

        public override string ToString()
        {
            return
                "Exit Code: " + this.ExitCode + Environment.NewLine +
                "Output Text: " + this.OutputText.ToString() + Environment.NewLine;
        }
    }
}
