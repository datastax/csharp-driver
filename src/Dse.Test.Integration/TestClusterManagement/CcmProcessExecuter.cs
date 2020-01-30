//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Dse.Test.Integration.TestClusterManagement
{
    public abstract class CcmProcessExecuter : ICcmProcessExecuter
    {
        public virtual ProcessOutput ExecuteCcm(string args, bool throwOnProcessError = true)
        {
            var executable = GetExecutable(ref args);
            Trace.TraceInformation(executable + " " + args);
            var output = ExecuteProcess(executable, args);
            if (throwOnProcessError)
            {
                ValidateOutput(output);
            }
            return output;
        }

        public virtual int GetDefaultTimeout()
        {
            return 90000;
        }

        protected abstract string GetExecutable(ref string args);

        protected static void ValidateOutput(ProcessOutput output)
        {
            if (output.ExitCode != 0)
            {
                throw new TestInfrastructureException("Process exited in error " + output.ToString());
            }
        }

        /// <summary>
        /// Spawns a new process (platform independent)
        /// </summary>
        public static ProcessOutput ExecuteProcess(string processName, string args, int timeout = 90000, IReadOnlyDictionary<string, string> envVariables = null, string workDir = null)
        {
            var output = new ProcessOutput();
            using (var process = new Process())
            {
                process.StartInfo.FileName = processName;
                process.StartInfo.Arguments = args;
                process.StartInfo.RedirectStandardOutput = true;
                process.StartInfo.RedirectStandardError = true;
                //Hide the python window if possible
                process.StartInfo.UseShellExecute = false;
#if !NETCORE
                process.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
#endif
                process.StartInfo.CreateNoWindow = true;

                if (envVariables != null)
                {
                    foreach (var envVar in envVariables)
                    {
                        process.StartInfo.EnvironmentVariables[envVar.Key] = envVar.Value;
                    }
                }

                if (workDir != null)
                {
                    process.StartInfo.WorkingDirectory = workDir;
                }
                
                process.Start();

                if (process.WaitForExit(timeout))
                {
                    // Process completed.
                    output.ExitCode = process.ExitCode;
                }
                else
                {
                    // Timed out.
                    output.ExitCode = -1;
                }

                output.SetOutput(process.StandardOutput.ReadToEnd() + 
                                 Environment.NewLine + "STDERR:" + Environment.NewLine + process.StandardError.ReadToEnd());
            }
            return output;
        }
    }
}
