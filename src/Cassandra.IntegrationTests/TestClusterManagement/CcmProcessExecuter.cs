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
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
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
                throw new TestInfrastructureException(string.Format("Process exited in error {0}", output.ToString()));
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

                process.OutputDataReceived += (sender, e) =>
                {
                    if (e.Data != null)
                    {
                        output.OutputText.AppendLine(e.Data);
                    }
                };
                process.ErrorDataReceived += (sender, e) =>
                {
                    if (e.Data != null)
                    {
                        output.OutputText.AppendLine(e.Data);
                    }
                };

                process.Start();

                process.BeginOutputReadLine();
                process.BeginErrorReadLine();

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
            }
            return output;
        }
    }
}
