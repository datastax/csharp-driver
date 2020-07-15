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
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public abstract class CcmProcessExecuter : ICcmProcessExecuter
    {
        public virtual ProcessOutput ExecuteCcm(string args, bool throwOnProcessError = true)
        {
            var executable = GetExecutable(ref args);
            Trace.TraceInformation(executable + " " + args);
            var output = ExecuteProcess(executable, args, GetDefaultTimeout());
            if (throwOnProcessError)
            {
                ValidateOutput(output);
            }
            return output;
        }

        public virtual int GetDefaultTimeout()
        {
            return 5 * 60 * 1000;
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
        public static ProcessOutput ExecuteProcess(string processName, string args, int timeout, IReadOnlyDictionary<string, string> envVariables = null, string workDir = null)
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
                process.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
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

                var processEndTokenSource = new CancellationTokenSource();
                var timeoutTokenSource = new CancellationTokenSource();

                var tStandardOutput = CcmProcessExecuter.ReadStreamAsync(
                    process.StandardOutput, processEndTokenSource.Token, timeoutTokenSource.Token);
                var tStandardError = CcmProcessExecuter.ReadStreamAsync(
                    process.StandardError, processEndTokenSource.Token, timeoutTokenSource.Token);

                if (process.WaitForExit(timeout))
                {
                    // Process completed.
                    output.ExitCode = process.ExitCode;
                    processEndTokenSource.Cancel();

                    // process terminated normally, give some time for streams to catch up
                    // (note that usually this will happen instantly and this 5 second timeout won't be necessary)
                    timeoutTokenSource.CancelAfter(5000);
                }
                else
                {
                    // Timed out.
                    output.ExitCode = -1;
                    processEndTokenSource.Cancel();
                    timeoutTokenSource.Cancel();
                }
                var stdOut = tStandardOutput.GetAwaiter().GetResult();
                var stdErr = tStandardError.GetAwaiter().GetResult();

                output.SetOutput(stdOut + Environment.NewLine +
                                 "STDERR:" + Environment.NewLine +
                                 stdErr);
            }
            return output;
        }

        private static async Task<string> ReadStreamAsync(
            StreamReader stream, CancellationToken endProcessToken, CancellationToken timeoutToken)
        {
            const int bufLength = 1024;
            var stringBuilder = new StringBuilder();
            var buf = new char[bufLength];
            try
            {
                while (!endProcessToken.IsCancellationRequested)
                {
                    while (!endProcessToken.IsCancellationRequested)
                    {
                        var read = await Task.Run(
                            () => stream.ReadAsync(buf, 0, bufLength), 
                            timeoutToken).ConfigureAwait(false);
                        if (read <= 0)
                        {
                            break;
                        }

                        stringBuilder.Append(buf, 0, read);
                    }

                    await Task.Delay(500, endProcessToken).ConfigureAwait(false);
                }
            }
            catch
            {
                // ignored
            }

            try
            {
                var readEnd = await Task.Run(
                    stream.ReadToEndAsync, 
                    timeoutToken).ConfigureAwait(false);
                stringBuilder.Append(readEnd);
            }
            catch
            {
                // ignored
            }

            return stringBuilder.ToString();
        }
    }
}
