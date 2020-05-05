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
using System.IO;
using System.Threading;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class SimulacronManager : IDisposable
    {
        private volatile Process _simulacronProcess;

        private volatile bool _initialized;

        public static SimulacronManager DefaultInstance { get; } = new SimulacronManager();
        
        public Uri BaseAddress => new Uri($"http://127.0.0.1:{HttpPort}");

        public int? StartPort { get; } = null;

        public string StartIp { get; } = "127.0.0.101";

        public int HttpPort { get; } = 8188;

        private SimulacronManager()
        {
        }

        private SimulacronManager(int httpPort, string startIp, int? startPort)
        {
            HttpPort = httpPort;
            StartIp = startIp;
            StartPort = startPort;
        }

        public static SimulacronManager GetForPeersTests()
        {
            var manager = new SimulacronManager(8178, "127.0.0.11", 9011);
            manager.Start();
            return manager;
        }

        public void Start()
        {
            if (_initialized)
            {
                return;
            }
            _initialized = true;
            var started = false;
            var errorMessage = "Simulacron is taking too long to start. Aborting initialization...";
            _simulacronProcess = new Process();
            var jarPath = Environment.GetEnvironmentVariable("SIMULACRON_PATH");
            if (string.IsNullOrEmpty(jarPath))
            {
                jarPath = Environment.GetEnvironmentVariable("HOME") + "/simulacron.jar";
            }
            if (!File.Exists(jarPath))
            {
                throw new Exception("Simulacron: Simulacron jar not found: " + jarPath);
            }

            var args = $"-jar {jarPath} --ip {StartIp} -p {HttpPort}";
            if (StartPort.HasValue)
            {
                args += $" -s {StartPort}";
            }

            _simulacronProcess.StartInfo.FileName = "java";
            _simulacronProcess.StartInfo.Arguments = args;
            _simulacronProcess.StartInfo.UseShellExecute = false;
            _simulacronProcess.StartInfo.CreateNoWindow = true;
            _simulacronProcess.StartInfo.RedirectStandardOutput = true;
            _simulacronProcess.StartInfo.RedirectStandardError = true;
            _simulacronProcess.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

            var eventWaitHandler = new AutoResetEvent(false);
            _simulacronProcess.OutputDataReceived += (sender, e) =>
            {
                if (e.Data == null || started) return;
                Trace.TraceInformation(e.Data);
                if (e.Data.Contains("Created nodes will start with ip") || e.Data.Contains("Address already in use"))
                {
                    started = true;
                    eventWaitHandler.Set();
                }
            };
            _simulacronProcess.ErrorDataReceived += (sender, e) =>
            {
                if (e.Data == null) return;
                Trace.TraceError(e.Data);
                errorMessage = $"Simulacron start error: {e.Data}";
            };
            _simulacronProcess.Start();

            _simulacronProcess.BeginOutputReadLine();
            _simulacronProcess.BeginErrorReadLine();

            eventWaitHandler.WaitOne(30000);
            if (!started)
            {
                Trace.TraceError(errorMessage);
                Dispose();
                throw new Exception("Simulacron failed to start!");
            }
            Trace.TraceInformation("Simulacron started");
        }

        public void Dispose()
        {
            if (_simulacronProcess == null) return;

            try
            {
                _simulacronProcess.Kill();
                _simulacronProcess.Dispose();
            }
            catch (Exception ex)
            {
                Trace.TraceWarning(ex.Message);
            }
            finally
            {
                _simulacronProcess = null;
                _initialized = false;
                Trace.TraceInformation("Simulacron process stopped");
            }
        }
    }
}
