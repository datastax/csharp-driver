using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class SimulacronManager
    {
        private Process _simulacronProcess;

        private bool _initialized;

        public static SimulacronManager Instance { get; } = new SimulacronManager();

        public static Uri BaseAddress => new Uri("http://127.0.0.1:8188");

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
            _simulacronProcess.StartInfo.FileName = "java";
            _simulacronProcess.StartInfo.Arguments = $"-jar {jarPath} --ip 127.0.0.101 -p 8188";
            _simulacronProcess.StartInfo.UseShellExecute = false;
            _simulacronProcess.StartInfo.CreateNoWindow = true;
            _simulacronProcess.StartInfo.RedirectStandardOutput = true;
            _simulacronProcess.StartInfo.RedirectStandardError = true;
#if !NETCORE
            _simulacronProcess.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
#endif
            var eventWaitHandler = new AutoResetEvent(false);
            _simulacronProcess.OutputDataReceived += (sender, e) =>
            {
                if (e.Data == null || started) return;
                Trace.TraceInformation(e.Data);
                if (e.Data.Contains("Created nodes will start with ip"))
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
                eventWaitHandler.Set();
            };
            _simulacronProcess.Start();

            _simulacronProcess.BeginOutputReadLine();
            _simulacronProcess.BeginErrorReadLine();

            eventWaitHandler.WaitOne(8000);
            if (!started)
            {
                Trace.TraceError(errorMessage);
                Stop();
                throw new Exception("Simulacron failed to start!");
            }
            Trace.TraceInformation("Simulacron started");
        }

        public void Stop()
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
