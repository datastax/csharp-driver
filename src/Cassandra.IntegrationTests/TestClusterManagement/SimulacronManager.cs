using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class SimulacronManager
    {
        private Process _simulacronProcess;

        private static SimulacronManager _instance;

        private bool _started;

        public static SimulacronManager Instance
        {
            get
            {
                if (_instance != null) return _instance;
                _instance = new SimulacronManager();
                return _instance;
            }
        }

        public static Uri BaseAddress
        {
            get { return new Uri("http://127.0.0.1:8188"); }
        }

        public void Start()
        {
            if (IsUp())
            {
                return;
            }
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
            _simulacronProcess.StartInfo.Arguments = string.Format("-jar {0} --ip 127.0.0.101 -p 8188", jarPath);
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
                if (e.Data == null || _started) return;
                Console.WriteLine(e.Data);
                if (e.Data.Contains("Created nodes will start with ip"))
                {
                    _started = true;
                    eventWaitHandler.Set();
                }
            };
            _simulacronProcess.ErrorDataReceived += (sender, e) =>
            {
                if (e.Data == null) return;
                Console.WriteLine(e.Data);
            };
            _simulacronProcess.Start();

            _simulacronProcess.BeginOutputReadLine();
            _simulacronProcess.BeginErrorReadLine();

            eventWaitHandler.WaitOne(8000);
            if (!_started)
            {
                Stop();
                throw new Exception("Simulacron not started!");
            }
        }

        public void Stop()
        {
            if (_simulacronProcess == null) return;

            try
            {
                _simulacronProcess.Kill();
                _simulacronProcess.Dispose();
            }
            catch
            {
                //ignore 
            }
            finally
            {
                _simulacronProcess = null;
                _started = false;
            }
        }

        public bool IsUp()
        {
            return _started && _simulacronProcess != null && !_simulacronProcess.HasExited;
        }
    }
}
