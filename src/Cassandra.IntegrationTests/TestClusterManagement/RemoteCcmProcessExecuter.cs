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
using System.Diagnostics;
using Cassandra.IntegrationTests.TestBase;
using Renci.SshNet;
using Renci.SshNet.Common;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class RemoteCcmProcessExecuter : CcmProcessExecuter
    {
        private readonly string _user;
        private readonly string _ip;
        private readonly int _port;
        private readonly string _password;
        private readonly string _privateKeyFilePath;
        private SshClient _sshClient;

        public RemoteCcmProcessExecuter(string ip, string user, string password, int port = 22, string privateKeyFilePath = null)
        {
            _user = user;
            _ip = ip;
            _password = password;
            _port = port;
            _privateKeyFilePath = privateKeyFilePath;
        }


        public override ProcessOutput ExecuteCcm(string args, bool throwOnProcessError = true)
        {
            var executable = GetExecutable(ref args);
            Trace.TraceInformation(executable + " " + args);

            var output = new ProcessOutput();
            if (_sshClient == null)
            {
                Trace.TraceInformation("Connecting ssh client...");
                var kauth = new KeyboardInteractiveAuthenticationMethod(_user);
                var pauth = new PasswordAuthenticationMethod(_user, _password);

                var connectionInfo = new ConnectionInfo(_ip, _port, _user, kauth, pauth);

                kauth.AuthenticationPrompt += (sender, e) =>
                {
                    foreach (var prompt in e.Prompts)
                    {
                        if (prompt.Request.ToLowerInvariant().StartsWith("password"))
                        {
                            prompt.Response = _password;
                        }
                    }
                };

                if (!string.IsNullOrEmpty(_privateKeyFilePath))
                {
                    var privateKeyAuth = new PrivateKeyAuthenticationMethod(_user, new PrivateKeyFile[]
                    {
                        new PrivateKeyFile(_privateKeyFilePath)
                    });
                    connectionInfo = new ConnectionInfo(_ip, _port, _user, privateKeyAuth);
                }

                _sshClient = new SshClient(connectionInfo);
            }
            if (!_sshClient.IsConnected)
                _sshClient.Connect();

            var result = _sshClient.RunCommand(string.Format(@"{0} {1}", executable, args));

            output.ExitCode = result.ExitStatus;
            if (result.Error != null)
            {
                output.OutputText.Append(result.Error);
            }
            else
            {
                output.OutputText.Append(result.Result);
            }

            if (throwOnProcessError)
            {
                ValidateOutput(output);
            }
            return output;
        }

        protected override string GetExecutable(ref string args)
        {
            return LocalCcmProcessExecuter.CcmCommandPath;
        }

        ~RemoteCcmProcessExecuter()
        {
            if (_sshClient != null && _sshClient.IsConnected)
                _sshClient.Disconnect();
        }
    }
}
