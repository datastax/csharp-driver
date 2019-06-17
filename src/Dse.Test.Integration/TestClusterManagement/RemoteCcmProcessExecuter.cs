//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Diagnostics;
using Renci.SshNet;
using Renci.SshNet.Common;

namespace Dse.Test.Integration.TestClusterManagement
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


        public override ProcessOutput ExecuteCcm(string args, int timeout = 90000, bool throwOnProcessError = true)
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

                kauth.AuthenticationPrompt += delegate(object sender, AuthenticationPromptEventArgs e)
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
