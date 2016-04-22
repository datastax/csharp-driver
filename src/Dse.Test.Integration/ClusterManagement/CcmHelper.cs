using System;
using System.Configuration;
using Dse.Test.Integration.TestBase;

namespace Dse.Test.Integration.ClusterManagement
{
    public class CcmHelper
    {
        private const string InitialContactPointSufix = "1";
        private const string InitialIpPrefix = "127.0.0.";

        private static CcmDseBridge _ccmDseBridge;

        public static string DseInitialIpPrefix = 
            Environment.GetEnvironmentVariable("DSE_INITIAL_IPPREFIX") ?? ConfigurationManager.AppSettings["DseInitialIpPrefix"];

        public static void Start(int amountOfNodes, string[] dseYamlOptions = null, string[] cassYamlOptions = null, string[] jvmArgs = null, bool enableGraph = false)
        {
            var dseDir = Environment.GetEnvironmentVariable("DSE_PATH") ??
                         ConfigurationManager.AppSettings["DseInstallPath"];

            if (string.IsNullOrEmpty(dseDir))
            {
                throw new ConfigurationErrorsException("You must specify the DSE path either via DSE_PATH environment variable or by 'DseInstallPath' app setting");
            }

            var dseInitialIpPrefix = DseInitialIpPrefix;

            if (string.IsNullOrEmpty(dseInitialIpPrefix))
            {
                dseInitialIpPrefix = InitialIpPrefix;
            }

            ICcmProcessExecuter executer;

            var dseRemote = bool.Parse(Environment.GetEnvironmentVariable("DSE_IN_REMOTE_SERVER") ??
                         ConfigurationManager.AppSettings["DseInRemoteServer"]);
            if (dseRemote)
            {
                var remoteDseServer = Environment.GetEnvironmentVariable("DSE_SERVER_IP") ??
                                      ConfigurationManager.AppSettings["DseServerIp"];

                if (string.IsNullOrEmpty(remoteDseServer))
                {
                    throw new ConfigurationErrorsException(
                        "You must specify the DSE server ip either via DSE_SERVER_IP environment variable or by 'DseServerIp' app setting");
                }

                var remoteDseServerUser = Environment.GetEnvironmentVariable("DSE_SERVER_USER") ??
                                          ConfigurationManager.AppSettings["DseServerUser"];

                if (string.IsNullOrEmpty(remoteDseServerUser))
                {
                    throw new ConfigurationErrorsException(
                        "You must specify the DSE server user to connect through ssh either via DSE_SERVER_USER environment variable or by 'DseServerUser' app setting");
                }

                var remoteDseServerPassword = Environment.GetEnvironmentVariable("DSE_SERVER_PWD") ??
                                              ConfigurationManager.AppSettings["DseServerPwd"];

                if (string.IsNullOrEmpty(remoteDseServerPassword))
                {
                    throw new ConfigurationErrorsException(
                        "You must specify the DSE server user's password to connect through ssh either via DSE_SERVER_PWD environment variable or by 'DseServerPwd' app setting");
                }

                var remoteDseServerPort = int.Parse(Environment.GetEnvironmentVariable("DSE_SERVER_PORT") ??
                                                      ConfigurationManager.AppSettings["DseServerPort"]);

                if (remoteDseServerPort == 0)
                {
                    remoteDseServerPort = 22;
                }
                executer = new RemoteCcmProcessExecuter(remoteDseServer, remoteDseServerUser, remoteDseServerPassword,
                    remoteDseServerPort);
            }
            else
            {
                executer = new LocalCcmProcessExecuter();
            }


            _ccmDseBridge = new CcmDseBridge(
                TestUtils.GetTestClusterNameBasedOnTime(), 
                dseInitialIpPrefix, 
                dseDir,
                executer);
            
            try
            {
                Remove();
            }
            catch
            {
                //Pre-emptive remove failed, nevermind
            }

            _ccmDseBridge.Create(string.Empty, false); //passing empty version
            _ccmDseBridge.Populate(amountOfNodes, 0, false);
            if (cassYamlOptions != null)
            {
                _ccmDseBridge.UpdateConfig(cassYamlOptions);
            }
            if (dseYamlOptions != null)
            {
                _ccmDseBridge.UpdateDseConfig(dseYamlOptions);
            }
            if (enableGraph)
            {
                for (var i = 1; i <= amountOfNodes; i++)
                {
                    _ccmDseBridge.SetWorkload(i, "graph");
                }
            }
            _ccmDseBridge.Start(jvmArgs);
        }

        public static void Remove()
        {
            if (_ccmDseBridge == null) return;

            _ccmDseBridge.Remove();
        }

        public static string InitialContactPoint
        {
            get
            {
                return DseInitialIpPrefix + InitialContactPointSufix;
            }
        }
    }
}
