//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using Dse.Test.Integration.TestClusterManagement;

namespace Dse.Test.Integration.ClusterManagement
{
    public class CcmHelper
    {
        private const string InitialContactPointSufix = "1";
        private const string InitialIpPrefix = "127.0.0.";

        private static CcmDseBridge _ccmDseBridge;

        public static string DseInitialIpPrefix = 
            Environment.GetEnvironmentVariable("DSE_INITIAL_IPPREFIX") ?? InitialIpPrefix;

        private static Version _dseVersion;

        public static Version DseVersion
        {
            get
            {
                if (_dseVersion == null)
                {
                    var dseVersion = Environment.GetEnvironmentVariable("DSE_VERSION") ?? "dse-5.0.0";
                    if (dseVersion.Contains("dse-"))
                    {
                        dseVersion = dseVersion.Substring(dseVersion.IndexOf("-", StringComparison.Ordinal) + 1);
                    }
                    _dseVersion = Version.Parse(dseVersion);
                }
                return _dseVersion;
            }
        }

        public static void Start(int amountOfNodes, string[] dseYamlOptions = null, string[] cassYamlOptions = null, string[] jvmArgs = null, string nodeWorkload = null)
        {
            var dseDir = Environment.GetEnvironmentVariable("DSE_PATH") ?? "/home/vagrant/dse-5.0.0";
            var dseInitialIpPrefix = DseInitialIpPrefix;

            ICcmProcessExecuter executer;

            var dseRemote = bool.Parse(Environment.GetEnvironmentVariable("DSE_IN_REMOTE_SERVER") ?? "true");
            if (dseRemote)
            {
                var remoteDseServer = Environment.GetEnvironmentVariable("DSE_SERVER_IP") ?? "127.0.0.1";
                var remoteDseServerUser = Environment.GetEnvironmentVariable("DSE_SERVER_USER") ?? "vagrant";
                var remoteDseServerPassword = Environment.GetEnvironmentVariable("DSE_SERVER_PWD") ?? "vagrant";
                var remoteDseServerPort = int.Parse(Environment.GetEnvironmentVariable("DSE_SERVER_PORT") ?? "2222");

                if (remoteDseServerPort == 0)
                {
                    remoteDseServerPort = 22;
                }

                var remoteDseServerUserPrivateKey = Environment.GetEnvironmentVariable("DSE_SERVER_PRIVATE_KEY");
                executer = new RemoteCcmProcessExecuter(remoteDseServer, remoteDseServerUser, remoteDseServerPassword,
                    remoteDseServerPort, remoteDseServerUserPrivateKey);
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
            if (!string.IsNullOrEmpty(nodeWorkload))
            {
                for (var i = 1; i <= amountOfNodes; i++)
                {
                    _ccmDseBridge.SetWorkload(i, nodeWorkload);
                }
            }
            _ccmDseBridge.Start(jvmArgs);
        }

        public static void Remove()
        {
            if (_ccmDseBridge == null) return;

            try
            {
                _ccmDseBridge.Remove();
            }
            catch
            {
                //ignore
            }
            
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
