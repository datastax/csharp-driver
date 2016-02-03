using System;
using System.IO;
using Dse.Test.Integration.TestBase;

namespace Dse.Test.Integration.ClusterManagement
{
    public class CcmDseBridge : CcmBridge
    {
        private readonly string _dseInstallPath;
        private readonly string _dseYamlOption;

        public CcmDseBridge(string name, string ipPrefix, string dseInstallPath, ICcmProcessExecuter executer = null, string yamlOption = null) : base(name, ipPrefix, executer)
        {
            _dseInstallPath = dseInstallPath;
            _dseYamlOption = yamlOption;
        }

        public override void Create(string version, bool useSsl)
        {
            var sslParams = "";
            if (useSsl)
            {
                var sslPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "ssl");
                if (!File.Exists(Path.Combine(sslPath, "keystore.jks")))
                {
                    throw new Exception(
                        string.Format("In order to use SSL with CCM you must provide have the keystore.jks and cassandra.crt files located in your {0} folder", sslPath));
                }
                sslParams = "--ssl " + sslPath;
            }
            ExecuteCcm(
                string.Format("create {0} --dse -i {1} {2} --install-dir={3} {4}", Name, IpPrefix, 
                    (string.IsNullOrEmpty(version) ? string.Empty : string.Format("-v {0}", version)),
                    _dseInstallPath, sslParams));

            if (!string.IsNullOrEmpty(_dseYamlOption))
                ExecuteCcm(string.Format("updatedseconf {0}", _dseYamlOption));
        }

    }
}
