//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.IO;
using Dse.Test.Integration.TestClusterManagement;

namespace Dse.Test.Integration.ClusterManagement
{
    public class CcmDseBridge : CcmBridge
    {
        private readonly string _dseInstallPath;

        public CcmDseBridge(string name, string ipPrefix, string dseInstallPath, ICcmProcessExecuter executer = null)
            : base(name, ipPrefix, executer)
        {
            _dseInstallPath = dseInstallPath;
        }

        public override void Create(string version, bool useSsl)
        {
            var sslParams = "";
            if (useSsl)
            {
                var sslPath = Path.Combine(GetHomePath(), "ssl");
                if (!File.Exists(Path.Combine(sslPath, "keystore.jks")))
                {
                    throw new Exception(string.Format("In order to use SSL with CCM you must provide have the keystore.jks and cassandra.crt files located in your {0} folder", sslPath));
                }
                sslParams = "--ssl " + sslPath;
            }
            ExecuteCcm(
                string.Format("create {0} --dse -i {1} {2} --install-dir={3} {4}", Name, IpPrefix, 
                    (string.IsNullOrEmpty(version) ? string.Empty : string.Format("-v {0}", version)),
                    _dseInstallPath, sslParams));
        }
    }
}
