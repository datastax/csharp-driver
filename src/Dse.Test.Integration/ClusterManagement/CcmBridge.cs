//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.IO;
using Dse.Test.Integration.TestBase;

namespace Dse.Test.Integration.ClusterManagement
{
    public class CcmBridge : IDisposable
    {
        public const int DefaultCmdTimeout = 90 * 1000;
        public string Name { get; private set; }
        public string IpPrefix { get; private set; }
        protected readonly ICcmProcessExecuter CcmProcessExecuter;

        public CcmBridge(string name, string ipPrefix)
        {
            Name = name;
            IpPrefix = ipPrefix;
            CcmProcessExecuter = new LocalCcmProcessExecuter();
        }

        public CcmBridge(string name, string ipPrefix, ICcmProcessExecuter executer) : this(name, ipPrefix)
        {
            CcmProcessExecuter = executer;
        }

        public void Dispose()
        {
        }

        public virtual void Create(string version, bool useSsl)
        {
            var sslParams = "";
            if (useSsl)
            {
                var sslPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "ssl");
                if (!File.Exists(Path.Combine(sslPath, "keystore.jks")))
                {
                    throw new Exception(string.Format("In order to use SSL with CCM you must provide have the keystore.jks and cassandra.crt files located in your {0} folder", sslPath));
                }
                sslParams = "--ssl " + sslPath;
            }
            ExecuteCcm(string.Format("create {0} -v {1} {2}", Name, version, sslParams));
        }

        public void Start(string[] jvmArgs = null)
        {
            var parameters = new List<string>
            {
                "start",
                "--wait-for-binary-proto"
            };
            if (jvmArgs != null)
            {
                foreach (var arg in jvmArgs)
                {
                    parameters.Add("--jvm_arg");
                    parameters.Add(arg);
                }
            }
            ExecuteCcm(string.Join(" ", parameters));
        }

        public void UpdateConfig(params string[] configs)
        {
            foreach (var c in configs)
            {
                ExecuteCcm(string.Format("updateconf \"{0}\"", c));
            }
        }

        public void UpdateDseConfig(params string[] configs)
        {
            foreach (var c in configs)
            {
                ExecuteCcm(string.Format("updatedseconf \"{0}\"", c));
            }
        }

        public void Populate(int dc1NodeLength, int dc2NodeLength, bool useVNodes)
        {
            var parameters = new List<string>
            {
                "populate",
                "-n",
                dc1NodeLength + (dc2NodeLength > 0 ? ":" + dc2NodeLength : null),
                "-i",
                IpPrefix
            };
            if (useVNodes)
            {
                parameters.Add("--vnodes");
            }
            ExecuteCcm(string.Join(" ", parameters));
        }

        public void Start(int n, string additionalArgs = null)
        {
            ExecuteCcm(string.Format("node{0} start --wait-for-binary-proto {1}", n, additionalArgs));
        }

        internal void SetWorkload(int nodeId, string workload)
        {
            ExecuteCcm(string.Format("node{0} setworkload {1}", nodeId, workload));
        }

        public void Remove()
        {
            ExecuteCcm("remove");
        }

        public ProcessOutput ExecuteCcm(string args, int timeout = DefaultCmdTimeout, bool throwOnProcessError = true)
        {
            return CcmProcessExecuter.ExecuteCcm(args, timeout, throwOnProcessError);
        }
    }
}
