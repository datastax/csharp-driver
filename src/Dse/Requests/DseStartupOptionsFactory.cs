// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using System.Collections.Generic;
using System.Linq;

namespace Dse.Requests
{
    internal class DseStartupOptionsFactory : IStartupOptionsFactory
    {
        public const string ApplicationNameOption = "APPLICATION_NAME";
        public const string ApplicationVersionOption = "APPLICATION_VERSION";
        public const string ClientIdOption = "CLIENT_ID";

        private readonly string _appName;
        private readonly string _appVersion;
        private readonly Guid _clusterId;
        private readonly IStartupOptionsFactory _startupOptionsFactory;

        public DseStartupOptionsFactory(Guid clusterId, string appVersion, string appName)
        {
            _appName = appName;
            _appVersion = appVersion;
            _clusterId = clusterId;
            _startupOptionsFactory = new StartupOptionsFactory();
        }

        public IReadOnlyDictionary<string, string> CreateStartupOptions(ProtocolOptions options)
        {
            var startupOptions = _startupOptionsFactory.CreateStartupOptions(options).ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            if (_appName != null)
            {
                startupOptions[DseStartupOptionsFactory.ApplicationNameOption] = _appName;
            }

            if (_appVersion != null)
            {
                startupOptions[DseStartupOptionsFactory.ApplicationVersionOption] = _appVersion;
            }
            
            startupOptions[DseStartupOptionsFactory.ClientIdOption] = _clusterId.ToString();
            return startupOptions;
        }
    }
}