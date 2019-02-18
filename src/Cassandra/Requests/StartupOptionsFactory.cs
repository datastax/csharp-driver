// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Collections.Generic;
using Cassandra.Helpers;

namespace Cassandra.Requests
{
    internal class StartupOptionsFactory : IStartupOptionsFactory
    {
        private const string CqlVersionOption = "CQL_VERSION";
        private const string CompressionOption = "COMPRESSION";
        private const string NoCompactOption = "NO_COMPACT";
        private const string DriverNameOption = "DRIVER_NAME";
        private const string DriverVersionOption = "DRIVER_VERSION";

        private const string DriverName = "DataStax C# Driver for Apache Cassandra";

        public IReadOnlyDictionary<string, string> CreateStartupOptions(ProtocolOptions options)
        {
            var startupOptions = new Dictionary<string, string>
            {
                { StartupOptionsFactory.CqlVersionOption, "3.0.0" }
            };

            string compressionName = null;
            switch (options.Compression)
            {
                case CompressionType.LZ4:
                    compressionName = "lz4";
                    break;
                case CompressionType.Snappy:
                    compressionName = "snappy";
                    break;
            }

            if (compressionName != null)
            {
                startupOptions.Add(StartupOptionsFactory.CompressionOption, compressionName);
            }

            if (options.NoCompact)
            {
                startupOptions.Add(StartupOptionsFactory.NoCompactOption, "true");
            }

            startupOptions.Add(StartupOptionsFactory.DriverNameOption, StartupOptionsFactory.DriverName);
            startupOptions.Add(
                StartupOptionsFactory.DriverVersionOption, MultiTargetHelpers.GetAssemblyFileVersionString(typeof(StartupOptionsFactory)));

            return startupOptions;
        }
    }
}