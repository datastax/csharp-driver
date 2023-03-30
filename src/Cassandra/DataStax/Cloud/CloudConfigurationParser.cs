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

using System;
using System.IO;
using Newtonsoft.Json;

namespace Cassandra.DataStax.Cloud
{
    /// <inheritdoc />
    internal class CloudConfigurationParser : ICloudConfigurationParser
    {
        private static readonly Logger Logger = new Logger(typeof(CloudConfigurationParser));

        /// <inheritdoc />
        public CloudConfiguration ParseConfig(Stream stream)
        {
            CloudConfiguration cloudConfiguration;
            using (var configStream = new StreamReader(stream))
            {
                var json = configStream.ReadToEnd();
                cloudConfiguration = JsonConvert.DeserializeObject<CloudConfiguration>(json);
            }

            ValidateConfiguration(cloudConfiguration);

            return cloudConfiguration;
        }
        
        private void ValidateConfiguration(CloudConfiguration config)
        {
            if (config == null)
            {
                throw new ArgumentException("Config file is empty.");
            }

            if (config.Port == 0)
            {
                throw new ArgumentException("Could not parse the \"port\" property from the config file.");
            }

            if (string.IsNullOrWhiteSpace(config.Host))
            {
                throw new ArgumentException("Could not parse the \"host\" property from the config file.");
            }
            
            if (string.IsNullOrEmpty(config.CertificatePassword))
            {
                CloudConfigurationParser.Logger.Warning("Could not parse the \"pfxCertPassword\" property from the config file.");
            }
        }
    }
}