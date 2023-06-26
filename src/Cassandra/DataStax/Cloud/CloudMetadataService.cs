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
using System.Threading.Tasks;
using Cassandra.Helpers;

namespace Cassandra.DataStax.Cloud
{
    /// <inheritdoc />
    internal class CloudMetadataService : ICloudMetadataService
    {
        /// <inheritdoc />
        public Task<CloudMetadataResult> GetClusterMetadataAsync(
            string url, SocketOptions socketOptions, SSLOptions sslOptions)
        {
            throw new NotSupportedException("DataStax Astra support in .NET Core requires .NET Core 2.1 runtime or later. " +
                                            "The HTTPS implementation of .NET Core 2.0 and below don't work when some TLS settings are set. " +
                                            $"The runtime that is being used is: .NET Core {PlatformHelper.GetNetCoreVersion()}");
        }

        private Exception GetServiceRequestException(bool isParsingError, string url, Exception exception = null, int? statusCode = null)
        {
            var message =
                isParsingError
                    ? $"There was an error while parsing the metadata service information from the Metadata Service ({url})."
                    : $"There was an error fetching the metadata information from the Cloud Metadata Service ({url}). " +
                      "Please make sure your cluster is not parked or terminated.";

            if (statusCode.HasValue)
            {
                message += $" It returned a {statusCode.Value} status code.";
            }

            if (exception != null)
            {
                message += " See inner exception for more details.";
                return new NoHostAvailableException(message, exception);
            }

            return new NoHostAvailableException(message);
        }
    }
}