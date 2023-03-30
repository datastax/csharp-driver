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

using System.Threading.Tasks;

namespace Cassandra.DataStax.Cloud
{
    /// <summary>
    /// Client to interact with the Cloud Metadata Service.
    /// </summary>
    internal interface ICloudMetadataService
    {
        /// <summary>
        /// Retrieve the cloud cluster's metadata from the cloud metadata service.
        /// </summary>
        /// <param name="url">Metadata endpoint</param>
        /// <param name="socketOptions">Socket options to use for the HTTPS request (timeout).</param>
        /// <param name="sslOptions">SSL Options to use for the HTTPS request.</param>
        Task<CloudMetadataResult> GetClusterMetadataAsync(string url, SocketOptions socketOptions, SSLOptions sslOptions);
    }
}