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

namespace Cassandra.DataStax.Cloud
{
    /// <summary>
    /// Loads and parses the secure connection bundle.
    /// </summary>
    internal interface ISecureConnectionBundleParser
    {
        /// <summary>
        /// Loads and parses the secure connection bundle (creds.zip)
        /// </summary>
        /// <param name="path">Path of the secure connection bundle file.</param>
        /// <returns>The configuration object built from the provided bundle.</returns>
        SecureConnectionBundle ParseBundle(string path);
    }
}