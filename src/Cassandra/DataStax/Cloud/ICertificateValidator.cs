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

using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Cassandra.DataStax.Cloud
{
    /// <summary>
    /// Validates a certificate (and its chain). Used in server certificate callbacks with SslStream, HttpClient, etc.
    /// </summary>
    internal interface ICertificateValidator
    {
        bool Validate(X509Certificate cert, X509Chain chain, SslPolicyErrors errors);
    }
}