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

using System.Net;

namespace Cassandra
{
    /// <summary>
    /// Provides <see cref="IAuthenticator"/> instances for use when connecting to Cassandra nodes. See 
    /// <see cref="PlainTextAuthProvider"/> for an implementation which uses SASL PLAIN mechanism to authenticate using
    /// username/password strings.
    /// </summary>
    public interface IAuthProvider
    {
        /// <summary>
        /// The <see cref="IAuthenticator"/> to use when connecting to host.
        /// </summary>
        /// <param name="host">The Cassandra host to connect to. </param>
        /// <returns>The authentication instance to use.</returns>
        IAuthenticator NewAuthenticator(IPEndPoint host);
    }

    /// <summary>
    /// Represents a <see cref="IAuthProvider"/> that is dependant on the name provided by Cassandra.
    /// </summary>
    /// <exclude />
    public interface IAuthProviderNamed : IAuthProvider
    {
        /// <summary>
        /// Sets the authenticator name from Cassandra.
        /// <para>
        /// This method is guaranteed to be called before <see cref="IAuthProvider.NewAuthenticator"/>.
        /// </para>
        /// </summary>
        void SetName(string name);
    }
}
