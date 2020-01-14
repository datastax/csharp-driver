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

namespace Cassandra.DataStax.Auth
{
    /// <summary>
    /// Base class for Authenticator implementations that want to make use of
    /// the authentication scheme negotiation in the DseAuthenticator
    /// </summary>
    internal abstract class BaseDseAuthenticator : IAuthenticator
    {
        private readonly string _name;
        private const string DseAuthenticatorName = "com.datastax.bdp.cassandra.auth.DseAuthenticator";

        protected BaseDseAuthenticator(string name)
        {
            _name = name;
        }

        protected abstract byte[] GetMechanism();

        protected abstract byte[] GetInitialServerChallenge();

        public virtual byte[] InitialResponse()
        {
            if (!IsDseAuthenticator())
            {
                //fallback
                return EvaluateChallenge(GetInitialServerChallenge());
            }
            //send the mechanism as a first auth message
            return GetMechanism();
        }

        public abstract byte[] EvaluateChallenge(byte[] challenge);

        protected bool IsDseAuthenticator()
        {
            return _name == BaseDseAuthenticator.DseAuthenticatorName;
        }
    }
}
