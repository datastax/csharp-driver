//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra
{
    /// <summary>
    ///  Handles SASL authentication with Cassandra servers. A server which requires
    ///  authentication responds to a startup message with an challenge in the form of
    ///  an <c>AuthenticateMessage</c>. Authenticator implementations should be
    ///  able to respond to that challenge and perform whatever authentication
    ///  negotiation is required by the server. The exact nature of that negotiation
    ///  is specific to the configuration of the server.
    /// </summary>
    public interface IAuthenticator
    {
        /// <summary>
        ///  Obtain an initial response token for initializing the SASL handshake
        /// </summary>
        /// 
        /// <returns>the initial response to send to the server, may be null</returns>
        byte[] InitialResponse();

        /// <summary>
        ///  Evaluate a challenge received from the Server. Generally, this method should
        ///  return null when authentication is complete from the client perspective
        /// </summary>
        /// <param name="challenge"> the server's SASL challenge' </param>
        /// 
        /// <returns>updated SASL token, may be null to indicate the client requires no
        ///  further action</returns>
        byte[] EvaluateChallenge(byte[] challenge);
    }
}