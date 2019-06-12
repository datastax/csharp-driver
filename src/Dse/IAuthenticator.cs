//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
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
