//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

#if !NETCORE

using System;
using System.Text;

namespace Cassandra
{
    internal static class SASL
    {
        static public byte[] FinalHandshake(SSPI.SSPIHelper sspi, byte[] challenge, string userName)
        {
            if (challenge.Length == 0)
            {
                //S0
                return new byte[0];
            }
            else
            {
                //S1
                //byte[] decryptedChallenge;
                //sspi.Unwrap(challenge.Length, challenge, out decryptedChallenge); // pure buffer - need SASL parser to preceed

                byte[] userNameUtf8 = null;
                if (userName != null && userName.Length > 0)
                    userNameUtf8 = Encoding.UTF8.GetBytes(userName);

                byte[] rawResponse = new byte[4 + ((userNameUtf8 == null) ? 0 : userNameUtf8.Length)];
                rawResponse[0] = 1; // QOP
                rawResponse[1] = 1; // MAX BUF SIZE 
                rawResponse[2] = 0; // MAX BUF SIZE 
                rawResponse[3] = 0; // MAX BUF SIZE 

                if (userNameUtf8 != null)
                    Buffer.BlockCopy(userNameUtf8, 0, rawResponse, 4, userNameUtf8.Length);

                byte[] encryptedResponse;
                sspi.Wrap(rawResponse, out encryptedResponse);
                return encryptedResponse;
            }
        }
    }
}
#endif