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
