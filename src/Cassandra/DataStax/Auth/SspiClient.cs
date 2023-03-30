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
using System.Threading;

using Cassandra.DataStax.Auth.Sspi;
using Cassandra.DataStax.Auth.Sspi.Contexts;
using Cassandra.DataStax.Auth.Sspi.Credentials;

namespace Cassandra.DataStax.Auth
{
    /// <summary>
    /// A Windows-only <see cref="IGssapiClient"/> implementation.
    /// </summary>
    internal class SspiClient : IGssapiClient
    {
        private delegate byte[] TransitionHandler(byte[] challenge);
        private const ContextAttrib ContextRequestAttributes = ContextAttrib.MutualAuth;
        private static readonly byte[] EmptyBuffer = new byte[0];
        private readonly TransitionHandler[] _transitions;
        private int _transitionIndex = -1;
        private volatile ClientCredential _credentials;
        private volatile ClientContext _context;

        public SspiClient()
        {
            _transitions = new TransitionHandler[]
            {
                FirstTransition,
                SecondTransition,
                ThirdTransition
            };
        }

        public void Init(string service, string host)
        {
            if (!string.IsNullOrEmpty(service))
            {
                //For the server principal: "dse/cassandra1.datastax.com@DATASTAX.COM"
                //the expected Uri is: "dse/cassandra1.datastax.com"
                service = service + "/" + host;
            }
            else
            {
                //Use string empty
                service = "";
            }
            //Acquire credentials
            _credentials = new ClientCredential(PackageNames.Kerberos);
            //Initialize security context
            _context = new ClientContext(_credentials, service, SspiClient.ContextRequestAttributes);
        }

        public byte[] EvaluateChallenge(byte[] challenge)
        {
            var index = Interlocked.Increment(ref _transitionIndex);
            if (index > 2)
            {
                throw new InvalidOperationException("No additional transitions supported");
            }
            //According to RFC 2222 7.2.1: Client can respond with no data
            //Use empty buffer instead
            return _transitions[index](challenge) ?? SspiClient.EmptyBuffer;
        }

        private byte[] FirstTransition(byte[] challenge)
        {
            _context.Init(null, out byte[] resultToken);
            return resultToken;
        }

        private byte[] SecondTransition(byte[] challenge)
        {
            _context.Init(challenge, out byte[] resultToken);
            return resultToken;
        }

        private byte[] ThirdTransition(byte[] challenge)
        {
            _context.Decrypt(challenge);

            var plainResult = new byte[]
            {
                0x1, // QOP
                0x0,
                0x0,
                0x0
            };
            return _context.Encrypt(plainResult);
        }

        public void Dispose()
        {
            var context = _context;
            if (context != null)
            {
                context.Dispose();
            }
            var credentials = _credentials;
            if (credentials != null)
            {
                credentials.Dispose();   
            }
        }
    }
}