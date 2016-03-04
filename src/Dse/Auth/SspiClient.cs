using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Dse.Auth.Sspi;
using Dse.Auth.Sspi.Contexts;
using Dse.Auth.Sspi.Credentials;

namespace Dse.Auth
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
            if (!String.IsNullOrEmpty(service))
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
            _context = new ClientContext(_credentials, service, ContextRequestAttributes);
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
            return _transitions[index](challenge) ?? EmptyBuffer;
        }

        private byte[] FirstTransition(byte[] challenge)
        {
            byte[] resultToken;
            _context.Init(null, out resultToken);
            return resultToken;
        }

        private byte[] SecondTransition(byte[] challenge)
        {
            byte[] resultToken;
            _context.Init(challenge, out resultToken);
            return resultToken;
        }

        private byte[] ThirdTransition(byte[] challenge)
        {
            _context.Decrypt(challenge);

            var plainResult = new byte[]
            {
                0x1, // QOP
                0x1, // MAX BUF SIZE
                0x0, // MAX BUF SIZE
                0x0 // MAX BUF SIZE
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
