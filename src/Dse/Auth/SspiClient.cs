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
    internal class SspiClient : IGssapiClient
    {
        private delegate byte[] TransitionHandler(byte[] challenge);
        private const ContextAttrib ContextRequestAttributes =
            ContextAttrib.InitIntegrity |
            ContextAttrib.ReplayDetect |
            ContextAttrib.SequenceDetect |
            ContextAttrib.MutualAuth |
            ContextAttrib.Delegate |
            ContextAttrib.Confidentiality;
        private static readonly byte[] EmptyBuffer = new byte[0];

        private readonly TransitionHandler[] _transitions;
        private int _transitionIndex = -1;
        private ClientCredential _credentials;
        private ClientContext _context;

        public SspiClient()
        {
            _transitions = new TransitionHandler[]
            {
                FirstTransition,
                SecondTransition
            };
        }

        public void Init(string host)
        {
            //For the principal    "dse/cassandra1.datastax.com@DATASTAX.COM"
            //the expected uri is: "dse@cassandra1.datastax.com"

            //Acquire credentials
            _credentials = new ClientCredential(PackageNames.Kerberos);
            _context = new ClientContext(_credentials, "", ContextRequestAttributes);
            //Initialize security context
        }

        public byte[] EvaluateChallenge(byte[] challenge)
        {
            var index = Interlocked.Increment(ref _transitionIndex);
            if (index > 2)
            {
                throw new InvalidOperationException("No additional transitions supported");
            }
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

        public void Dispose()
        {
            
        }
    }
}
