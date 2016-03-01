using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Cassandra;

namespace Dse.Auth
{
    public class DseGssapiAuthProvider : IAuthProviderNamed
    {
        private string _name;

        public IAuthenticator NewAuthenticator(IPEndPoint host)
        {
            return new GssapiAuthenticator(_name, host);
        }

        public void SetName(string name)
        {
            _name = name;
        }

        private class GssapiAuthenticator : BaseAuthenticator, IDisposable
        {
            private readonly IPEndPoint _hostEndPoint;
            private readonly IGssapiClient _client;

            public GssapiAuthenticator(string authenticatorName, IPEndPoint hostEndPoint) : 
                base(authenticatorName)
            {
                _hostEndPoint = hostEndPoint;
                _client = GssapiClientFactory.CreateNew();
            }

            protected override byte[] GetMechanism()
            {
                throw new NotImplementedException();
            }

            protected override byte[] GetInitialServerChallenge()
            {
                throw new NotImplementedException();
            }

            public override byte[] InitialResponse()
            {
                //TODO: Resolve
                _client.Init(null);
                //return mechanism;
                throw new NotImplementedException();
            }

            public override byte[] EvaluateChallenge(byte[] challenge)
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {
                _client.Dispose();
            }
        }
    }
}
