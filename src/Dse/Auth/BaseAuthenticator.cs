using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Cassandra;

namespace Dse.Auth
{
    /// <summary>
    /// Base class for Authenticator implementations that want to make use of
    /// the authentication scheme negotiation in the DseAuthenticator
    /// </summary>
    internal abstract class BaseAuthenticator : IAuthenticator
    {
        private readonly string _name;
        private const string DseAuthenticatorName = "com.datastax.bdp.cassandra.auth.DseAuthenticator";

        protected BaseAuthenticator(string name)
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
            return _name == DseAuthenticatorName;
        }
    }
}
