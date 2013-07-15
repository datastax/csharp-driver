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
using System.Net;
using System.Text;
using System;
using System.IdentityModel.Selectors;
using System.Security.Principal;
using System.IdentityModel.Tokens;

namespace Cassandra
{
    /// <summary>
    ///  Responsible for authenticating with secured DSE services using Kerberos over
    ///  GSSAPI & SASL. The actual SASL negotiation is delegated to a
    ///  PrivilegedSaslClient which performs the priviledged actions on behalf of the
    ///  logged in user. </p> The SASL protocol name defaults to "dse"; should you
    ///  need to change that it can be overridden using the
    ///  <code>dse.sasl.protocol</code> system property. </p> Keytab and ticket cache
    ///  settings are specified using a standard JAAS configuration file. The location
    ///  of the file can be set using the <code>java.security.auth.login.config</code>
    ///  system property or by adding a <code>login.config.url.n</code> entry in the
    ///  <code>java.security</code> properties file. </p> See
    ///  <link>http://docs.oracle.com/javase/1.4.2/docs/guide/security/jaas/tutorials/L
    ///  oginConfigFile.html</link> for further details on the Login configuration
    ///  file and
    ///  <link>http://docs.oracle.com/javase/6/docs/technotes/guides/security/jaas/tuto
    ///  rials/GeneralAcnOnly.html</link> for more on JAAS in general. </p>
    ///  <h1>Authentication using ticket cache</h1> Run <code>kinit</code> to obtain a
    ///  ticket and populate the cache before connecting. JAAS config: <pre> DseClient
    ///  com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true
    ///  renewTGT=true; }; </pre> <h1>Authentication using a keytab file</h1> <p>To
    ///  enable authentication using a keytab file, specify its location on disk. If
    ///  your keytab contains more than one principal key, you should also specify
    ///  which one to select. <pre> DseClient
    ///  com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true
    ///  keyTab="/path/to/file.keytab" principal="user@MYDOMAIN.COM"; }; </pre>
    /// </summary>

    public class KerberosAuthenticator : IAuthenticator
    {
        private readonly Logger _logger = new Logger(typeof(KerberosAuthenticator));

        private readonly KerberosSecurityTokenProvider _oProvider;

        public KerberosAuthenticator(IPAddress host)
        {
            _oProvider = new KerberosSecurityTokenProvider("COGSERVER01", TokenImpersonationLevel.Identification, new NetworkCredential(@"p.kaplanski", "3wiOiSiDSa","COGNET"));
        }

        public byte[] InitialResponse()
        {
            KerberosRequestorSecurityToken oToken = (KerberosRequestorSecurityToken)_oProvider.GetToken(TimeSpan.FromHours(1));
            return oToken.SecurityKey.GetSymmetricKey();
        }

        public byte[] EvaluateChallenge(byte[] challenge)
        {
            return null;
        }
    }
}