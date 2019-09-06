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

#if !NETCORE
using System.Net;
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    ///  AuthProvider which supplies authenticator instances for clients to connect to
    ///  DSE clusters secured with Kerberos. See <link>KerberosAuthenticator</link>
    ///  for how to configure client side Kerberos options. To connect to clusters
    ///  using internal authentication, use the standard method for setting
    ///  credentials. eg: <pre> Cluster cluster = Cluster.builder()
    ///  .addContactPoint(hostname) .withCredentials("username", "password") .build();
    ///  </pre>
    /// </summary>

    public class DseAuthProvider : IAuthProvider
    {
        readonly IDseCredentialsResolver _credentialsResolver;
        public DseAuthProvider()
        {
            _credentialsResolver = new SimpleDseCredentialsResolver();
        }
        public DseAuthProvider(IDseCredentialsResolver credentialsResolver)
        {
            _credentialsResolver = credentialsResolver;
        }
        public IAuthenticator NewAuthenticator(IPEndPoint host)
        {
            return new KerberosAuthenticator(_credentialsResolver.GetHostName(host), _credentialsResolver.GetCredential(host), _credentialsResolver.GetPrincipal(host));
        }
    }

    public interface IDseCredentialsResolver
    {
        string GetPrincipal(IPEndPoint host);
        string GetHostName(IPEndPoint host);
        NetworkCredential GetCredential(IPEndPoint host);
    }

    public class SimpleDseCredentialsResolver : IDseCredentialsResolver
    {
        readonly string _principal;
        readonly Dictionary<IPEndPoint, string> _hostnames = new Dictionary<IPEndPoint, string>();
        readonly NetworkCredential _credential;

        public SimpleDseCredentialsResolver(NetworkCredential credential = null, string principal = null)
        {
            _principal = principal;
            _credential = credential;
        }
        public string GetPrincipal(IPEndPoint host)
        {
            return _principal;
        }

        public NetworkCredential GetCredential(IPEndPoint host)
        {
            return _credential;
        }

        public string GetHostName(IPEndPoint host)
        {
            lock (_hostnames)
            {
                if (!_hostnames.ContainsKey(host))
                {
                    IPHostEntry entry = Dns.GetHostEntry(host.Address);
                    _hostnames.Add(host, "dse/" + entry.HostName + "@" + (_credential == null ? System.Net.NetworkInformation.IPGlobalProperties.GetIPGlobalProperties().DomainName.ToUpper() : _credential.Domain));
                }
                return _hostnames[host];
            }
        }
    }
}
#endif