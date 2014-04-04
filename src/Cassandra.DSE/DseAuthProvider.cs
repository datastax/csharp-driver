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
        IDseCredentialsResolver _credentialsResolver;
        public DseAuthProvider()
        {
            _credentialsResolver = new SimpleDseCredentialsResolver();
        }
        public DseAuthProvider(IDseCredentialsResolver credentialsResolver)
        {
            _credentialsResolver = credentialsResolver;
        }
        public IAuthenticator NewAuthenticator(IPAddress host)
        {
            return new KerberosAuthenticator(_credentialsResolver.GetHostName(host), _credentialsResolver.GetCredential(host), _credentialsResolver.GetPrincipal(host));
        }
    }

    public interface IDseCredentialsResolver
    {
        string GetPrincipal(IPAddress host);
        string GetHostName(IPAddress host);
        NetworkCredential GetCredential(IPAddress host);
    }

    public class SimpleDseCredentialsResolver : IDseCredentialsResolver
    {
        string _principal;
        Dictionary<IPAddress, string> _hostnames = new Dictionary<IPAddress, string>();
        NetworkCredential _credential;

        public SimpleDseCredentialsResolver(NetworkCredential credential = null, string principal = null)
        {
            _principal = principal;
            _credential = credential;
        }
        public string GetPrincipal(IPAddress host)
        {
            return _principal;
        }

        public NetworkCredential GetCredential(IPAddress host)
        {
            return _credential;
        }

        public string GetHostName(IPAddress host)
        {
            lock (_hostnames)
            {
                if (!_hostnames.ContainsKey(host))
                {
                    IPHostEntry entry = Dns.GetHostEntry(host);
                    _hostnames.Add(host, "dse/" + entry.HostName + "@" + (_credential == null ? System.Net.NetworkInformation.IPGlobalProperties.GetIPGlobalProperties().DomainName.ToUpper() : _credential.Domain));
                }
                return _hostnames[host];
            }
        }
    }
}