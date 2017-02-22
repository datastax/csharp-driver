//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
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