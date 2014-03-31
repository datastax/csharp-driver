using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Cassandra
{
    public class SSLOptions
    {
        private readonly Logger _logger = new Logger(typeof (SSLOptions));
        private readonly RemoteCertificateValidationCallback _remoteCertValidationCallback;
        private readonly SslProtocols _sslProtocol = SslProtocols.Tls;
        private bool _checkCertificateRevocation;

        /// <summary>
        /// Verifies Cassandra host SSL certificate used for authentication.
        /// </summary>
        public RemoteCertificateValidationCallback RemoteCertValidationCallback
        {
            get { return _remoteCertValidationCallback; }
        }

        /// <summary>
        /// Ssl Protocol used for communication with Cassandra hosts.
        /// </summary>
        public SslProtocols SslProtocol
        {
            get { return _sslProtocol; }
        }

        /// <summary>
        ///  Creates SSLOptions with default values.   
        /// </summary>
        public SSLOptions()
        {
            _remoteCertValidationCallback = ValidateServerCertificate;
        }

        /// <summary>
        /// Creates SSL options used for SSL connections with Casandra hosts. 
        /// </summary>
        /// <param name="SSLProtocol">type of SSL protocol, default set to Tls.</param>
        /// <param name="CertificateRevocation">specifies whether the certificate revocation list is checked during connection authentication.</param>
        /// <param name="RemoteCertValidationCallback">verifies Cassandra host SSL certificate used for authentication.
        ///     <remarks>
        ///         Default RemoteCertificateValidationCallback won't establish a connection if any error will occur.         
        ///     </remarks> 
        ///     </param>
        public SSLOptions(SslProtocols SSLProtocol, bool CertificateRevocation, RemoteCertificateValidationCallback RemoteCertValidationCallback)
        {
            _sslProtocol = SSLProtocol;
            _checkCertificateRevocation = CertificateRevocation;
            _remoteCertValidationCallback = RemoteCertValidationCallback;
        }

        private bool ValidateServerCertificate(
            object sender,
            X509Certificate certificate,
            X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            _logger.Error(string.Format("Cassandra node SSL certificate validation error(s): {0}", sslPolicyErrors));

            // Do not allow this client to communicate with unauthenticated Cassandra hosts.
            return false;
        }
    }
}