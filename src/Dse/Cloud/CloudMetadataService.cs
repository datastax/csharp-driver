//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Newtonsoft.Json;

using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

namespace Dse.Cloud
{
    /// <inheritdoc />
    internal class CloudMetadataService : ICloudMetadataService
    {
        /// <inheritdoc />
        public async Task<CloudMetadataResult> GetClusterMetadataAsync(string url, string certFile)
        {
#if NETSTANDARD
            var httpClientHandler = new HttpClientHandler();
            httpClientHandler.SslProtocols = SslProtocols.Tls12 | SslProtocols.Tls;
#pragma warning restore 618

            // TODO remove
            httpClientHandler.ServerCertificateCustomValidationCallback = (httpRequestMessage, cert, cetChain, policyErrors) => true;

            if (certFile != null)
            {
                httpClientHandler.ClientCertificateOptions = ClientCertificateOption.Manual;
                httpClientHandler.ClientCertificates.Add(new X509Certificate2(certFile));
            }

            using (var httpClient = new HttpClient(httpClientHandler))
            {
                string body = null;
                try
                {
                    httpClient.Timeout = TimeSpan.FromMilliseconds(20000);
                    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                    var response = await httpClient.GetAsync(url).ConfigureAwait(false);
                    body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                    if (!response.IsSuccessStatusCode)
                    {
                        throw new DriverInternalError(
                            $"Metadata Service returned a {(int)response.StatusCode} status code. " +
                            $"Response received: {body}");
                    }
                }
                catch (Exception ex)
                {
                    throw new DriverInternalError("Could not fetch cluster information from the Metadata Service.", ex);
                }

                try
                {
                    return JsonConvert.DeserializeObject<CloudMetadataResult>(body);
                }
                catch (Exception ex)
                {
                    throw new DriverInternalError("Could not parse the cluster information returned by the Metadata Service.", ex);
                }
            }
#else
            System.Net.ServicePointManager.SecurityProtocol |= SecurityProtocolType.Tls12;
            var request = (HttpWebRequest)WebRequest.Create(url);
            request.KeepAlive = false;
            request.Timeout = 5000;
            request.Accept = new MediaTypeWithQualityHeaderValue("application/json").ToString();

            // TODO REMOVE
            request.ServerCertificateValidationCallback = (sender, certificate, chain, errors) => true;

            if (certFile != null)
            {
                request.ClientCertificates.Add(new X509Certificate2(certFile));
            }

            try
            {
                using (var response = (HttpWebResponse)await request.GetResponseAsync().ConfigureAwait(false))
                {
                    var responseString = await new StreamReader(response.GetResponseStream()).ReadToEndAsync()
                        .ConfigureAwait(false);
                    if ((int)response.StatusCode < 200 || (int)response.StatusCode >= 300)
                    {
                        throw new DriverInternalError(
                            $"Metadata Service returned a {(int)response.StatusCode} status code. " +
                            $"Response received: {responseString}");
                    }

                    try
                    {
                        return JsonConvert.DeserializeObject<CloudMetadataResult>(responseString);
                    }
                    catch (Exception ex)
                    {
                        throw new DriverInternalError("Could not parse the cluster information returned by the Metadata Service.", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new DriverInternalError("Could not fetch cluster information from the Metadata Service.", ex);
            }
#endif
        }
    }
}