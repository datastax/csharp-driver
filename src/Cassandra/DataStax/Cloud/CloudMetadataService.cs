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

using System;
using System.IO;
using System.Net;
using System.Security.Authentication;
using System.Threading.Tasks;
using Cassandra.Helpers;
using Newtonsoft.Json;
#if NETSTANDARD
using System.Net.Http;
using System.Net.Http.Headers;
#endif

namespace Cassandra.DataStax.Cloud
{
    /// <inheritdoc />
    internal class CloudMetadataService : ICloudMetadataService
    {
        /// <inheritdoc />
        public Task<CloudMetadataResult> GetClusterMetadataAsync(
            string url, SocketOptions socketOptions, SSLOptions sslOptions)
        {
#if NET452
            return GetWithWebRequestAsync(url, socketOptions, sslOptions);
#else
            if (PlatformHelper.RuntimeSupportsCloudTlsSettings())
            {
                return GetWithHttpClientAsync(url, socketOptions, sslOptions);
            }

            throw new NotSupportedException("DataStax Astra support in .NET Core requires .NET Core 2.1 runtime or later. " +
                                            "The HTTPS implementation of .NET Core 2.0 and below don't work when some TLS settings are set. " +
                                            $"The runtime that is being used is: .NET Core {PlatformHelper.GetNetCoreVersion()}");
#endif
        }

#if !NETSTANDARD

        private async Task<CloudMetadataResult> GetWithWebRequestAsync(
            string url, SocketOptions socketOptions, SSLOptions sslOptions)
        {
            ServicePointManager.SecurityProtocol |= ConvertSslProtocolEnum(sslOptions.SslProtocol);
            var request = (HttpWebRequest)WebRequest.Create(url);
            request.KeepAlive = false;
            request.Timeout = socketOptions.ConnectTimeoutMillis;
            request.Accept = "application/json";

            request.ServerCertificateValidationCallback = sslOptions.RemoteCertValidationCallback;

            if (sslOptions.CertificateCollection.Count > 0)
            {
                request.ClientCertificates.AddRange(sslOptions.CertificateCollection);
            }

            try
            {
                using (var response = (HttpWebResponse) await request.GetResponseAsync().ConfigureAwait(false))
                {
                    var responseString = await new StreamReader(response.GetResponseStream()).ReadToEndAsync().ConfigureAwait(false);
                    if ((int) response.StatusCode < 200 || (int) response.StatusCode >= 300)
                    {
                        throw GetServiceRequestException(false, url, null, (int)response.StatusCode);
                    }

                    try
                    {
                        return JsonConvert.DeserializeObject<CloudMetadataResult>(responseString);
                    }
                    catch (Exception ex)
                    {
                        throw GetServiceRequestException(true, url, ex, (int)response.StatusCode);
                    }
                }
            }
            catch (NoHostAvailableException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw GetServiceRequestException(false, url, ex);
            }
        }

        private SecurityProtocolType ConvertSslProtocolEnum(SslProtocols protocol)
        {
            SecurityProtocolType securityProtocolType = 0;
            if ((protocol & SslProtocols.Ssl3) != 0)
            {
                securityProtocolType |= SecurityProtocolType.Ssl3;
            }
            
            if ((protocol & SslProtocols.Tls) != 0)
            {
                securityProtocolType |= SecurityProtocolType.Tls;
            }
            
            if ((protocol & SslProtocols.Tls11) != 0)
            {
                securityProtocolType |= SecurityProtocolType.Tls11;
            }
            
            if ((protocol & SslProtocols.Tls12) != 0)
            {
                securityProtocolType |= SecurityProtocolType.Tls12;
            }

            return securityProtocolType;
        }

#endif

#if NETSTANDARD
        private async Task<CloudMetadataResult> GetWithHttpClientAsync(
            string url, SocketOptions socketOptions, SSLOptions sslOptions)
        {
            var handler = CreateHttpClientHandler(sslOptions);
            return await GetWithHandlerAsync(url, handler, socketOptions).ConfigureAwait(false);
        }

        private async Task<CloudMetadataResult> GetWithHandlerAsync(
            string url, HttpMessageHandler handler, SocketOptions socketOptions)
        {
            using (var httpClient = new HttpClient(handler))
            {
                string body = null;
                try
                {
                    httpClient.Timeout = TimeSpan.FromMilliseconds(socketOptions.ConnectTimeoutMillis);
                    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                    var response = await httpClient.GetAsync(url).ConfigureAwait(false);
                    body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                    if (!response.IsSuccessStatusCode)
                    {
                        throw GetServiceRequestException(false, url, null, (int) response.StatusCode);
                    }

                    try
                    {
                        return JsonConvert.DeserializeObject<CloudMetadataResult>(body);
                    }
                    catch (Exception ex2)
                    {
                        throw GetServiceRequestException(true, url, ex2, (int) response.StatusCode);
                    }
                }
                catch (NoHostAvailableException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw GetServiceRequestException(false, url, ex);
                }

            }
        }

        private HttpClientHandler CreateHttpClientHandler(SSLOptions sslOptions)
        {
            var httpClientHandler = new HttpClientHandler();
            httpClientHandler.SslProtocols = sslOptions.SslProtocol;

            httpClientHandler.CheckCertificateRevocationList = sslOptions.CheckCertificateRevocation;
            httpClientHandler.ServerCertificateCustomValidationCallback =
                (httpRequestMessage, cert, chain, errors) =>
                    sslOptions.RemoteCertValidationCallback.Invoke(httpRequestMessage, cert, chain, errors);

            if (sslOptions.CertificateCollection.Count > 0)
            {
                httpClientHandler.ClientCertificateOptions = ClientCertificateOption.Manual;
                httpClientHandler.ClientCertificates.AddRange(sslOptions.CertificateCollection);
            }

            return httpClientHandler;
        }
#endif
        private Exception GetServiceRequestException(bool isParsingError, string url, Exception exception = null, int? statusCode = null)
        {
            var message =
                isParsingError
                    ? $"There was an error while parsing the metadata service information from the Metadata Service ({url})."
                    : $"There was an error fetching the metadata information from the Cloud Metadata Service ({url}). " +
                      "Please make sure your cluster is not parked or terminated.";

            if (statusCode.HasValue)
            {
                message += $" It returned a {statusCode.Value} status code.";
            }

            if (exception != null)
            {
                message += " See inner exception for more details.";
                return new NoHostAvailableException(message, exception);
            }

            return new NoHostAvailableException(message);
        }
    }
}