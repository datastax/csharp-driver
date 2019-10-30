//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

using Cassandra;

namespace SslTwoWayAuth
{
    /// <summary>
    /// To enable client authentication, the client application must provide a certificate collection which will be used
    /// by the driver to fetch a suitable client certificate for its connections.
    /// If the certificate authority is added to the Personal location of the Local Machine store
    /// (using the windows certificate manager tool) then you can provide the local machine store's collection.
    /// </summary>
    public class WindowsCertificateStoreExample
    {
        // Set these constants accordingly
        private static readonly string[] ContactPoints = { "127.0.0.1" };

        public static void Run()
        {
            X509Certificate2Collection collection;
            using (var store = new X509Store(StoreLocation.LocalMachine))
            {
                store.Open(OpenFlags.ReadOnly);
                collection = store.Certificates;
            }

            var cluster = Cluster.Builder()
                .AddContactPoints(WindowsCertificateStoreExample.ContactPoints)
                .WithSSL(new SSLOptions()
                    .SetCertificateCollection(collection))
                .Build();

            var session = cluster.Connect();

            var rowSet = session.Execute("select * from system_schema.keyspaces");
            Console.WriteLine(string.Join(Environment.NewLine, rowSet.Select(row => row.GetValue<string>("keyspace_name"))));
        }
    }
}