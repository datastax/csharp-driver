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

using Cassandra;

namespace SslServerAuthOnly
{
    /// <summary>
    /// If the certificate authority is added to the Trusted Root Certification Authorities location of the Local Machine store
    /// (using the windows certificate manager tool) then you just have to call .WithSSL() on the builder to enable server authentication.
    /// </summary>
    public class WindowsCertificateStoreExample
    {
        // Set these constants accordingly
        private static readonly string[] ContactPoints = { "127.0.0.1" };

        public static void Run()
        {
            var cluster = Cluster.Builder()
                .AddContactPoints(WindowsCertificateStoreExample.ContactPoints)
                .WithSSL()
                .Build();

            var session = cluster.Connect();

            var rowSet = session.Execute("select * from system_schema.keyspaces");
            Console.WriteLine(string.Join(Environment.NewLine, rowSet.Select(row => row.GetValue<string>("keyspace_name"))));
        }
    }
}