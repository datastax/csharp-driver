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

namespace MinimalExample
{
    /// <summary>
    /// Connects to a database using a secure connection bundle and executes a simple query
    /// to retrieve the Cluster Name.
    /// </summary>
    internal class Program
    {
        // Fill in these constants with your database credentials and bundle path
        private const string BundlePath = @"";
        private const string Username = @"";
        private const string Password = @"";

        private static void Main(string[] args)
        {
            var session =
                Cluster.Builder()
                    .WithCloudSecureConnectionBundle(Program.BundlePath)
                    .WithCredentials(Program.Username, Program.Password)
                    .Build()
                    .Connect();

            var rowSet = session.Execute("select * from system.local");
            Console.WriteLine(rowSet.First().GetValue<string>("cluster_name"));

            Console.WriteLine("Press enter to exit.");
            Console.ReadLine();
        }
    }
}