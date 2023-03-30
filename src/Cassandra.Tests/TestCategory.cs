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

namespace Cassandra.Tests
{
    /// <summary>
    /// Test categories to specify which tests should run in each CI platform / schedule.
    /// </summary>
    public static class TestCategory
    {
        /// <summary>
        /// Currently these tests are skipped in all CI schedules.
        /// They need to be refactored or removed.
        /// </summary>
        public const string Long = "long";

        /// <summary>
        /// These tests run in all CI schedules (both Appveyor and Jenkins) except if they are marked with the categories below.
        /// </summary>
        public const string Short = "short";
        
        /// <summary>
        /// These tests run once in Appveyor per commit (not for the entire matrix) and in all Jenkins schedules.
        /// </summary>
        public const string RealCluster = "realcluster";

        /// <summary>
        /// These tests run in Jenkins nightly builds only.
        /// </summary>
        public const string RealClusterLong = "realclusterlong";
        
        /// <summary>
        /// These tests run once in Jenkins per commit (not for the entire matrix).
        /// </summary>
        public const string Cloud = "cloud";
        
        /// <summary>
        /// These tests can also be marked with other categories.
        /// This category is used in the smoke tests against DSE or Apache C* release candidates.
        /// https://github.com/datastax/cassandra-drivers-smoke-test
        /// </summary>
        public const string ServerApi = "serverapi";
        
        /// <summary>
        /// These tests can only run against a kerberos enabled cluster.
        /// At the moment, they are skipped in all CI schedules.
        /// </summary>
        public const string Kerberos = "kerberos";
    }
}