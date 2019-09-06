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

namespace Cassandra.IntegrationTests.TestBase
{
    /// <summary>
    /// Quantifies the Cassandra version to determine whether a test should be run.
    /// </summary>
    public class TestCassandraVersion : Attribute
    {
        public int Major { get; set; }

        public int Minor { get; set; }

        public int Build { get; set; }

        public Comparison Comparison { get; set; }

        /// <summary>
        /// Creates the TestCassandraVersion object
        /// </summary>
        /// <param name="major">Major version</param>
        /// <param name="minor">Minor version</param>
        /// <param name="comparison">
        /// Determines if the Cassandra version required should be "greater or equals to" = 1, "equals to" = 0,
        /// "less than or equal to " = -1
        /// </param>
        public TestCassandraVersion(int major, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
        {
            Major = major;
            Minor = minor;
            Build = 0;
            Comparison = comparison;
        }

        /// <summary>
        /// Creates the TestCassandraVersion object with the option of specifying a Build
        /// </summary>
        /// <param name="major">Major version</param>
        /// <param name="minor">Minor version</param>
        /// <param name="build">Build number</param>
        /// <param name="comparison">
        /// Determines if the Cassandra version required should be "greater or equals to" = 1, "equals to" = 0,
        /// "less than or equal to " = -1
        /// </param>
        public TestCassandraVersion(int major, int minor, int build,
                                    Comparison comparison = Comparison.GreaterThanOrEqualsTo) : this(major, minor,
            comparison)
        {
            Build = build;
        }
    }

    public enum Comparison
    {
        LessThan = -1, 
        Equal = 0, 
        GreaterThanOrEqualsTo = 1
    };
}
