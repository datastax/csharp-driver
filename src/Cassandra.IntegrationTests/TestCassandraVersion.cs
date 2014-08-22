//
//      Copyright (C) 2012-2014 DataStax Inc.
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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    /// It determines the Cassandra version required to execute the test
    /// </summary>
    public class TestCassandraVersion : Attribute
    {
        public int Major { get; set; }

        public int Minor { get; set; }

        public Comparison Comparison { get; set; }

        /// <summary>
        /// It determines Cassandra version required to execute
        /// </summary>
        /// <param name="comparisonOperator">Determines if the Cassandra version required should be "greater or equals to" = 1, "equals to" = 0, "less than or equal to " = -1</param>
        public TestCassandraVersion(int mayor, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
        {
            this.Major = mayor;
            this.Minor = minor;
            this.Comparison = comparison;
        }
    }

    public enum Comparison
    {
        LessThan = -1, 
        Equal = 0, 
        GreaterThanOrEqualsTo = 1
    };
}
