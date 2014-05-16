using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    /// It determines minimum version Cassandra required to execute
    /// </summary>
    public class TestCassandraVersion : Attribute
    {
        public int Major { get; set; }

        public int Minor { get; set; }

        /// <summary>
        /// It determines minimum version Cassandra required to execute
        /// </summary>
        public TestCassandraVersion(int mayor, int minor)
        {
            this.Major = mayor;
            this.Minor = minor;
        }
    }
}
