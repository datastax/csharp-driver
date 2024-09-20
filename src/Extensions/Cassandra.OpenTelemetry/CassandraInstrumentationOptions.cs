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

using System.Diagnostics;

namespace Cassandra.OpenTelemetry
{
    /// <summary>
    /// Instrumentation options that can be used in <see cref="OpenTelemetryRequestTracker"/>.
    /// </summary>
    public class CassandraInstrumentationOptions
    {
        /// <summary>
        /// Default value of <see cref="BatchChildStatementLimit"/>.
        /// </summary>
        public const int DefaultBatchChildStatementLimit = 5;

        /// <summary>
        /// If true, includes the statement as a tag of the <see cref="Activity"/>.
        /// Default: false.
        /// </summary>
        public bool IncludeDatabaseStatement { get; set; } = false;

        /// <summary>
        /// <para>
        /// The max number of statements from a <see cref="BatchStatement"/> that are added to the Database Statement tag.
        /// The default is defined in <see cref="DefaultBatchChildStatementLimit"/>.
        /// </para>
        /// <para>
        /// This only applies if <see cref="IncludeDatabaseStatement"/> is true.
        /// </para>
        /// </summary>
        public int BatchChildStatementLimit { get; set; } = DefaultBatchChildStatementLimit;
    }
}
