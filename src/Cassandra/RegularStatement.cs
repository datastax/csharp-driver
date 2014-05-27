//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra
{
    /// <summary>
    ///  A non-prepared CQL statement.
    ///  This class represents a query string along with query options. This class can be extended but
    ///  <see cref="SimpleStatement"/> is provided to build a <see cref="IStatement"/>
    ///  directly from its query string.
    /// </summary>
    public abstract class RegularStatement : Statement
    {
        /// <summary>
        ///  Gets the query string for this statement.
        /// </summary>
        public abstract string QueryString { get; }

        protected RegularStatement(QueryProtocolOptions queryProtocolOptions) : base(queryProtocolOptions)
        {

        }

        public override string ToString()
        {
            return QueryString;
        }
    }
}