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

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Represents the different phases during the parsing a Linq expressions.
    /// </summary>
    internal enum ParsePhase
    {
        None,

        /// <summary>
        /// Select() method calls.
        /// </summary>
        Select,

        /// <summary>
        /// Where() method calls or LWT conditions.
        /// </summary>
        Condition,

        /// <summary>
        /// Lambda evaluation after Select()
        /// </summary>
        SelectBinding,

        /// <summary>
        /// Take() method calls.
        /// </summary>
        Take,
        
        /// <summary>
        /// OrderBy() method calls.
        /// </summary>
        OrderBy,

        /// <summary>
        /// OrderByDescending() method calls.
        /// </summary>
        OrderByDescending,
        
        /// <summary>
        /// GroupBy() method calls.
        /// </summary>
        GroupBy
    }
}