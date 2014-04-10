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
    ///  Exception thrown when a query attemps to create a keyspace or table that
    ///  already exists.
    /// </summary>
    public class AlreadyExistsException : QueryValidationException
    {
        /// <summary>
        ///  Gets the name of keyspace that either already exists or is home to the table that
        ///  already exists.
        /// </summary>
        public string Keyspace { get; private set; }

        /// <summary>
        ///  If the failed creation was a table creation, gets the name of the table that already exists. 
        /// </summary>
        public string Table { get; private set; }

        /// <summary>
        ///  Gets whether the query yielding this exception was a table creation
        ///  attempt.
        /// </summary>
        public bool WasTableCreation
        {
            get { return !string.IsNullOrEmpty((Table)); }
        }

        public AlreadyExistsException(string keyspace, string table) :
            base(MakeMsg(keyspace, table))
        {
            Keyspace = string.IsNullOrEmpty(keyspace.Trim()) ? null : keyspace;
            Table = string.IsNullOrEmpty(table.Trim()) ? null : table;
        }

        private static string MakeMsg(string keyspace, string table)
        {
            if (string.IsNullOrEmpty(table))
                return string.Format("Keyspace {0} already exists", keyspace);
            return string.Format("Table {0}.{1} already exists", keyspace, table);
        }
    }
}