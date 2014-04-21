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

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Cassandra
{
    public class RowSet : IDisposable
    {
        /// <summary>
        /// Gets or set the internal row list. It contains the rows of the latest query page.
        /// </summary>
        protected virtual List<Row> RowList { get; set; }

        /// <summary>
        /// Gets the execution info of the query
        /// </summary>
        public ExecutionInfo Info { get; set; }

        /// <summary>
        /// Gets or sets the columns in the rowset
        /// </summary>
        public CqlColumn[] Columns { get; set; }

        /// <summary>
        /// Gets or sets the paging state of the query for the rowset
        /// </summary>
        public byte[] PagingState { get; set; }

        /// <summary>
        /// Determines if all the rows from the previous query have been retrieved
        /// </summary>
        public bool IsExhausted
        {
            get
            {
                if (RowList.Count == 0 || PagingState == null)
                {
                    return true;
                }
                return false;
            }
        }

        public RowSet()
        {
            RowList = new List<Row>();
            Info = new ExecutionInfo();
        }

        /// <summary>
        /// Adds a row to the inner row list
        /// </summary>
        internal virtual void AddRow(Row row)
        {
            RowList.Add(row);
        }

        public void Dispose()
        {

        }

        public IEnumerable<Row> GetRows()
        {
            return RowList;
        }
    }
}