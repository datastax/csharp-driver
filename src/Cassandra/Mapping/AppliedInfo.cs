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

using System.Linq;

namespace Cassandra.Mapping
{
    /// <summary>
    /// When using Lightweight transactions, it provides information whether the change was applied or not.
    /// </summary>
    public class AppliedInfo<T>
    {
        /// <summary>
        /// Determines if the change was applied.
        /// </summary>
        public bool Applied { get; set; }

        /// <summary>
        /// Gets or sets the existing data that prevented
        /// </summary>
        public T Existing { get; set; }

        /// <summary>
        /// Creates a new instance marking the change as applied 
        /// </summary>
        public AppliedInfo(bool applied)
        {
            Applied = applied;
        }

        /// <summary>
        /// Creates a new instance marking the change as not applied and provides information about the existing data.
        /// </summary>
        /// <param name="existing"></param>
        public AppliedInfo(T existing)
        {
            Applied = false;
            Existing = existing;
        }

        /// <summary>
        /// Adapts a LWT RowSet and returns a new AppliedInfo
        /// </summary>
        internal static AppliedInfo<T> FromRowSet(MapperFactory mapperFactory, string cql, RowSet rs)
        {
            var row = rs.FirstOrDefault();
            const string appliedColumn = "[applied]";
            if (row == null || row.GetColumn(appliedColumn) == null || row.GetValue<bool>(appliedColumn))
            {
                //The change was applied correctly
                return new AppliedInfo<T>(true);
            }
            if (rs.Columns.Length == 1)
            {
                //There isn't more information on why it was not applied
                return new AppliedInfo<T>(false);
            }
            //It was not applied, map the information returned
            var mapper = mapperFactory.GetMapper<T>(cql, rs);
            return new AppliedInfo<T>(mapper(row));
        }
    }
}
