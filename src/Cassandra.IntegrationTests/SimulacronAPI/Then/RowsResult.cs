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

using System;
using System.Collections.Generic;
using System.Linq;

namespace Cassandra.IntegrationTests.SimulacronAPI.Then
{
    public class RowsResult : IRowsResult
    {
        private readonly (string, string)[] _columnNamesToTypes;
        private readonly List<(string, object)[]> _rows;

        public RowsResult(params (string, string)[] columnNamesToTypes)
        {
            _columnNamesToTypes = columnNamesToTypes;
            _rows = new List<(string, object)[]>();
        }

        public IRowsResult WithRows(params string[][] columnNamesToValues)
        {
            var result = this as IRowsResult;

            return columnNamesToValues.Aggregate(result, (current, elem) => current.WithRow(elem));
        }

        public IRowsResult WithRow(params object[] values)
        {
            if (values.Length != _columnNamesToTypes.Length)
            {
                throw new ArgumentException("Number of values don't match columns.");
            }

            _rows.Add(_columnNamesToTypes.Zip(values, (val1, val2) => (val1.Item1, val2)).ToArray());
            return this;
        }

        public object RenderRows()
        {
            return _rows.Select(row => row.ToDictionary(tuple => tuple.Item1, tuple => tuple.Item2));
        }

        public object RenderColumnTypes()
        {
            return _columnNamesToTypes.ToDictionary(tuple => tuple.Item1, tuple => tuple.Item2);
        }
    }
}