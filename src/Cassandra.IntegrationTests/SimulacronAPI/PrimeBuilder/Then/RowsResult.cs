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

namespace Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then
{
    public class RowsResult : IRowsResult
    {
        private readonly string[] _columnNames;
        private (string, string)[] _columnNamesToTypes;
        private readonly List<(string, object)[]> _rows = new List<(string, object)[]>();
        
        public RowsResult(params string[] columnNames)
        {
            _columnNames = columnNames;
        }

        public RowsResult(params (string, DataType)[] columnNamesToTypes)
        {
            _columnNamesToTypes = columnNamesToTypes.Select(tuple => (tuple.Item1, tuple.Item2.Value)).ToArray();
        }

        public IRowsResult WithRows(params object[][] columnNamesToValues)
        {
            var result = this as IRowsResult;

            return columnNamesToValues.Aggregate(result, (current, elem) => current.WithRow(elem));
        }

        public IRowsResult WithRow(params object[] values)
        {
            if (_columnNamesToTypes == null || _columnNamesToTypes.Length == 0)
            {
                _columnNamesToTypes = _columnNames.Zip(values, (name, val) => (name, DataType.GetDataType(val).Value)).ToArray();
            }

            if (values.Length != _columnNamesToTypes.Length)
            {
                throw new ArgumentException("Number of values don't match columns.");
            }

            _rows.Add(_columnNamesToTypes.Zip(values, (val1, val2) => (val1.Item1, AdaptValue(val2))).ToArray());
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
        
        private object AdaptValue(object value)
        {
            if (value is DateTimeOffset dateTimeOffset)
            {
                return DataType.GetTimestamp(dateTimeOffset);
            }

            return value;
        }
    }
}