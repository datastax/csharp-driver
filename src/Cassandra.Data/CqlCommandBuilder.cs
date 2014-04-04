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
using System.Data;
using System.Data.Common;

namespace Cassandra.Data
{
    internal class CqlCommandBuilder : DbCommandBuilder
    {
        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
        {
            throw new NotSupportedException();
        }

        protected override string GetParameterName(string parameterName)
        {
            throw new NotSupportedException();
        }

        protected override string GetParameterName(int parameterOrdinal)
        {
            throw new NotSupportedException();
        }

        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            throw new NotSupportedException();
        }

        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            throw new NotSupportedException();
        }
    }
}