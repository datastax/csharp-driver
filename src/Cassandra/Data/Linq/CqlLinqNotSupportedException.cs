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

using System;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public class CqlLinqNotSupportedException : NotSupportedException
    {
        public Expression Expression { get; private set; }

        internal CqlLinqNotSupportedException(Expression expression, ParsePhase parsePhase)
            : base(string.Format("The expression {0} = [{1}] is not supported in {2} parse phase.",
                                 expression.NodeType, expression, parsePhase))
        {
            Expression = expression;
        }
    }
}