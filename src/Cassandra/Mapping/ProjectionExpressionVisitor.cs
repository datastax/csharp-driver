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
using Cassandra.Mapping.TypeConversion;

namespace Cassandra.Mapping
{
    /// <summary>
    /// A Linq expression visitor that extracts the projection to allow to be reconstructed from a different origin.
    /// </summary>
    internal class ProjectionExpressionVisitor : ExpressionVisitor
    {
        private Expression _expression;
        public NewTypeProjection Projection { get; private set; }

        public override Expression Visit(Expression node)
        {
            _expression = node;
            return base.Visit(node);
        }

        protected override MemberAssignment VisitMemberAssignment(MemberAssignment node)
        {
            if (Projection == null)
            {
                throw new NotSupportedException("Projection expression not supported: " + _expression);
            }
            Projection.Members.Add(node.Member);
            return node;
        }

        protected override Expression VisitNew(NewExpression node)
        {
            Projection = new NewTypeProjection(node.Constructor);
            return node;
        }
    }
}
