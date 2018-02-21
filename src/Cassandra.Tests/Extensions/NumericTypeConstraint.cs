// 
//       Copyright DataStax Inc.
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
// 

using NUnit.Framework.Constraints;

namespace Cassandra.Tests.Extensions
{
    /// <summary>
    /// A NUnit constraint designed to enforce strict equality (no coersion) on numeric values.
    /// </summary>
    public class NumericTypeConstraint<T> : Constraint where T: struct
    {
        private readonly T _expected;

        public override string Description => $"{_expected} ({_expected.GetType().Name})";

        public NumericTypeConstraint(T expected)
        {
            _expected = expected;
        }
        
        public override ConstraintResult ApplyTo<TActual>(TActual actual)
        {
            var areEqual = _expected.Equals(actual);
            return new ConstraintResult(this, actual, areEqual);
        }
    }

    public static class NumericTypeConstraint
    {
        public static NumericTypeConstraint<T> Create<T>(T instance) where T : struct
        {
            return new NumericTypeConstraint<T>(instance);
        }
    }
}