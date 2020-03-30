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

namespace Cassandra
{
    /// <summary>
    /// Describes a CQL aggregate.
    /// </summary>
    public class AggregateMetadata
    {
        /// <summary>
        /// Name of the CQL aggregate.
        /// </summary>
        public string Name { get; internal set; }

        /// <summary>
        /// Name of the keyspace where the cql aggregate is declared.
        /// </summary>
        public string KeyspaceName { get; internal set; }

        /// <summary>
        /// Signature of the function.
        /// </summary>
        public string[] Signature { get; internal set; }

        /// <summary>
        /// List of the function argument types.
        /// </summary>
        public ColumnDesc[] ArgumentTypes { get; internal set; }

        /// <summary>
        /// State Function.
        /// </summary>
        public string StateFunction { get; internal set; }

        /// <summary>
        /// State type.
        /// </summary>
        public ColumnDesc StateType { get; internal set; }

        /// <summary>
        /// Final function.
        /// </summary>
        public string FinalFunction { get; internal set; }

        /// <summary>
        /// Initial state value of this aggregate.
        /// </summary>
        public string InitialCondition { get; internal set; }

        /// <summary>
        /// Type of the return value.
        /// </summary>
        public ColumnDesc ReturnType { get; internal set; }

        /// <summary>
        /// Indicates whether or not this aggregate is deterministic. This means that given a particular input,
        /// the aggregate will always produce the same output.
        /// </summary>
        public bool Deterministic { get; internal set; }

        public AggregateMetadata()
        {
            
        }

        public AggregateMetadata(string name, string keyspaceName, string[] signature, ColumnDesc[] argumentTypes, 
                                 string stateFunction, ColumnDesc stateType, string finalFunction,
                                 string initialCondition, ColumnDesc returnType)
        {
            Name = name;
            KeyspaceName = keyspaceName;
            Signature = signature;
            ArgumentTypes = argumentTypes;
            StateFunction = stateFunction;
            StateType = stateType;
            FinalFunction = finalFunction;
            InitialCondition = initialCondition;
            ReturnType = returnType;
        }
    }
}
