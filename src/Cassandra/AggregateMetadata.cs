using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

        public AggregateMetadata()
        {
            
        }

        public AggregateMetadata(string name, string keyspaceName, string[] signature, ColumnDesc[] argumentTypes, 
                                 string stateFunction, ColumnDesc stateType, string finalFunction, string initialCondition, ColumnDesc returnType)
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
