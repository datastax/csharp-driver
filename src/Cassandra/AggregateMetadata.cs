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
        public string Name { get; private set; }

        /// <summary>
        /// Name of the keyspace where the cql aggregate is declared.
        /// </summary>
        public string KeyspaceName { get; private set; }

        /// <summary>
        /// Signature of the function.
        /// </summary>
        public string[] Signature { get; private set; }

        /// <summary>
        /// List of the function argument names.
        /// </summary>
        public string[] ArgumentNames { get; private set; }

        /// <summary>
        /// List of the function argument types.
        /// </summary>
        public ColumnDesc[] ArgumentTypes { get; private set; }

        /// <summary>
        /// State Function.
        /// </summary>
        public string StateFunction { get; private set; }

        /// <summary>
        /// State type.
        /// </summary>
        public ColumnDesc StateType { get; private set; }

        /// <summary>
        /// Final function.
        /// </summary>
        public string FinalFunction { get; private set; }

        /// <summary>
        /// Initial state value of this aggregate.
        /// </summary>
        public object InitialCondition { get; set; }

        /// <summary>
        /// Type of the return value.
        /// </summary>
        public ColumnDesc ReturnType { get; private set; }

        public AggregateMetadata()
        {
            
        }

        public AggregateMetadata(string name, string keyspaceName, string[] signature, string[] argumentNames, ColumnDesc[] argumentTypes, 
                                 string stateFunction, ColumnDesc stateType, string finalFunction, object initialCondition, ColumnDesc returnType)
        {
            Name = name;
            KeyspaceName = keyspaceName;
            Signature = signature;
            ArgumentNames = argumentNames;
            ArgumentTypes = argumentTypes;
            StateFunction = stateFunction;
            StateType = stateType;
            FinalFunction = finalFunction;
            InitialCondition = initialCondition;
            ReturnType = returnType;
        }
    }
}
