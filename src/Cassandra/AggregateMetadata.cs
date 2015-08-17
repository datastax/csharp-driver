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

        public AggregateMetadata(string name, string keyspaceName, string[] signature, ColumnDesc[] argumentTypes, 
                                 string stateFunction, ColumnDesc stateType, string finalFunction, object initialCondition, ColumnDesc returnType)
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

        /// <summary>
        /// Creates a new instance of function metadata based on a schema_function row.
        /// </summary>
        internal static AggregateMetadata Build(int protocolVersion, Row row)
        {
            var emptyArray = new string[0];
            var aggregate = new AggregateMetadata
            {
                Name = row.GetValue<string>("aggregate_name"),
                KeyspaceName = row.GetValue<string>("keyspace_name"),
                Signature = row.GetValue<string[]>("signature") ?? emptyArray,
                StateFunction = row.GetValue<string>("state_func"),
                StateType = TypeCodec.ParseDataType(row.GetValue<string>("state_type")),
                FinalFunction = row.GetValue<string>("final_func"),
                ReturnType = TypeCodec.ParseDataType(row.GetValue<string>("return_type")),
                ArgumentTypes = (row.GetValue<string[]>("argument_types") ?? emptyArray).Select(s => TypeCodec.ParseDataType(s)).ToArray(),
            };
            aggregate.InitialCondition = TypeCodec.Decode(protocolVersion, row.GetValue<byte[]>("initcond"), aggregate.StateType.TypeCode, aggregate.StateType.TypeInfo);
            return aggregate;
        }
    }
}
