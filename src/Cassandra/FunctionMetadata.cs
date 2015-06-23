using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Describes a CQL function.
    /// </summary>
    public class FunctionMetadata
    {
        /// <summary>
        /// Name of the CQL function.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Name of the keyspace where the CQL function is declared.
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
        /// Body of the function.
        /// </summary>
        public string Body { get; private set; }

        /// <summary>
        /// Determines if the function is called when the input is null.
        /// </summary>
        public bool CalledOnNullInput { get; private set; }

        /// <summary>
        /// Name of the programming language, for example: java, javascript, ...
        /// </summary>
        public string Language { get; private set; }

        /// <summary>
        /// Type of the return value.
        /// </summary>
        public ColumnDesc ReturnType { get; private set; }

        /// <summary>
        /// Creates a new instance of Function metadata.
        /// </summary>
        public FunctionMetadata()
        {
            
        }

        /// <summary>
        /// Creates a new instance of Function metadata.
        /// </summary>
        public FunctionMetadata(string name, string keyspaceName, string[] signature, string[] argumentNames, ColumnDesc[] argumentTypes, 
                                string body, bool calledOnNullInput, string language, ColumnDesc returnType)
        {
            Name = name;
            KeyspaceName = keyspaceName;
            Signature = signature;
            ArgumentNames = argumentNames;
            ArgumentTypes = argumentTypes;
            Body = body;
            CalledOnNullInput = calledOnNullInput;
            Language = language;
            ReturnType = returnType;
        }

        /// <summary>
        /// Creates a new instance of function metadata based on a schema_function row.
        /// </summary>
        internal static FunctionMetadata Build(Row row)
        {
            var emptyArray = new string[0];
            return new FunctionMetadata
            {
                Name = row.GetValue<string>("function_name"),
                KeyspaceName = row.GetValue<string>("keyspace_name"),
                Signature = row.GetValue<string[]>("signature") ?? emptyArray,
                ArgumentNames = row.GetValue<string[]>("argument_names") ?? emptyArray,
                Body = row.GetValue<string>("body"),
                CalledOnNullInput = row.GetValue<bool>("called_on_null_input"),
                Language = row.GetValue<string>("language"),
                ReturnType = TypeCodec.ParseDataType(row.GetValue<string>("return_type")),
                ArgumentTypes = (row.GetValue<string[]>("argument_types") ?? emptyArray).Select(s => TypeCodec.ParseDataType(s)).ToArray()
            };
        }
    }
}
