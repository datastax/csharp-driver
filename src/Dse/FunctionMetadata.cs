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
        public string Name { get; internal set; }

        /// <summary>
        /// Name of the keyspace where the CQL function is declared.
        /// </summary>
        public string KeyspaceName { get; internal set; }

        /// <summary>
        /// Signature of the function.
        /// </summary>
        public string[] Signature { get; internal set; }

        /// <summary>
        /// List of the function argument names.
        /// </summary>
        public string[] ArgumentNames { get; internal set; }

        /// <summary>
        /// List of the function argument types.
        /// </summary>
        public ColumnDesc[] ArgumentTypes { get; internal set; }

        /// <summary>
        /// Body of the function.
        /// </summary>
        public string Body { get; internal set; }

        /// <summary>
        /// Determines if the function is called when the input is null.
        /// </summary>
        public bool CalledOnNullInput { get; internal set; }

        /// <summary>
        /// Name of the programming language, for example: java, javascript, ...
        /// </summary>
        public string Language { get; internal set; }

        /// <summary>
        /// Type of the return value.
        /// </summary>
        public ColumnDesc ReturnType { get; internal set; }

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
    }
}
