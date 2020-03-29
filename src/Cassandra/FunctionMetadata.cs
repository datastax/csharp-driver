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

using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Describes a CQL function.
    /// </summary>
    public class FunctionMetadata
    {
        private static readonly string[] EmptyStringArray = new string[0];

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
        /// Indicates whether or not this function is deterministic. This means that given a particular input,
        /// the function will always produce the same output.
        /// </summary>
        public bool Deterministic { get; internal set; }

        /// <summary>
        /// Indicates whether or not this function is monotonic on all of its arguments. This means that it is
        /// either entirely non-increasing or non-decreasing. Even if the function is not monotonic on
        /// all of its arguments, it's possible to specify that it is monotonic on one of its arguments, meaning
        /// that partial applications of the function over that argument will be monotonic.
        /// </summary>
        /// <remarks>Monotonicity is required to use the function in a GROUP BY clause.</remarks>
        public bool Monotonic { get; internal set; }

        /// <summary>
        /// The argument names that the function is monotonic on.
        /// <para>
        /// If <see cref="Monotonic"/> is true, this will return all argument names.
        /// Otherwise, this will return either one argument or an empty collection.
        /// </para>
        /// </summary>
        public ICollection<string> MonotonicOn { get; internal set; } = EmptyStringArray;

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
