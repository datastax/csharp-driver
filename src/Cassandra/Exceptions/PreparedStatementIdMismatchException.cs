// 
//       Copyright (C) DataStax Inc.
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

using System;

namespace Cassandra
{
    /// <summary>
    /// <para>
    /// This exception is thrown when the driver attempts to re-prepare a statement
    /// and the returned prepared statement's ID is different than the one on the existing
    /// <see cref="PreparedStatement"/>.
    /// </para>
    /// <para>
    /// When this exception is thrown, it means that the <see cref="PreparedStatement"/>
    /// object with the ID that matches <see cref="Id"/> should not be used anymore.
    /// </para>
    /// </summary>
    public class PreparedStatementIdMismatchException : DriverException
    {
        public PreparedStatementIdMismatchException(byte[] originalId, byte[] outputId) 
            : base("ID mismatch while trying to reprepare (expected " 
                   + $"{BitConverter.ToString(originalId).Replace("-", "")}, " 
                   + $"got {BitConverter.ToString(outputId).Replace("-", "")}). " 
                   + "This prepared statement won't work anymore. " 
                   + "This usually happens when you run a 'USE...' query after " 
                   + "the statement was prepared.")
        {
            Id = originalId;
        }

        /// <summary>
        /// ID of the prepared statement that should not be used anymore.
        /// </summary>
        public byte[] Id { get; }
    }
}