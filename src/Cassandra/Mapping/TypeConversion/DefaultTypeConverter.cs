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

namespace Cassandra.Mapping.TypeConversion
{
    /// <summary>
    /// A default implementation of TypeConversionFactory that doesn't do any user defined conversions.
    /// </summary>
    public class DefaultTypeConverter : TypeConverter
    {
        /// <summary>
        /// Always returns null.
        /// </summary>
        protected override Func<TDatabase, TPoco> GetUserDefinedFromDbConverter<TDatabase, TPoco>()
        {
            return null;
        }

        /// <summary>
        /// Always returns null.
        /// </summary>
        protected override Func<TPoco, TDatabase> GetUserDefinedToDbConverter<TPoco, TDatabase>()
        {
            return null;
        }
    }
}