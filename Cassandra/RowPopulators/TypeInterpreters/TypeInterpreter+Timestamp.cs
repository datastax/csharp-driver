//
//      Copyright (C) 2012 DataStax Inc.
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
﻿using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromTimestamp(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (cSharpType == null || cSharpType.Equals(typeof(DateTimeOffset)))
                return BytesToDateTimeOffset(value, 0);
            else
                return BytesToDateTimeOffset(value, 0).DateTime;
        }

        public static Type GetDefaultTypeFromTimestamp(IColumnInfo type_info)
        {
            return typeof(DateTimeOffset);
        }

        public static byte[] InvConvertFromTimestamp(IColumnInfo type_info, object value)
        {
            CheckArgument<DateTimeOffset, DateTime>(value);
            if(value is DateTimeOffset)
                return DateTimeOffsetToBytes((DateTimeOffset)value);
            else
            {
                var dt = (DateTime)value;
                // need to treat "Unspecified" as UTC (+0) not the default behavior of DateTimeOffset which treats as Local Timezone
                // because we are about to do math against EPOCH which must align with UTC. 
                // If we don't, then the value saved will be shifted by the local timezone when retrieved back out as DateTime.
                return DateTimeOffsetToBytes(dt.Kind == DateTimeKind.Unspecified 
                    ? new DateTimeOffset(dt,TimeSpan.Zero) 
                    : new DateTimeOffset(dt));
            }
        }
    }
}
