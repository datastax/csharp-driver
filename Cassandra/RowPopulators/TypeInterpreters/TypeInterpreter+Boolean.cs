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
        public static object ConvertFromBoolean(IColumnInfo type_info, byte[] _buffer, Type cSharpType)
        {
            return _buffer[0] == 1;
        }

        public static Type GetDefaultTypeFromBoolean(IColumnInfo type_info)
        {
            return typeof(bool);
        }

        public static byte[] InvConvertFromBoolean(IColumnInfo type_info, object value)
        {
            CheckArgument<bool>(value);
            var buffer = new byte[1];
            buffer[0] = ((bool)value) ? (byte)0x01 : (byte)0x00;
            return buffer;
        }
    }
}
