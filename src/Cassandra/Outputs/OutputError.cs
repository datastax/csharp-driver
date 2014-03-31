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

using System;
using System.Reflection;

namespace Cassandra
{
}

namespace Cassandra
{
    internal abstract class OutputError : IOutput, IWaitableForDispose
    {
        public CassandraErrorType CassandraErrorType = CassandraErrorType.Invalid;
        public string Message = "";

        public void Dispose()
        {
        }

        public void WaitForDispose()
        {
        }

        internal static OutputError CreateOutputError(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            Type tpy = Assembly.GetExecutingAssembly().GetType("Cassandra.Output" + code);
            if (tpy == null)
                throw new DriverInternalError("unknown error" + code);
            ConstructorInfo cnstr = tpy.GetConstructor(new Type[] {});
            var outp = (OutputError) cnstr.Invoke(new object[] {});
            tpy.GetField("CassandraErrorType").SetValue(outp, code);
            tpy.GetField("Message").SetValue(outp, message);
            MethodInfo loadM = tpy.GetMethod("Load", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance, null,
                                             new[] {typeof (CassandraErrorType), typeof (string), typeof (BEBinaryReader)}, null);
            if (loadM != null)
                loadM.Invoke(outp, new object[] {code, message, cb});
            return outp;
        }

        public abstract DriverException CreateException();
    }
}