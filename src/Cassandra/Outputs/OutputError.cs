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

namespace Cassandra
{
    internal abstract class OutputError : IOutput, IWaitableForDispose
    {
        // Cache of factory methods for creating instances of OutputError, indexed by the error code
        private static readonly Func<OutputError>[] OutputErrorFactoryMethods;
 
        static OutputError()
        {
            // Use 0x2500 as the array size since that's currently the highest error code (a Dictionary lookup would be slower, but
            // more memory efficient if that becomes an issue)
            OutputErrorFactoryMethods = new Func<OutputError>[0x2500];

            // Add factory methods for all known error codes
            OutputErrorFactoryMethods[0x0000] = () => new OutputServerError();
            OutputErrorFactoryMethods[0x000A] = () => new OutputProtocolError();
            OutputErrorFactoryMethods[0x0100] = () => new OutputBadCredentials();
            OutputErrorFactoryMethods[0x1000] = () => new OutputUnavailableException();
            OutputErrorFactoryMethods[0x1001] = () => new OutputOverloaded();
            OutputErrorFactoryMethods[0x1002] = () => new OutputIsBootstrapping();
            OutputErrorFactoryMethods[0x1003] = () => new OutputTruncateError();
            OutputErrorFactoryMethods[0x1100] = () => new OutputWriteTimeout();
            OutputErrorFactoryMethods[0x1200] = () => new OutputReadTimeout();
            OutputErrorFactoryMethods[0x2000] = () => new OutputSyntaxError();
            OutputErrorFactoryMethods[0x2100] = () => new OutputUnauthorized();
            OutputErrorFactoryMethods[0x2200] = () => new OutputInvalid();
            OutputErrorFactoryMethods[0x2300] = () => new OutputConfigError();
            OutputErrorFactoryMethods[0x2400] = () => new OutputAlreadyExists();
            OutputErrorFactoryMethods[0x2500] = () => new OutputUnprepared();
        }

        protected string Message { get; private set; }

        protected OutputError()
        {
            Message = "";
        }

        public void Dispose()
        {
        }

        public void WaitForDispose()
        {
        }

        public abstract DriverException CreateException();

        protected abstract void Load(BEBinaryReader reader);

        internal static OutputError CreateOutputError(int code, string message, BEBinaryReader cb)
        {
            var factoryMethod = OutputErrorFactoryMethods[code];
            if (factoryMethod == null)
                throw new DriverInternalError("unknown error" + code);

            OutputError error = factoryMethod();
            error.Message = message;
            error.Load(cb);
            return error;
        }
    }
}