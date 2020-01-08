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

namespace Dse.Test.Integration.SimulacronAPI.PrimeBuilder.Then
{
    public class ThenFunctionFailure : BaseThen
    {
        private readonly string _keyspace;
        private readonly string _function;
        private readonly string[] _argTypes;
        private readonly string _detail;

        public ThenFunctionFailure(string keyspace, string function, string[] argTypes, string detail)
        {
            this._keyspace = keyspace;
            _function = function;
            _argTypes = argTypes;
            _detail = detail;
        }

        public override object Render()
        {
            return new
            {
                result = "function_failure",
                keyspace = _keyspace,
                function = _function,
                arg_types = _argTypes,
                detail = _detail,
                delay_in_ms = DelayInMs,
                ignore_on_prepare = IgnoreOnPrepare
            };
        }
    }
}