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
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.When;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder
{
    public class PrimeRequestBuilder : IWhenFluent, IThenFluent, IPrimeRequestBuilder
    {
        private IThen _then;
        private IWhen _when;

        public IThenFluent ThenVoid()
        {
            _then = new ThenVoid();
            return this;
        }

        public IThenFluent ThenVoidSuccess()
        {
            _then = new ThenVoidSuccess();
            return this;
        }

        public IThenFluent ThenRowsSuccess((string, DataType)[] columnNamesToTypes, Action<IRowsResult> rowsBuilder)
        {
            var rows = new RowsResult(columnNamesToTypes);
            rowsBuilder(rows);
            _then = new ThenRowsSuccess(rows);
            return this;
        }
        
        public IThenFluent ThenRowsSuccess(string[] columnNames, Action<IRowsResult> rowsBuilder)
        {
            var rows = new RowsResult(columnNames);
            rowsBuilder(rows);
            _then = new ThenRowsSuccess(rows);
            return this;
        }

        public IThenFluent ThenRowsSuccess(params (string, DataType)[] columnNamesToTypes)
        {
            var rows = new RowsResult(columnNamesToTypes);
            _then = new ThenRowsSuccess(rows);
            return this;
        }

        public IThenFluent ThenRowsSuccess(RowsResult result)
        {
            _then = new ThenRowsSuccess(result);
            return this;
        }

        public IThenFluent ThenAlreadyExists(string keyspace, string table)
        {
            _then = new ThenAlreadyExists(keyspace, table);
            return this;
        }

        public IThenFluent ThenReadTimeout(int consistencyLevel, int received, int blockFor, bool dataPresent)
        {
            _then = new ThenReadTimeout(consistencyLevel, received, blockFor, dataPresent);
            return this;
        }

        public IThenFluent ThenUnavailable(string message, int consistencyLevel, int required, int alive)
        {
            _then = new ThenUnavailableError(message, consistencyLevel, required, alive);
            return this;
        }

        public IThenFluent ThenWriteTimeout(string message, int consistencyLevel, int received, int blockFor, string writeType)
        {
            _then = new ThenWriteTimeout(message, consistencyLevel, received, blockFor, writeType);
            return this;
        }

        public IThenFluent ThenSyntaxError(string message)
        {
            _then = new ThenSyntaxError(message);
            return this;
        }

        public IThenFluent ThenWriteFailure(
            int consistencyLevel, int received, int blockFor, string message, IDictionary<string, int> failureReasons, string writeType)
        {
            _then = new ThenWriteFailure(consistencyLevel, received, blockFor, message, failureReasons, writeType);
            return this;
        }

        public IThenFluent ThenReadFailure(int consistencyLevel, int received, int blockFor, string message, IDictionary<string, int> failureReasons, bool dataPresent)
        {
            _then = new ThenReadFailure(consistencyLevel, received, blockFor, message, failureReasons, dataPresent);
            return this;
        }

        public IThenFluent ThenFunctionFailure(string keyspace, string function, string[] argTypes, string detail)
        {
            _then = new ThenFunctionFailure(keyspace, function, argTypes, detail);
            return this;
        }

        public IThenFluent ThenIsBootstrapping()
        {
            _then = new ThenIsBootstrapping();
            return this;
        }

        public IThenFluent ThenOverloaded(string testOverloadedError)
        {
            _then = new ThenServerError(ServerError.Overloaded, testOverloadedError);
            return this;
        }

        public IThenFluent ThenServerError(ServerError resultError, string message)
        {
            _then = new ThenServerError(resultError, message);
            return this;
        }

        public IWhenFluent WhenBatch(Action<IWhenBatchBuilder> whenAction)
        {
            var when = new WhenBatchBuilder();
            whenAction(when);
            _when = when;
            return this;
        }

        public IWhenFluent WhenQuery(string cql)
        {
            _when = new WhenQueryBuilder(cql);
            return this;
        }

        public IWhenFluent WhenQuery(string cql, Action<IWhenQueryBuilder> whenAction)
        {
            var when = new WhenQueryBuilder(cql);
            whenAction(when);
            _when = when;
            return this;
        }

        public IThenFluent WithDelayInMs(int delay)
        {
            _then.SetDelayInMs(delay);
            return this;
        }

        public IThenFluent WithIgnoreOnPrepare(bool ignoreOnPrepare)
        {
            _then.SetIgnoreOnPrepare(ignoreOnPrepare);
            return this;
        }

        public IPrimeRequest BuildRequest()
        {
            return new PrimeRequest(_when, _then);
        }

        public Task<JObject> ApplyAsync(SimulacronBase simulacron)
        {
            return simulacron.PrimeAsync(BuildRequest());
        }
    }
}