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
using Cassandra.IntegrationTests.SimulacronAPI.Then;
using Cassandra.IntegrationTests.SimulacronAPI.When;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;

namespace Cassandra.IntegrationTests.SimulacronAPI
{
    public class PrimeRequestFluent : IWhenFluent, IThenFluent, IPrimeRequestFluent
    {
        private readonly SimulacronBase _simulacron;
        private IThen _then;
        private IWhen _when;

        public PrimeRequestFluent(SimulacronBase simulacron)
        {
            _simulacron = simulacron;
        }

        public IThenFluent ThenVoidSuccess()
        {
            _then = new ThenVoidSuccess();
            return this;
        }

        public IThenFluent ThenRowsSuccess((string, string)[] columnNamesToTypes, Action<IRowsResult> rowsBuilder)
        {
            var rows = new RowsResult(columnNamesToTypes);
            rowsBuilder(rows);
            _then = new ThenRowsSuccess(rows);
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
            return ThenServerError(ServerError.Unavailable, message, consistencyLevel, required, alive);
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

        private IThenFluent ThenServerError(ServerError error, string message, int cl, int required, int alive)
        {
            _then = new ThenConsistencyError(error, message, cl, required, alive);
            return this;
        }

        public IWhenFluent WhenQuery(string cql)
        {
            _when = new WhenQueryFluent(cql);
            return this;
        }

        public IWhenFluent WhenQuery(string cql, Action<IWhenQueryFluent> whenAction)
        {
            var when = new WhenQueryFluent(cql);
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

        public void Apply()
        {
            _simulacron.Prime(new
            {
                when = _when.Render(),
                then = _then.Render()
            });
        }
    }

    public interface IPrimeRequestFluent
    {
        IWhenFluent WhenQuery(string cql);

        IWhenFluent WhenQuery(string cql, Action<IWhenQueryFluent> whenAction);
    }

    public interface IThenFluent
    {
        IThenFluent WithDelayInMs(int delay);

        IThenFluent WithIgnoreOnPrepare(bool ignoreOnPrepare);

        void Apply();
    }

    public interface IWhenFluent
    {
        IThenFluent ThenVoidSuccess();

        IThenFluent ThenRowsSuccess((string, string)[] columnNamesToTypes, Action<IRowsResult> rowsBuilder);

        IThenFluent ThenAlreadyExists(string keyspace, string table);

        IThenFluent ThenReadTimeout(int consistencyLevel, int received, int blockFor, bool dataPresent);

        IThenFluent ThenUnavailable(string message, int consistencyLevel, int required, int alive);

        IThenFluent ThenWriteTimeout(string message, int consistencyLevel, int received, int blockFor, string writeType);

        IThenFluent ThenSyntaxError(string message);

        IThenFluent ThenWriteFailure(
            int consistencyLevel,
            int received,
            int blockFor,
            string message,
            IDictionary<string, int> failureReasons,
            string writeType);

        IThenFluent ThenReadFailure(
            int consistencyLevel, 
            int received, 
            int blockFor, 
            string message, 
            IDictionary<string, int> failureReasons, 
            bool dataPresent);

        IThenFluent ThenFunctionFailure(string keyspace, string function, string[] argTypes, string detail);
    }
}