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
using System.Collections.Generic;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;

namespace Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder
{
    public interface IWhenFluent
    {
        IThenFluent ThenVoid();

        IThenFluent ThenVoidSuccess();

        IThenFluent ThenRowsSuccess((string, DataType)[] columnNamesToTypes, Action<IRowsResult> rowsBuilder);

        IThenFluent ThenRowsSuccess(params (string, DataType)[] columnNamesToTypes);

        IThenFluent ThenRowsSuccess(string[] columnNames, Action<IRowsResult> rowsBuilder);

        IThenFluent ThenRowsSuccess(RowsResult result);

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

        IThenFluent ThenIsBootstrapping();

        IThenFluent ThenOverloaded(string testOverloadedError);

        IThenFluent ThenServerError(ServerError resultError, string message);
    }
}