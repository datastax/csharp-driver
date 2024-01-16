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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra
{
    public interface IColumnEncryptionPolicy
    {
        /// <summary>
        /// Encrypt the specified bytes using the cryptography materials for the specified column.
        /// Largely used internally, although this could also be used to encrypt values supplied
        /// to non-prepared statements in a way that is consistent with this policy.
        /// </summary>
        /// <returns></returns>
        byte[] Encrypt(string ks, string table, string col, byte[] objBytes);

        /// <summary>
        /// Decrypt the specified (encrypted) bytes using the cryptography materials for the
        /// specified column.  Used internally; could be used externally as well but there's
        /// not currently an obvious use case.
        /// </summary>
        byte[] Decrypt(string ks, string table, string col, byte[] encryptedBytes);

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="ks"></param>
        /// <param name="table"></param>
        /// <param name="col"></param>
        /// <returns></returns>
        Tuple<ColumnTypeCode, IColumnInfo> GetColumn(string ks, string table, string col);
    }
}
