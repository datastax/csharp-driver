﻿//
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

namespace Cassandra.Data.Linq
{
    public static class SessionExtensions
    {
        public static Table<TEntity> GetTable<TEntity>(this Session @this, string tableName = null, string keyspaceName = null) where TEntity : class
        {
            return new Table<TEntity>(@this, Table<TEntity>.CalculateName(tableName), keyspaceName);
        }

        public static Batch CreateBatch(this Session @this)
        {
            if (@this == null || @this.BinaryProtocolVersion > 1)
                return new BatchV2(@this);
            return new BatchV1(@this);
        }
    }
}