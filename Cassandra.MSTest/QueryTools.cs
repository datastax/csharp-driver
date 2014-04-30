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
using System.Collections.Generic;
using System.Linq;
using System.Text;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif
using System.Text;
using System.Net;

namespace Cassandra.MSTest
{
    internal static class QueryTools
    {
        private static string CellEncoder(object col)
        {
            return col.ToString();
        }

        internal static void ExecuteSyncQuery(Session session, string query, List<object[]> expectedValues = null, ConsistencyLevel consistency = ConsistencyLevel.Default, string messageInstead = null)
        {
            if (messageInstead != null)
                Console.WriteLine("CQL<\t" + messageInstead);
            else
                Console.WriteLine("CQL< Query:\t" + query);

            if (expectedValues != null)
                using (var ret = session.Execute(query, consistency))
                    valueComparator(ret, expectedValues);

            using (var ret = session.Execute(query, consistency))
                ret.PrintTo(stream: Console.Out, cellEncoder: CellEncoder);

            Console.WriteLine("CQL> Done.");
        }

        internal static void valueComparator(RowSet rawrowset, List<object[]> insertedRows)
        {
            var rowset = rawrowset.GetRows().ToList();
            Assert.True(rowset.Count == insertedRows.Count, string.Format("Returned rows count is not equal with the count of rows that were inserted! \n Returned: {0} \n Expected: {1} \n", rowset.Count, insertedRows.Count));
            int i = 0;
            foreach (var row in rowset)
            {
                if (row.Any(col => col.GetType() == typeof(byte[])))
                    for (int j = 0; j < row.Length; j++)
                        Assert.True(row[j].GetType() == typeof(byte[]) ? Assert.ArrEqual((byte[])row[j], (byte[])insertedRows[i][j]) : row[j].Equals(insertedRows[i][j]));
                else
                {
                    for (int m = 0; m < row.Length; m++)
                    {
                        if (!row[m].Equals(insertedRows[i][m]))
                        {
                            insertedRows.Reverse();// To check if needed and why 
                            if (!row[m].Equals(insertedRows[i][m]))
                                insertedRows.Reverse();
                        }
                        Assert.True(row[m].Equals(insertedRows[i][m]), "Inserted data !Equals with returned data.");
                    }
                }
                i++;
            }
        }

        internal static IPAddress ExecuteSyncNonQuery(Session session, string query, string messageInstead = null, ConsistencyLevel consistency = ConsistencyLevel.Default)
        {
            if (messageInstead != null)
                Console.WriteLine("CQL<\t" + messageInstead);
            else
                Console.WriteLine("CQL< Query:\t" + query);
            var ret = session.Execute(query, consistency);
            Console.WriteLine("CQL> (OK).");
            return ret.Info.QueriedHost;
        }


        internal static PreparedStatement PrepareQuery(Session session, string query, string messageInstead = null)
        {
            if (messageInstead != null)
                Console.WriteLine("CQL<\t" + messageInstead);
            else
                Console.WriteLine("CQL< Prepared Query:\t" + query);
            var ret = session.Prepare(query);
            Console.WriteLine("CQL> (OK).");
            return ret;
        }

        internal static IPAddress ExecutePreparedQuery(Session session, PreparedStatement prepared, object[] values, ConsistencyLevel consistency = ConsistencyLevel.Default, string messageInstead = null)
        {
            if (messageInstead != null)
                Console.WriteLine("CQL<\t" + messageInstead);
            else
                Console.WriteLine("CQL< Executing Prepared Query:\t");
            var ret = session.Execute(prepared.Bind(values).SetConsistencyLevel(consistency));
            Console.WriteLine("CQL> (OK).");
            return ret.Info.QueriedHost;
        }

        internal static string convertTypeNameToCassandraEquivalent(Type t)
        {
            switch (t.Name)
            {
                case "Int32":
                    return "int";

                case "Int64":
                    return "bigint";

                case "Single":
                    return "float";

                case "Double":
                    return "double";

                case "Decimal":
                    return "decimal";

                case "BigInteger":
                    return "varint";

                case "Char":
                    return "ascii";

                case "string":
                case "String":
                    return "text";

                case "DateTimeOffset":
                    return "timestamp";

                case "Byte":
                    return "blob";

                case "Boolean":
                    return "boolean";

                case "Guid":
                    return "uuid";
                case "IPAddress":
                    return "inet";

                default:
                    throw new InvalidOperationException();
            }
        }


    }
}
