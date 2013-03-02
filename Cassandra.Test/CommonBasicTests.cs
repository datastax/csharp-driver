using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Net;
#if CASSANDRA_NET_40_OR_GREATER
using System.Numerics;
#endif
using System.Globalization;
using Cassandra;
using System.Threading.Tasks;
using System.Linq;
using Dev;


namespace MyUTExt
{

    public class CommonBasicTests :  IDisposable
    {
        bool _compression;
        CompressionType Compression
        {
            get
            {
                return _compression ? CompressionType.Snappy : CompressionType.NoCompression;
            }
        }

        string Keyspace = "tester";

        public CommonBasicTests(bool compression)
        {
            this._compression = compression;
        }

        Cluster Cluster;
        Session Session;

        public void SetFixture(Dev.SettingsFixture setFix)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US"); //"pl-PL");                       
            var clusterb = Cluster.Builder().AddContactPoint("cassi.cloudapp.net");
            clusterb.WithDefaultKeyspace(Keyspace);
            
            if (_compression)
                clusterb.WithCompression(CompressionType.Snappy);
            Cluster = clusterb.Build();
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Verbose;
            Diagnostics.CassandraStackTraceIncluded= true;
            Diagnostics.CassandraPerformanceCountersEnabled = true;
            Session = Cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(ReplicationStrategies.CreateSimpleStrategyReplicationProperty(2), true);            
        }

        public void Dispose()
        {
            Session.DeleteKeyspace(Keyspace);
            Session.Dispose();
            Cluster.Shutdown();
        }

        private static string CellEncoder(object col)
        {
            return col.ToString();
        }

        public void ExecuteSyncQuery(Session session, string query, List<object[]> expectedValues = null, ConsistencyLevel consistency = ConsistencyLevel.Default, string messageInstead = null)
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

        public static bool ArrEqual(byte[] a1, byte[] a2)
        {
            if (ReferenceEquals(a1, a2))
                return true;

            if (a1 == null || a2 == null)
                return false;

            if (a1.Length != a2.Length)
                return false;

            EqualityComparer<byte> comparer = EqualityComparer<byte>.Default;
            for (int i = 0; i < a1.Length; i++)
            {
                if (!comparer.Equals(a1[i], a2[i])) return false;
            }
            return true;
        }

        public void valueComparator(CqlRowSet rowset, List<object[]> insertedRows)
        {                
            Assert.True(rowset.RowsCount == insertedRows.Count, string.Format("Returned rows count is not equal with the count of rows that were inserted! \n Returned: {0} \n Expected: {1} \n", rowset.RowsCount, insertedRows.Count));            
            int i = 0;            
            foreach (var row in rowset.GetRows())
            {                
                if (row.Columns.Any(col => col.GetType() == typeof(byte[])))
                    for (int j = 0; j < row.Length; j++)
                        Assert.True(row[j].GetType() == typeof(byte[]) ? ArrEqual((byte[])row[j], (byte[])insertedRows[i][j]) : row[j].Equals(insertedRows[i][j]));                        
                else
                {
                    for (int m = 0; m < row.Columns.Length; m++)
                    {                     
                        if (!row.Columns[m].Equals(insertedRows[i][m]))
                        {
                            insertedRows.Reverse();// To check if needed and why 
                            if (!row.Columns[m].Equals(insertedRows[i][m]))
                                insertedRows.Reverse();
                        }
                        Assert.True(row.Columns[m].Equals(insertedRows[i][m]), "Inserted data !Equals with returned data.");
                    }
                }
                i++;
            }
        }


        public void ExecuteSyncNonQuery(Session session, string query, string messageInstead = null, ConsistencyLevel consistency= ConsistencyLevel.Default)
        {
            if (messageInstead != null)
                Console.WriteLine("CQL<\t" + messageInstead);
            else
                Console.WriteLine("CQL< Query:\t" + query);
            var ret = session.Execute(query, consistency);
            Console.WriteLine("CQL> (OK).");
        }


        public PreparedStatement PrepareQuery(Session session, string query, string messageInstead = null)
        {
            if (messageInstead != null)
                Console.WriteLine("CQL<\t" + messageInstead);
            else
                Console.WriteLine("CQL< Prepared Query:\t" + query);
            var ret = session.Prepare(query);
            Console.WriteLine("CQL> (OK).");
            return ret;
        }

        public void ExecutePreparedQuery(Session session, PreparedStatement prepared, object[] values, ConsistencyLevel consistency = ConsistencyLevel.Default, string messageInstead = null)
        {
            if (messageInstead != null)
                Console.WriteLine("CQL<\t" + messageInstead);
            else
                Console.WriteLine("CQL< Executing Prepared Query:\t");
            session.Execute(prepared.Bind(values).SetConsistencyLevel(consistency));
            Console.WriteLine("CQL> (OK).");
        }

        public void Test(int RowsNo=5000)
        {            
            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();

            ExecuteSyncNonQuery(Session, string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};"
                , keyspaceName));

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
		 fval float,
		 dval double,
         PRIMARY KEY(tweet_id))", tableName));
            }
            catch (AlreadyExistsException)
            {
            }
            
            StringBuilder longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            for (int i = 0; i < RowsNo; i++)
            {
                longQ.AppendFormat(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body,
		 fval,
		 dval)
VALUES ({1},'test{2}',{3},'body{2}',{4},{5});", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true", rndm.NextSingle(), rndm.NextDouble());
            }
            longQ.AppendLine("APPLY BATCH;");
            ExecuteSyncNonQuery(Session, longQ.ToString(), "Inserting...");
            ExecuteSyncQuery(Session, string.Format(@"SELECT * from {0};", tableName));
            ExecuteSyncNonQuery(Session, string.Format(@"DROP TABLE {0};", tableName));

            ExecuteSyncNonQuery(Session, string.Format(@"DROP KEYSPACE {0};", keyspaceName));
        }


        public void ExceedingCassandraType(Type toExceed, Type toExceedWith, bool sameOutput = true)
        {
            string cassandraDataTypeName = convertTypeNameToCassandraEquivalent(toExceed);
            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         label text,
         number {1}
         );", tableName, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }
            

            var Minimum = toExceedWith.GetField("MinValue").GetValue(this);
            var Maximum = toExceedWith.GetField("MaxValue").GetValue(this);


            object[] row1 = new object[3] { Guid.NewGuid(), "Minimum", Minimum };
            object[] row2 = new object[3] { Guid.NewGuid(), "Maximum", Maximum };
            List<object[]> toInsert_and_Check = new List<object[]>(2) { row1, row2 };

            if (toExceedWith == typeof(Double) || toExceedWith == typeof(Single))
            {
                Minimum = Minimum.GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(Minimum, new object[1] { "r" });
                Maximum = Maximum.GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(Maximum, new object[1] { "r" });

                if (!sameOutput) //for ExceedingCassandra_FLOAT() test case
                {
                    toInsert_and_Check[0][2] = Single.NegativeInfinity;
                    toInsert_and_Check[1][2] = Single.PositiveInfinity;
                }
            }

            try
            {
                ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName, toInsert_and_Check[0][0], toInsert_and_Check[0][1], Minimum), null);
                ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName, toInsert_and_Check[1][0].ToString(), toInsert_and_Check[1][1], Maximum), null);
            }
            catch (InvalidException) {
                if (!sameOutput && toExceed == typeof(Int32)) //for ExceedingCassandra_INT() test case
                {
                    ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
                    Assert.True(true);
                    return;
                }
            }
                        
            ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), toInsert_and_Check);
            ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }


        private string convertTypeNameToCassandraEquivalent(Type t)
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
                case "BigDecimal":
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
                case "IPEndPoint":
                    return "inet";

                default:
                    throw new InvalidOperationException();                    
            }
        }

        public void insertingSingleValuePrepared(Type tp)
        {
            string cassandraDataTypeName = convertTypeNameToCassandraEquivalent(tp);
            string tableName = "table" + Guid.NewGuid().ToString("N");
            
            ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         value {1}
         );", tableName, cassandraDataTypeName));

            List<object[]> toInsert = new List<object[]>(1);
            var val = RandomVal(tp);
            if (tp == typeof (string))
                val = "'" + val.ToString().Replace("'", "''") + "'";

            var row1 = new object[2] { Guid.NewGuid(), val };

            toInsert.Add(row1);

            var prep = PrepareQuery(this.Session, string.Format("INSERT INTO {0}(tweet_id, value) VALUES ({1}, ?);", tableName, toInsert[0][0].ToString()));
            ExecutePreparedQuery(this.Session, prep, new object[1] { toInsert[0][1] });

#if CASSANDRA_NET_40_OR_GREATER
            if (tp == typeof(Decimal))
                toInsert[0][1] = new BigDecimal((Decimal)toInsert[0][1]);
#endif
            ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), toInsert);
            ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void testCounters()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         incdec counter
         );", tableName));
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();                                    
            
            Parallel.For(0, 100, i =>
                {                     
                   ExecuteSyncNonQuery(Session, string.Format(@"UPDATE {0} SET incdec = incdec {2}  WHERE tweet_id = {1};", tableName, tweet_id, (i%2 == 0 ? "-":"+") + i));
                });

            ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), new List<object[]> { new object[2] { tweet_id, (Int64)50 } });
            ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void insertingSingleValue(Type tp)
        {
            string cassandraDataTypeName = convertTypeNameToCassandraEquivalent(tp);
            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         value {1}
         );", tableName, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }

            List<object[]> toInsert = new List<object[]>(1);
            var val = RandomVal(tp);
            if (tp == typeof(string))
                val = "'" + val.ToString().Replace("'", "''") + "'";
            object[] row1 = new object[2] { Guid.NewGuid(), val };
            toInsert.Add(row1);

            bool isFloatingPoint = false;

            if ( row1[1].GetType() == typeof(string) || row1[1].GetType() == typeof(byte[]))
                ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0].ToString(), row1[1].GetType() == typeof(byte[]) ? "0x" + Cassandra.CqlQueryTools.ToHex((byte[])toInsert[0][1]) : "'" + toInsert[0][1] + "'"), null); // rndm.GetType().GetMethod("Next" + tp.Name).Invoke(rndm, new object[] { })
            else
            {
                if (tp == typeof(Single) || tp == typeof(Double) 
#if CASSANDRA_NET_40_OR_GREATER
                    || tp == typeof(BigDecimal)
#endif
                    )
                    isFloatingPoint = true;
                ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0].ToString(), !isFloatingPoint ? toInsert[0][1] : toInsert[0][1].GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(toInsert[0][1], new object[] { "r" })), null);
            }

            ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), toInsert);
            ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }


        public void massivePreparedStatementTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");

            try
            {
                ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         numb1 double,
         numb2 int
         );", tableName));
            }
            catch (AlreadyExistsException)
            {
            }
            int numberOfPrepares = 100;

            List<object[]> values = new List<object[]>(numberOfPrepares);
            List<PreparedStatement> prepares = new List<PreparedStatement>();

            Parallel.For(0, numberOfPrepares, i =>
            {

                var prep = PrepareQuery(Session, string.Format("INSERT INTO {0}(tweet_id, numb1, numb2) VALUES ({1}, ?, ?);", tableName, Guid.NewGuid()));

                lock (prepares)
                    prepares.Add(prep);

            });

            Parallel.ForEach(prepares, prep =>
                {
                    ExecutePreparedQuery(this.Session, prep, new object[] { (double)RandomVal(typeof(double)), (int)RandomVal(typeof(int)) });
                });

            ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName));
        }

        public void checkingOrderOfCollection(string CassandraCollectionType, Type TypeOfDataToBeInputed, Type TypeOfKeyForMap = null, string pendingMode="")
        {                       
            string cassandraDataTypeName = convertTypeNameToCassandraEquivalent(TypeOfDataToBeInputed);
            string cassandraKeyDataTypeName = "";

            string openBracket = CassandraCollectionType == "list" ? "[" : "{";
            string closeBracket = CassandraCollectionType == "list" ? "]" : "}";
            string mapSyntax = "";

            string randomKeyValue = string.Empty;

            if (TypeOfKeyForMap != null)
            {
                cassandraKeyDataTypeName = convertTypeNameToCassandraEquivalent(TypeOfKeyForMap);
                mapSyntax = cassandraKeyDataTypeName + ",";

                if (TypeOfKeyForMap == typeof(DateTimeOffset))
                    randomKeyValue = (string)(RandomVal(typeof(DateTimeOffset)).GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(RandomVal(typeof(DateTimeOffset)), new object[1] { "yyyy-MM-dd H:mm:sszz00" }) + "' : '");
                else
                    randomKeyValue = RandomVal(TypeOfDataToBeInputed) + "' : '";
            }

            
            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         some_collection {1}<{2}{3}>
         );", tableName, CassandraCollectionType, mapSyntax, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }
            Guid tweet_id = Guid.NewGuid();            

            StringBuilder longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            int CollectionElementsNo = 100;
            List<Int32> orderedAsInputed = new List<Int32>(CollectionElementsNo);

            string inputSide = "some_collection + {1}";
            if (CassandraCollectionType == "list" && pendingMode == "prepending")
                inputSide = "{1} + some_collection";
            
            for (int i = 0; i < CollectionElementsNo; i++)
            {
                var data = i*(i%2);                
                longQ.AppendFormat(@"UPDATE {0} SET some_collection = "+inputSide+" WHERE tweet_id = {2};"
                    , tableName, openBracket + randomKeyValue + data + closeBracket, tweet_id.ToString());
                orderedAsInputed.Add(data);
            }

            longQ.AppendLine("APPLY BATCH;");
            ExecuteSyncNonQuery(Session, longQ.ToString(), "Inserting...");

            if (CassandraCollectionType == "set")
            {
                orderedAsInputed.Sort();
                orderedAsInputed.RemoveRange(0, orderedAsInputed.LastIndexOf(0));
            }
            else if (CassandraCollectionType == "list" && pendingMode == "prepending")
                orderedAsInputed.Reverse();

            CqlRowSet rs = Session.Execute(string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.Default);

            using (rs)
            {
                int ind = 0;
                foreach (var row in rs.GetRows())
                    foreach (var value in row[1] as System.Collections.IEnumerable)
                    {
                        Assert.True(orderedAsInputed[ind] == (int)value);
                        ind++;
                    }
            }

            ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName));
            ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void insertingSingleCollection(string CassandraCollectionType, Type TypeOfDataToBeInputed, Type TypeOfKeyForMap = null)
        {
            string cassandraDataTypeName = convertTypeNameToCassandraEquivalent(TypeOfDataToBeInputed);
            string cassandraKeyDataTypeName = "";

            string openBracket = CassandraCollectionType == "list" ? "[" : "{";
            string closeBracket = CassandraCollectionType == "list" ? "]" : "}";
            string mapSyntax = "";

            var randomValue = RandomVal(TypeOfDataToBeInputed);
            if (TypeOfDataToBeInputed == typeof(string))
                randomValue = "'" + randomValue.ToString().Replace("'", "''") + "'";

            string randomKeyValue = string.Empty;

            if (TypeOfKeyForMap != null)
            {
                cassandraKeyDataTypeName = convertTypeNameToCassandraEquivalent(TypeOfKeyForMap);
                mapSyntax = cassandraKeyDataTypeName + ",";

                if (TypeOfKeyForMap == typeof(DateTimeOffset))
                    randomKeyValue = "'"+(string)(RandomVal(typeof(DateTimeOffset)).GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(RandomVal(typeof(DateTimeOffset)), new object[1] { "yyyy-MM-dd H:mm:sszz00" }) + "'");
                else if(TypeOfKeyForMap == typeof(string))
                    randomKeyValue = "'"+RandomVal(TypeOfDataToBeInputed).ToString().Replace("'","''")+"'";
                else
                    randomKeyValue = RandomVal(TypeOfDataToBeInputed).ToString();
            }
            
            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         some_collection {1}<{2}{3}>
         );", tableName, CassandraCollectionType, mapSyntax, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();

            ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,some_collection) VALUES ({1}, {2});", tableName, tweet_id.ToString(), openBracket + randomKeyValue + (string.IsNullOrEmpty(randomKeyValue)?"":" : ") + randomValue + closeBracket));

            StringBuilder longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            int CollectionElementsNo = 100;
            for (int i = 0; i < CollectionElementsNo; i++)
            {
                var val = RandomVal(TypeOfDataToBeInputed);
                if (TypeOfDataToBeInputed == typeof(string))
                    val = "'" + val.ToString().Replace("'", "''") + "'";

                longQ.AppendFormat(@"UPDATE {0} SET some_collection = some_collection + {1} WHERE tweet_id = {2};"
                    , tableName, openBracket + randomKeyValue + (string.IsNullOrEmpty(randomKeyValue) ? "" : " : ") + val + closeBracket, tweet_id.ToString());
            }
            longQ.AppendLine("APPLY BATCH;");
            ExecuteSyncNonQuery(Session, longQ.ToString(), "Inserting...");

            ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName));
            ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }        

        public void TimestampTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");
                ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         ts timestamp
         );", tableName));

            ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), "2011-02-03 04:05+0000"), null);
            ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), 220898707200000), null);
            ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), 0), null);

            ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName));
            ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }
        
        public void createSecondaryIndexTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");
            string columns = "tweet_id uuid, name text, surname text";

            try
            {
                ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         {1},
PRIMARY KEY(tweet_id)
         );", tableName, columns));
            }
            catch (AlreadyExistsException)
            {
            }

            object[] row1 = new object[3] { Guid.NewGuid(), "Adam", "Małysz"}; 
            object[] row2 = new object[3] { Guid.NewGuid(), "Adam", "Miałczyński"};

            List<object[]> toReturn = new List<object[]>(2) 
            { row1, row2 };
            List<object[]> toInsert = new List<object[]>(2)
            { row1, row2 };

            ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, name, surname) VALUES({1},'{2}','{3}');", tableName, toInsert[0][0], toInsert[0][1], toInsert[0][2]));
            ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, name, surname) VALUES({1},'{2}','{3}');", tableName, toInsert[1][0], toInsert[1][1], toInsert[1][2]));

            int RowsNb = 0;

            for (int i = 0; i < RowsNb; i++)
            {
                toInsert.Add(new object[3] { Guid.NewGuid(), i.ToString(), "Miałczyński" });
                ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, name, surname) VALUES({1},'{2}','{3}');", tableName, toInsert[i][0], toInsert[i][1], toInsert[i][2]));
            }
            
            ExecuteSyncNonQuery(Session, string.Format("CREATE INDEX ON {0}(name);", tableName));            
            Thread.Sleep(50);
            ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0} WHERE name = 'Adam';", tableName), toReturn);
            ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        private Randomm rndm = new Randomm();
        private object RandomVal(Type tp)
        {            
            if (tp != null)
                return rndm.GetType().GetMethod("Next" + tp.Name).Invoke(rndm, new object[] { });            
            else
                return "";
        }

        public void checkMetadata(string TableName = null, string KeyspaceName = null)
        {
            Dictionary<string, ColumnTypeCode> columns = new Dictionary
                <string, ColumnTypeCode>()
                {
                    {"q0uuid", ColumnTypeCode.Uuid},
                    {"q1timestamp", ColumnTypeCode.Timestamp},
                    {"q2double", ColumnTypeCode.Double},
                    {"q3int32", ColumnTypeCode.Int},
                    {"q4int64", ColumnTypeCode.Bigint},
                    {"q5float", ColumnTypeCode.Float},
                    {"q6inet", ColumnTypeCode.Inet},
                    {"q7boolean", ColumnTypeCode.Boolean},
                    {"q8inet", ColumnTypeCode.Inet},
                    {"q9blob", ColumnTypeCode.Blob},
#if NET_40_OR_GREATER
                         {"q10varint", Metadata.ColumnTypeCode.Varint},
                         {"q11decimal", Metadata.ColumnTypeCode.Decimal},
#endif
                    {"q12list", ColumnTypeCode.List},
                    {"q13set", ColumnTypeCode.Set},
                    {"q14map", ColumnTypeCode.Map}
                    //{"q12counter", Metadata.ColumnTypeCode.Counter}, A table that contains a counter can only contain counters
                };            

            string tablename = TableName ?? "table" + Guid.NewGuid().ToString("N");
            StringBuilder sb = new StringBuilder(@"CREATE TABLE " + tablename + " (");
            Randomm urndm = new Randomm(DateTimeOffset.Now.Millisecond);
            
            foreach (var col in columns)
                sb.Append(col.Key + " " + col.Value.ToString() +
                          (((col.Value == ColumnTypeCode.List) ||
                            (col.Value == ColumnTypeCode.Set) ||
                            (col.Value == ColumnTypeCode.Map))
                               ? "<int" + (col.Value == ColumnTypeCode.Map ? ",varchar>" : ">")
                               : "") + ", ");

            sb.Append("PRIMARY KEY(");
            int rowKeys = urndm.Next(1, columns.Count - 3);

            for (int i = 0; i < rowKeys; i++)
                sb.Append(columns.Keys.Where(key => key.StartsWith("q" + i.ToString())).First() +
                          ((i == rowKeys - 1) ? "" : ", "));
            sb.Append("));");

            ExecuteSyncNonQuery(Session, sb.ToString());

            var table = this.Cluster.Metadata.GetTable(KeyspaceName ?? Keyspace, tablename);
            foreach (var metaCol in table.TableColumns)
            {
                Assert.True(columns.Keys.Contains(metaCol.Name));
                Assert.True(metaCol.TypeCode ==
                            columns.Where(tpc => tpc.Key == metaCol.Name).First().Value);
                Assert.True(metaCol.Table == tablename);
                Assert.True(metaCol.Keyspace == (KeyspaceName ?? Keyspace));
            }
        }


        public void checkKSMetadata()
        {
            string keyspacename = "keyspace" + Guid.NewGuid().ToString("N").ToLower();
            bool durableWrites = false;
            string strgyClass = "SimpleStrategy";
            short rplctnFactor = 1;
            Session.Execute(
string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : '{1}', 'replication_factor' : {2} }}
         AND durable_writes={3};"
, keyspacename, strgyClass, rplctnFactor.ToString(), durableWrites.ToString()));

            Session.ChangeKeyspace(keyspacename);
            

            for (int i = 0; i < 10; i++)
                checkMetadata("table" + Guid.NewGuid().ToString("N"),keyspacename);

            KeyspaceMetadata ksmd = Cluster.Metadata.GetKeyspace(keyspacename);
            Assert.True(ksmd.DurableWrites == durableWrites);
            Assert.True(ksmd.Replication.Where(opt => opt.Key == "replication_factor").First().Value == rplctnFactor);
            Assert.True(ksmd.StrategyClass == strgyClass);
        }

        public void CreateKeyspaceWithPropertiesTest(string strategy_class)
        {
            Session.DeleteKeyspaceIfExists(Keyspace);            
            bool durable_writes = rndm.NextBoolean();

            int? replication_factor = null;
            int? data_centers_count = null;
            Dictionary<string, int> datacenters_replication_factors = null;

            if (strategy_class == ReplicationStrategies.SimpleStrategy)
            {
                replication_factor = rndm.Next(1, 21);
                Session = Cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(ReplicationStrategies.CreateSimpleStrategyReplicationProperty((int)replication_factor), durable_writes);
            }
            else
                if (strategy_class == ReplicationStrategies.NetworkTopologyStrategy)
                {
                    data_centers_count = rndm.Next(1, 11);
                    datacenters_replication_factors = new Dictionary<string, int>((int)data_centers_count);
                    for (int i = 0; i < data_centers_count; i++)
                        datacenters_replication_factors.Add("dc" + i.ToString(), rndm.Next(1, 21));
                    Session = Cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(ReplicationStrategies.CreateNetworkTopologyStrategyReplicationProperty(datacenters_replication_factors), durable_writes);
                }

            KeyspaceMetadata ksmd = Cluster.Metadata.GetKeyspace(Keyspace);
            Assert.Equal(strategy_class, ksmd.StrategyClass);            
            Assert.Equal(durable_writes, ksmd.DurableWrites);  
            if(replication_factor != null)
                Assert.Equal(replication_factor, ksmd.Replication["replication_factor"]);
            if (datacenters_replication_factors != null)
                Assert.True(datacenters_replication_factors.SequenceEqual(ksmd.Replication));                
        }
    }
}
