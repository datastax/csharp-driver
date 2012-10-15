using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Native;
using System.Net;
using System.Threading;
using System.Globalization;

namespace Playground
{
    class Program
    {
        static void Main(string[] args)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");

            CassandraManager manager = new CassandraManager(
                new IPEndPoint[] { new IPEndPoint(IPAddress.Parse("168.63.14.29"), 8000) });

            {
                CassandraManagedConnection connection = manager.Connect();
                {
                    var response = connection.ExecuteQuery("USE test");
                    if (response is OutputSetKeyspace)
                    {
                    }
                }
//                {
//                    var ar = connection.BeginExecuteQuery(
//                         string.Format(@"CREATE TABLE {0}(
//         tweet_id uuid,
//         author text,
//         body text,
//         isok boolean,
//		 fval float,
//		 dval double,
//         PRIMARY KEY(tweet_id))", "test2"),
//                      (r) =>
//                      {
//                          try
//                          {
//                              var output = connection.EndExecuteQuery(r);
//                          }
//                          catch (Exception e)
//                          {
//                          }
//                      }
//                        , null);

//                    ar.AsyncWaitHandle.WaitOne();
//                }

//                {
//                    Random rndm = new Random();
//                    StringBuilder longQ = new StringBuilder();
//                    longQ.AppendLine("BEGIN BATCH ");

//                    int RowsNo = 100;
//                    for (int i = 0; i < RowsNo; i++)
//                    {
//                        longQ.AppendFormat(@"INSERT INTO {0} (
//         tweet_id,
//         author,
//         isok,
//         body,
//		 fval,
//		 dval)
//VALUES ({1},'test{2}','{3}','body{2}','{4}','{5}');", "test2", Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true", rndm.Next(234), rndm.NextDouble());
//                    }
//                    longQ.AppendLine("APPLY BATCH;");

//                    var result = connection.ExecuteQuery(longQ.ToString());


//                }

                {
                    var output = connection.ExecuteQuery(string.Format(@"SELECT * from {0} LIMIT 5000;", "test2"));
                    if (output is OutputRows)
                    {
                        CqlRowsPopulator populator = new CqlRowsPopulator(output as OutputRows);

                        foreach (var row in populator.GetRows())
                        {
                            for (int idx = 0; idx < populator.Columns.Length; idx++)
                            {
                                row.GetValue<int>(idx);
                                row.GetValue<int>("name");
                            }
                        }

                    }
                }
            }
            Console.ReadKey();
        }
    }
}
