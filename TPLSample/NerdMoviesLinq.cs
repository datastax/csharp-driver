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
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using System.Threading;

//based on https://github.com/pchalamet/cassandra-sharp/tree/master/Samples
namespace TPLSample.NerdMoviesLinqSample
{

    [AllowFiltering]
    public class NerdMovie
    {
        [ClusteringKey(1)]
        public string Director;

        public string MainActor;

        [PartitionKey]
        public string Movie;

        public int Year;
    }

    public class NerdMoviesLinqSample
    {
        public static void Run()
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("192.168.13.1").WithoutRowSetBuffering().Build();

            using (var session = cluster.Connect())
            {
                const string keyspaceName = "Excelsior";
                Console.WriteLine("============================================================");
                Console.WriteLine("Creating keyspace...");

                try
                {
                    session.ChangeKeyspace(keyspaceName);
                }
                catch (InvalidQueryException)
                {
                    session.CreateKeyspaceIfNotExists(keyspaceName);
                    session.ChangeKeyspace(keyspaceName);
                
                }

                Console.WriteLine("============================================================");
                Console.WriteLine("Creating tables...");
                var context = new Context(session);
                context.AddTable<NerdMovie>();
                context.CreateTablesIfNotExist();
                Console.WriteLine("============================================================");



                var tpl = session.GetTable<NerdMovie>().Insert(new NerdMovie() { Movie = "?", Director = "?", MainActor = "?", Year = 102 }).ToString();
                tpl = tpl.Replace(@"'?'", @"?").Replace("102", "?");

                var qq = session.Execute(new SimpleStatement(tpl).Bind("KakaX", "PikerY", "PikerZ", 2033));

                var tpl2 = session.GetTable<NerdMovie>().Insert(new NerdMovie() { Movie = "?", Director = "Kokosz", MainActor = "?", Year = 102 }).ToString();
                tpl2 = tpl2.Replace(@"'?'", @"?").Replace("102", "?");

                var tpl3 = session.GetTable<NerdMovie>().Insert(new NerdMovie() { Movie = "?", Director = "Kokoszx", MainActor = "?", Year = 102 }).ToString();
                tpl3 = tpl3.Replace(@"'?'", @"?").Replace("102", "?");

                var prep = session.Prepare(tpl);
                var bound = prep.Bind("Kaka1", "Loker1", "Piker1", 2023);

                var prep2 = session.Prepare(tpl2);
                var bound2 = prep2.Bind("Kaka2", "Piker2", 2023);

                var prep3 = session.Prepare(tpl3);
                var bound3 = prep2.Bind("Kaka3", "Piker3", 2023);


                //                session.Execute(bound);


                var bs = new BatchStatement()
                    //   .AddQuery(new SimpleStatement(
                    //session.GetTable<NerdMovie>().Insert(new NerdMovie() { Movie = "Serenity2", Director = "Joss Whedon", MainActor = "Nathan Fillion", Year = 2005 }).ToString()
                    //   ))
                    //   .AddQuery(new SimpleStatement(
                    //session.GetTable<NerdMovie>().Insert(new NerdMovie() { Movie = "Serenity3", Director = "Joss Whedon2", MainActor = "Nathan Fillion", Year = 2005 }).ToString()
                    //   ))
                    //   .AddQuery(new SimpleStatement(
                    //session.GetTable<NerdMovie>().Insert(new NerdMovie() { Movie = "Serenity4", Director = "Joss Whedon3", MainActor = "Nathan Fillion", Year = 2005 }).ToString()
                    //   ))
                    .AddQuery(bound)
                    .AddQuery(bound2)
                    .AddQuery(bound3)
                    .SetBatchType(BatchType.Unlogged);
                session.Execute(bs);


                context.GetTable<NerdMovie>().AddNew(new NerdMovie() { Movie = "Serenity", Director = "Joss Whedon", MainActor = "Nathan Fillion", Year = 2005 });
                var taskSaveMovies = Task.Factory.FromAsync(context.BeginSaveChangesBatch, context.EndSaveChangesBatch, TableType.Standard, ConsistencyLevel.Default, null);

                taskSaveMovies.Wait();

                var selectNerdMovies = context.GetTable<NerdMovie>(); //select everything from table


                var taskSelectStartMovies =
                    Task<IEnumerable<NerdMovie>>.Factory.FromAsync(selectNerdMovies.BeginExecute,
                                                                   selectNerdMovies.EndExecute, null)
                                                .ContinueWith(res => DisplayMovies(res.Result));



                taskSelectStartMovies.Wait();

                var selectAllFromWhere = from m in context.GetTable<NerdMovie>() where m.Director == "Joss Whedon" select m;

                var taskselectAllFromWhere =
                    Task<IEnumerable<NerdMovie>>.Factory.FromAsync(selectAllFromWhere.BeginExecute,
                                                                   selectAllFromWhere.EndExecute, null)
                                                .ContinueWith(res => DisplayMovies(res.Result));

                taskselectAllFromWhere.Wait();

                var taskselectAllFromWhereWithFuture =
                    Task<IEnumerable<NerdMovie>>.Factory.FromAsync(selectAllFromWhere.BeginExecute,
                                                                   selectAllFromWhere.EndExecute, null)
                                                .ContinueWith(a => a.Result.ToList());

                DisplayMovies(taskselectAllFromWhereWithFuture.Result);

                session.DeleteKeyspaceIfExists(keyspaceName);
            }

            cluster.Shutdown();
        }

        private static void DisplayMovies(IEnumerable<NerdMovie> result)
        {
            foreach (var resMovie in result)
            {
                Console.WriteLine("Movie={0} Director={1} MainActor={2}, Year={3}",
                                  resMovie.Movie, resMovie.Director, resMovie.MainActor, resMovie.Year);
            }
            Console.WriteLine();
            Console.WriteLine();
        }

    }
}