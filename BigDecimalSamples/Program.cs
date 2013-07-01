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
using Cassandra.Data.Linq;
using Cassandra;

namespace BigDecimalSamples
{
    class Program
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

            public java.math.BigDecimal Earned;
        }

        static void Main(string[] args)
        {
            TypeAdapters.DecimalTypeAdapter = new IKVMDecimalTypeAdapter();

            System.Threading.Thread.CurrentThread.CurrentCulture = System.Globalization.CultureInfo.CreateSpecificCulture("en-US");
            Cluster cluster = Cluster.Builder().AddContactPoint("cassi.cloudapp.net").Build();

            using (var session = cluster.Connect())
            {

                const string keyspaceName = "Excelsior";

                try
                {
                    session.ChangeKeyspace(keyspaceName);
                }
                catch (InvalidException)
                {
                    session.CreateKeyspaceIfNotExists(keyspaceName);
                    session.ChangeKeyspace(keyspaceName);
                }

                var table = session.GetTable<NerdMovie>();
                table.CreateIfNotExists();

                {
                    var batch = session.CreateBatch();                    		
                    var movies = new List<NerdMovie>()
                    {
                        new NerdMovie(){ Movie = "Avatar", Director = "James Cameron", MainActor = "Sam Worthington", Year = 2009, Earned = new java.math.BigDecimal("2782275172.2782275172")},
                        new NerdMovie(){ Movie = "The Avengers", Director = "Joss Whedon", MainActor = "Robert Downey Jr.", Year = 2012, Earned = new java.math.BigDecimal("1511757910.1511757910")},
                        new NerdMovie(){ Movie = "Star Wars Episode III: Revenge of the Sith", Director = "George Lucas", MainActor = "Ewan McGregor", Year = 2005, Earned = new java.math.BigDecimal("848754768.848754768")},
                        new NerdMovie(){ Movie = "Django Unchained", Director = "Quentin Tarantino", MainActor = "Jamie Foxx", Year = 2012, Earned = new java.math.BigDecimal("407783271.407783271")},
                        new NerdMovie(){ Movie = "Serenity", Director = "Joss Whedon", MainActor = "Nathan Fillion", Year = 2005, Earned = new java.math.BigDecimal("-987654321")},
                        new NerdMovie(){ Movie = "Pulp Fiction", Director = "Quentin Tarantino", MainActor = "John Travolta", Year = 1994, Earned = new java.math.BigDecimal("987654321")},
                    };

                    batch.Append(from m in movies select table.Insert(m));
                    batch.Execute();
                }

                java.math.BigDecimal reallyBigDecimal = new java.math.BigDecimal("-123456789012345678901234567890.1234567890123456789012345678901234567890123456789012345678901234567890");
                session.Execute(session.Prepare("INSERT INTO \"NerdMovie\"(\"Movie\",\"Director\", \"MainActor\", \"Year\", \"Earned\") VALUES('Serenjty','Joss Whedon','Nathan Fillion',1999, ?)").Bind(reallyBigDecimal));
                
                var nm = (from m in table select new { M = m.Movie, E = m.Earned }).Execute().ToList();
                foreach (var movie in nm)
                    Console.WriteLine("\""+movie.M + "\" earned : " + movie.E + "$");

                session.DeleteKeyspaceIfExists(keyspaceName);
                Console.ReadKey();
            }
            cluster.Shutdown();
        }

    }
}
