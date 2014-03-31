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
using Cassandra.Data.Linq;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Cassandra;

namespace Cassandra.Data.Linq.MSTest
{
    public class CassandraLog
    {
        [PartitionKey]        
        public string category;
        
        [ClusteringKey(0)]
        public DateTimeOffset date;

        [ClusteringKey(1)]
        public string message;
        
        public void display()        
        {
            Console.WriteLine(category + "\n " + date.ToString("MM/dd/yyyy H:mm:ss.fff zzz") + "\n " + message
                + Environment.NewLine);
        }
    }

    public class CassandraLogContext : Context
    {
        public CassandraLogContext(Session session)
            : base(session)
        {
            AddTable<CassandraLog>();
            CreateTablesIfNotExist();
        }
    }

    public class CassandraLogWriter : TextWriter 
    {
        public CassandraLogContext context { get; set; }
        public ContextTable<CassandraLog> LogsTable { get; set; }
        private List<CassandraLog> logsTableLocal = new List<CassandraLog>();
        private bool canWrite = false;


        private void Update(List<CassandraLog> logs)
        {
            foreach (var log in logs)
                LogsTable.AddNew(log);
            context.SaveChanges();
        }

        private void Update(CassandraLog log)
        {            
            LogsTable.AddNew(log);
            context.SaveChanges();
        }

        public void GetContext(CassandraLogContext cntxt)
        {
            this.context = cntxt;
            this.LogsTable = cntxt.GetTable<CassandraLog>();
            this.canWrite = true;
            Update(logsTableLocal);
        }

        public override void WriteLine(string value)
        {            
            var category = value.Split(':')[0];
            var message = value.Split('#')[1];
            var date = DateTimeOffset.ParseExact(value.Split('#')[0].Replace(category + ":", "").Trim(), "MM/dd/yyyy H:mm:ss.fff zzz", null);
            var newLog = new CassandraLog() { date = date , message = message, category = category };
            if (!canWrite)
                logsTableLocal.Add(newLog);
            else
                Update(newLog);                                               
        }

        public void StopWritingToDB()
        {
            canWrite = false;
        }
        public override System.Text.Encoding Encoding
        {
            get { return Encoding.UTF8; }
        }       
    }

}
