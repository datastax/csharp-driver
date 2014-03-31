using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Cassandra.Data.Linq;

namespace Cassandra.IntegrationTests.Linq.Structures
{
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