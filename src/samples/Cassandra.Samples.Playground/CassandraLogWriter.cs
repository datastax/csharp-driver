using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Cassandra.Data.Linq;

namespace Playground
{
    public class CassandraLogWriter : TextWriter
    {
        private readonly List<CassandraLog> logsTableLocal = new List<CassandraLog>();
        private bool canWrite;
        public CassandraLogContext context { get; set; }
        public ContextTable<CassandraLog> LogsTable { get; set; }

        public override Encoding Encoding
        {
            get { return Encoding.UTF8; }
        }


        private void Update(List<CassandraLog> logs)
        {
            foreach (CassandraLog log in logs)
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
            context = cntxt;
            LogsTable = cntxt.GetTable<CassandraLog>();
            canWrite = true;
            Update(logsTableLocal);
        }

        public override void WriteLine(string value)
        {
            string category = value.Split(':')[0];
            string message = value.Split('#')[1];
            DateTimeOffset date = DateTimeOffset.ParseExact(value.Split('#')[0].Replace(category + ":", "").Trim(), "MM/dd/yyyy H:mm:ss.fff zzz", null);
            var newLog = new CassandraLog {date = date, message = message, category = category};
            if (!canWrite)
                logsTableLocal.Add(newLog);
            else
                Update(newLog);
        }

        public void StopWritingToDB()
        {
            canWrite = false;
        }
    }
}