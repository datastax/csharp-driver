using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraSamples
{
    class Program
    {
        private static ISession _session;

        static void Main(string[] args)
        {
            //Create a cluster instance using a builder
            //You can specify additional options like: credentials, enable SSL, ...
            var cluster = Cluster.Builder()
                .AddContactPoint("127.0.0.1")
                .Build();

            //Create the schema for the sample (out of scope of the samples): TL;DR
            SchemaHelper.CreateSchema(cluster);

            //Use the cluster to create a Cassandra.Session instance
            //This session uses the specified keyspace
            _session = cluster.Connect("driver_samples_kp");

            TimeSeriesExample();

            Console.WriteLine("...Press enter for next sample...");
            Console.ReadLine();
            TimeSeriesExampleAsync();

            Console.WriteLine("...Press enter for next sample...");
            Console.ReadLine();
            ForumExample();

            Console.WriteLine("Press enter to exit...");
            Console.ReadLine();
        }


        /// <summary>
        /// Using a time series schema, it inserts a few records and then retrieves some of them 
        /// </summary>
        public static void TimeSeriesExample()
        {
            Console.WriteLine("--------------------");
            Console.WriteLine("Executing time series sample");

            var repository = new TemperatureRepository(_session);
            //Insert some data
            var weatherStation = "station1";
            //Trying to simulate the insertion of several rows
            //with temperature measures
            for (var i = 0; i < 1000; i++ )
            {
                repository.AddTemperature(weatherStation, i / 16M);
            }

            //Now lets retrieve the temperatures for a given date
            var rs = repository.GetTemperatureRecords(weatherStation, DateTime.Today);
            //lets print a few of them
            Console.WriteLine("Printing the first temperature records (only 20)");
            var counter = 0;
            foreach (var row in rs)
            {
                Console.Write(row.GetValue<string>("weatherstation_id"));
                Console.Write("\t");
                Console.Write(row.GetValue<DateTime>("event_time").ToString("HH:mm:ss.fff"));
                Console.Write("\t");
                Console.WriteLine(row.GetValue<decimal>("temperature"));
                //It is just an example, 20 is enough
                if (counter++ == 20)
                {
                    break;
                }
            }
        }

        /// <summary>
        /// Using a time series schema, it async inserts a few records and then retrieves some of them 
        /// </summary>
        public static void TimeSeriesExampleAsync()
        {
            Console.WriteLine("--------------------");
            Console.WriteLine("Executing time series async sample");

            var repository = new TemperatureRepository(_session);
            //Insert some data
            var weatherStation = "station2";
            //Trying to simulate the insertion of several rows
            //with temperature measures
            //We will not wait for the insertion to try to insert the next
            //We will create a task list
            var insertTaskList = new List<Task>();
            for (var i = 0; i < 1000; i++)
            {
                //It returns a task that is going to be completed when the Cassandra ring acknowledge the insert
                var task = repository.AddTemperatureAsync(weatherStation, i / 16M);
                //Maintain the task for later use.
                insertTaskList.Add(task);
            }

            //Now lets wait until any of the insert succeeds.
            Task.WaitAny(insertTaskList.ToArray());

            //Now lets retrieve the temperatures for a given date
            var rs = repository.GetTemperatureRecords(weatherStation, DateTime.Today);
            //lets print a few of them
            Console.WriteLine("Printing the first temperature records (only 20, if available)");
            var counter = 0;
            foreach (var row in rs)
            {
                Console.Write(row.GetValue<string>("weatherstation_id"));
                Console.Write("\t");
                Console.Write(row.GetValue<DateTime>("event_time").ToString("HH:mm:ss.fff"));
                Console.Write("\t");
                Console.WriteLine(row.GetValue<decimal>("temperature"));
                //It is just an example, 20 is enough
                if (counter++ == 20)
                {
                    Console.WriteLine();
                    break;
                }
            }
        }

        /// <summary>
        /// Using a typical topic/message schema, it inserts a topic, messages and retrieves them.
        /// </summary>
        public static void ForumExample()
        {
            Console.WriteLine("--------------------");
            Console.WriteLine("Executing forum sample");

            var repository = new ForumRepository(_session);
            //Add a topic
            //It will insert 2 rows in a batch
            var topicId = Guid.NewGuid();
            repository.AddTopic(topicId, "Sample forum thread", "This is the first message and body of the topic");

            //Insert some messages
            for (var i = 1; i < 250; i++)
            {
                repository.AddMessage(topicId, "Message " + (i + 1));
            }

            //Now lets retrieve the messages by topic with a page size of 20.
            var rs = repository.GetMessages(topicId, 100);
            //At this point only 100 rows are loaded into the RowSet.
            Console.WriteLine("Printing all the rows paginating with a page size of 100");
            foreach (var row in rs)
            {
                //While we iterate though the RowSet
                //We will paginate through all the rows
                Console.WriteLine(row.GetValue<string>("message_body"));
            }
        }
    }
}
