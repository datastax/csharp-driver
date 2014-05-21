using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

            Console.WriteLine("Press enter to exit...");
            Console.ReadLine();
        }

        public static void TimeSeriesExample()
        {
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
                    Console.WriteLine();
                    break;
                }
            }
        }
    }
}
