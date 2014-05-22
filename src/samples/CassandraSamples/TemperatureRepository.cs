using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraSamples
{
    /// <summary>
    /// Represents an application repository.
    /// Based on the schema in: http://planetcassandra.org/blog/post/getting-started-with-time-series-data-modeling/
    /// There is almost no code reuse in order to have all the necessary code in one place.
    /// There are several optimizations that can be made, but are out of scope of this sample.
    /// </summary>
    public class TemperatureRepository
    {
        protected ISession Session { get; set; }

        /// <summary>
        /// Create a new instance of the repository with the session as a dependency
        /// </summary>
        public TemperatureRepository(ISession session)
        {
            this.Session = session;
        }

        /// <summary>
        /// Add a temperature information for a given weather station
        /// </summary>
        public void AddTemperature(string weatherStationId, decimal value)
        {
            var insertCql = @"
                INSERT INTO temperature_by_day 
                (weatherstation_id, date, event_time, temperature)
                VALUES
                (?, ?, ?, ?)";
           
            //Create an insert statement
            var insertStatement = new SimpleStatement(insertCql);
            //Bind the parameters to the statement
            insertStatement.Bind(weatherStationId, DateTime.Now.ToString("yyyyMMdd"), DateTime.Now, value);
            //You can set other options of the statement execution, for example the consistency level.
            insertStatement.SetConsistencyLevel(ConsistencyLevel.Quorum);
            //Execute the insert
            Session.Execute(insertStatement);
        }

        /// <summary>
        /// Gets the list of temperature stored for a provided weather station and day.
        /// </summary>
        public RowSet GetTemperatureRecords(string weatherStationId, DateTime day)
        {
            var selectCql = "SELECT * FROM temperature_by_day WHERE weatherstation_id = ? AND date = ?";
            //Create a statement
            var selectStatement = new SimpleStatement(selectCql);
            //Add the parameters
            selectStatement.Bind(weatherStationId, DateTime.Now.ToString("yyyyMMdd"));
            //Execute the select statement
            return Session.Execute(selectStatement);
        }

        /// <summary>
        /// Add a temperature information for a given weather station asynchronously
        /// </summary>
        public Task AddTemperatureAsync(string weatherStationId, decimal value)
        {
            //It is basically the same code as the AddTemperature
            //Except it returns a Task that already started but didn't finished
            //The row would not be in Cassandra yet when this method finishes executing.
            var insertCql = @"
                INSERT INTO temperature_by_day 
                (weatherstation_id, date, event_time, temperature)
                VALUES
                (?, ?, ?, ?)";

            //Create an insert statement
            var insertStatement = new SimpleStatement(insertCql);
            //Bind the parameters to the statement
            insertStatement.Bind(weatherStationId, DateTime.Now.ToString("yyyyMMdd"), DateTime.Now, value);
            //You can set other options of the statement execution, for example the consistency level.
            insertStatement.SetConsistencyLevel(ConsistencyLevel.Quorum);
            //Execute the insert
            return Session.ExecuteAsync(insertStatement);
        }
    }
}
