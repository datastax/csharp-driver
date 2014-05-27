using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CassandraSamples
{
    /// <summary>
    /// Represents an application repository.
    /// Based on the schema in typical forum schema: topics and messages, being the first message the topic body.
    /// There is almost no code reuse in order to have all the necessary code in one place.
    /// There are several optimizations that can be made, but are out of scope of this sample.
    /// </summary>
    public class ForumRepository
    {
        protected ISession Session { get; set; }

        /// <summary>
        /// Create a new instance of the repository with the session as a dependency
        /// </summary>
        public ForumRepository(ISession session)
        {
            this.Session = session;
        }

        public void AddTopic(Guid topicId, string title, string body)
        {
            //We will be inserting 2 rows in 2 column families.
            //One for the topic and other for the first message (the topic body).
            var insertTopicCql = "INSERT INTO topics (topic_id, topic_title, topic_date) VALUES (?, ?, ?)";
            var insertMessageCql = "INSERT INTO messages (topic_id, message_date, message_body) VALUES (?, ?, ?)";

            //We will do it in a batch, this way we can ensure that the 2 rows are inserted in the same atomic operation.
            var batch = new BatchStatement();

            //Prepare the insert topic statement and bind the parameters
            var insertTopicStatement = Session
                .Prepare(insertTopicCql)
                .Bind(topicId, title, DateTime.Now);

            //Prepare the insert message statement
            var insertMessageStatement = Session
                .Prepare(insertMessageCql)
                .Bind(topicId, DateTime.Now, body);

            //Add the statements to the batch
            batch.Add(insertTopicStatement);
            batch.Add(insertMessageStatement);

            //You can set other options of the batch execution, for example the consistency level.
            batch.SetConsistencyLevel(ConsistencyLevel.Quorum);
            //Execute the insert of the 2 rows
            Session.Execute(batch);
        }

        public void AddMessage(Guid topicId, string body)
        {
            //We will add 1 row using a prepared statement.
            var insertMessageCql = "INSERT INTO messages (topic_id, message_date, message_body) VALUES (?, ?, ?)";
            //Prepare the insert message statement
            var insertStatement = Session
                .Prepare(insertMessageCql)
                .Bind(topicId, DateTime.Now, body);

            //We can specify execution options for the statement
            insertStatement.SetConsistencyLevel(ConsistencyLevel.Quorum);

            //Execute the prepared statement
            Session.Execute(insertStatement);
        }

        public RowSet GetMessages(Guid topicId, int pageSize)
        {
            //We will add 1 row using a prepared statement.
            var selectCql = "SELECT * FROM messages WHERE topic_id = ?";
            //Prepare the insert message statement and bind the parameters
            var statement = Session
                .Prepare(selectCql)
                .Bind(topicId);

            //We can specify execution options, like page size and consistency
            statement
                .SetPageSize(pageSize)
                .SetConsistencyLevel(ConsistencyLevel.One);

            //Execute the prepared statement
            return Session.Execute(statement);
        }
    }
}
