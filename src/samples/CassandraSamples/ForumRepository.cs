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
        //Prepared statements that will get prepared once and executed multiple times with different bind variables
        PreparedStatement _insertTopicStatement;
        PreparedStatement _insertMessageStatement;

        protected ISession Session { get; set; }

        /// <summary>
        /// Create a new instance of the repository with the session as a dependency
        /// </summary>
        public ForumRepository(ISession session)
        {
            this.Session = session;

            _insertTopicStatement = Session.Prepare("INSERT INTO topics (topic_id, topic_title, topic_date) VALUES (?, ?, ?)");
            _insertMessageStatement = Session.Prepare("INSERT INTO messages (topic_id, message_date, message_body) VALUES (?, ?, ?)");
        }

        public void AddTopic(Guid topicId, string title, string body)
        {
            
            //We will be inserting 2 rows in 2 column families.
            //One for the topic and other for the first message (the topic body).
            //We will do it in a batch, this way we can ensure that the 2 rows are inserted in the same atomic operation.
            var batch = new BatchStatement();

            //bind the parameters on each statement and add them to the batch
            batch.Add(_insertTopicStatement.Bind(topicId, title, DateTime.Now));
            batch.Add(_insertMessageStatement.Bind(topicId, DateTime.Now, body));

            //You can set other options of the batch execution, for example the consistency level.
            batch.SetConsistencyLevel(ConsistencyLevel.Quorum);
            //Execute the insert of the 2 rows
            Session.Execute(batch);
        }

        public void AddMessage(Guid topicId, string body)
        {
            //We will add 1 row using a prepared statement.
            var boundStatement = _insertMessageStatement.Bind(topicId, DateTime.Now, body);
            //We can specify execution options for the statement
            boundStatement.SetConsistencyLevel(ConsistencyLevel.Quorum);
            //Execute the bound statement
            Session.Execute(boundStatement);
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
