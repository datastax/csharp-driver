using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using System.Globalization;
using Cassandra.Native;
using Cassandra.Data;
using Cassandra;
using System.Linq;

namespace Playground
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Connecting, setting keyspace and creating tables..");
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");

            CassandraCluster cluster = CassandraCluster.builder().addContactPoint("168.63.107.22").build();

            var session = cluster.connect();

            var keyspaceName = "test" + Guid.NewGuid().ToString("N");

            try
            {
                session.ChangeKeyspace(keyspaceName);
            }
            catch (CassandraClusterInvalidException ex)
            {
                session.CreateKeyspaceIfNotExists(keyspaceName);
                session.ChangeKeyspace(keyspaceName);
            }

            TweetsContext tweets = new TweetsContext(session, CqlConsistencyLevel.ONE, CqlConsistencyLevel.ANY);
            var tweetsTable = tweets.GetTable<Tweet>();

            FollowersContext followers = new FollowersContext(session, CqlConsistencyLevel.ONE, CqlConsistencyLevel.ANY);
            var followersTable = followers.GetTable<Followers>();

            FollowingTweetsContext followedTweets = new FollowingTweetsContext(session, CqlConsistencyLevel.ONE, CqlConsistencyLevel.ANY);
            var followedTweetsTable = followedTweets.GetTable<FollowedTweet>();

            Console.WriteLine("Done!");

            //Adding authors and their followers to the "Followers" table: 
            Console.WriteLine("Adding authors and their followers to the \"Followers\" table..");
            int AuthorsNo = 100;
            List<Followers> FollowersLocal = new List<Followers>();
            List<string> AuthorsID = new List<string>();
            
            for (int i = 0; i < AuthorsNo; i++)
            {
                var author_ID = "Author" + i.ToString();
                var followerEnt = new Followers() { author_id = author_ID, followers = AuthorsID.Where(fol => fol != author_ID).ToList() };
                followersTable.AddNew(followerEnt);
                FollowersLocal.Add(followerEnt);
                AuthorsID.Add(followerEnt.author_id);
            }                        
            followers.SaveChanges(CqlSaveChangesMode.OneByOne);
            Console.WriteLine("Done!");

            //Now every author will add a single tweet:
            Console.WriteLine("Now authors are writing their tweets..");

            List<Tweet> TweetsLocal = new List<Tweet>();
            List<FollowedTweet> FollowedTweetsLocal = new List<FollowedTweet>();
            foreach (var auth in AuthorsID)
            {
                var tweetEnt = new Tweet() {tweet_id = Guid.NewGuid(), author_id = auth, body = "Hello world! My name is " + auth , date = DateTimeOffset.Now};                
                tweetsTable.AddNew(tweetEnt);
                TweetsLocal.Add(tweetEnt);
                
                //We also have to add this tweet to "FollowedTweet" table, so every user who follows that author, will get this tweet on his/her own wall!
                FollowedTweet followedTweetEnt = null;
                foreach (var author in (from f in followersTable where f.author_id == auth select f).Execute())                
                    if(author.followers != null)
                        foreach (var follower in author.followers)
                        {
                            followedTweetEnt = new FollowedTweet() { user_id = follower, author_id = tweetEnt.author_id, body = tweetEnt.body, date = tweetEnt.date, tweet_id = tweetEnt.tweet_id };
                            followedTweetsTable.AddNew(followedTweetEnt);
                            FollowedTweetsLocal.Add(followedTweetEnt);
                        }
            }

            tweets.SaveChanges(CqlSaveChangesMode.Batch);
            followedTweets.SaveChanges(CqlSaveChangesMode.Batch);
            Console.WriteLine("Done!");

            string separator = Environment.NewLine + "--------------------------------------------------------------------" + Environment.NewLine;
            Console.WriteLine(separator);


            //To display users that follows "Author8":            
            foreach (var author in (from f in followersTable where f.author_id == "Author8" select f).Execute())            
                author.displayFollowers();                            


            //To display all of user "Author15" tweets:
            Console.WriteLine(separator + "All tweets posted by Author15:" + Environment.NewLine);  
            foreach (var authTweets in (from t in tweetsTable where t.author_id == "Author15" select t).Execute())            
                authTweets.display();                
            

            //To display all tweets from users that "Author95" follows:
            Console.WriteLine(separator + "All tweets posted by users that \"Author95\" follows:" + Environment.NewLine);
            foreach (var foloTwts in (from t in followedTweetsTable where t.user_id == "Author95" select t).Execute())             
                foloTwts.display();                
            
                        
            //Deleting all tweets from "Tweet" table
            foreach (var ent in TweetsLocal)
                tweetsTable.Delete(ent);

            tweets.SaveChanges(CqlSaveChangesMode.Batch);


            Console.WriteLine(separator + "Deleting keyspace: \"" + keyspaceName + "\"");
            session.DeleteKeyspaceIfExists(keyspaceName);
            Console.WriteLine("Done! Press any key to exit..");
            Console.ReadKey();
        }


    }
}
