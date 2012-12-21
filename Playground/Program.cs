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

            Cluster cluster = Cluster.Builder.AddContactPoint("137.116.194.96").Build();

            var session = cluster.Connect();

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

            TwitterContext twitterContext = new TwitterContext(session, ConsistencyLevel.ONE, ConsistencyLevel.ONE);
            var tweetsTable = twitterContext.GetTable<Tweet>();
            var followersTable = twitterContext.GetTable<Followers>();
            var followedTweetsTable = twitterContext.GetTable<FollowedTweet>();
            var statisticsTable = twitterContext.GetTable<Statistics>();
            
            Console.WriteLine("Done!");

            //Adding authors and their followers to the "Followers" table: 
            Console.WriteLine("Adding authors and their followers to the \"Followers\" table..");
            int AuthorsNo = 50;
            List<Followers> FollowersLocal = new List<Followers>();
            List<Statistics> StatisticsLocal = new List<Statistics>();
            List<string> AuthorsID = new List<string>();
            
            for (int i = 0; i < AuthorsNo; i++)
            {
                var author_ID = "Author" + i.ToString();
                var followerEnt = new Followers() { author_id = author_ID, followers = AuthorsID.Where(fol => fol != author_ID).ToList() };
                followersTable.AddNew(followerEnt);
                FollowersLocal.Add(followerEnt);
                AuthorsID.Add(followerEnt.author_id);
                
                //We will also add authors to table with statistics: 
                var statEnt = new Statistics() { author_id = author_ID };
                statisticsTable.Attach(statEnt, EntityUpdateMode.ModifiedOnly, EntityTrackingMode.KeepAtachedAfterSave);                
                
                //And increment number of followers for each of them: 
                followerEnt.followers.ForEach(folo => statEnt.followers_count += 1);
                StatisticsLocal.Add(statEnt);
            }
            twitterContext.SaveChanges(SaveChangesMode.Batch);            
            Console.WriteLine("Done!");

            //Now every author will add a single tweet:
            Console.WriteLine("Now authors are writing their tweets..");
            List<Tweet> TweetsLocal = new List<Tweet>();
            List<FollowedTweet> FollowedTweetsLocal = new List<FollowedTweet>();
            foreach (var auth in AuthorsID)
            {
                var tweetEnt = new Tweet() {tweet_id = Guid.NewGuid(), author_id = auth, body = "Hello world! My name is " + auth + (DateTime.Now.Second % 2 == 0 ? ".":"") , date = DateTimeOffset.Now};                
                tweetsTable.AddNew(tweetEnt);
                TweetsLocal.Add(tweetEnt);

                //We will update our statistics table                 
                StatisticsLocal.Where(stat => stat.author_id == auth).First().tweets_count += 1;                                                                                
                
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

            twitterContext.SaveChanges(SaveChangesMode.Batch);            
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
            

            //Lets check all of authors punctuation in their tweets, or at least, if they end their tweets with full stop, exclamation or question mark:
            List<string> authorsWithPunctuationProblems = new List<string>();
            
            //To check it, we can use anonymous class because we are interested only in author_id and body of the tweet
            var slimmedTweets = (from t in tweetsTable select new { t.author_id, t.body }).Execute();                        
            foreach(var slimTwt in slimmedTweets)
                if (!(slimTwt.body.EndsWith(".") || slimTwt.body.EndsWith("!") || slimTwt.body.EndsWith("?")))
                    if (!authorsWithPunctuationProblems.Contains(slimTwt.author_id))
                        authorsWithPunctuationProblems.Add(slimTwt.author_id);

            
            // Now we can check how many of all authors have this problem..            
            float proportion = (float)authorsWithPunctuationProblems.Count() / followersTable.Count().Execute() * 100;            
            Console.WriteLine(separator + string.Format("{0}% of all authors doesn't end tweet with punctuation mark!", proportion)); 

            // This time I will help them, and update these tweets with a full stop..            
            foreach (var tweet in tweetsTable.Where(x => authorsWithPunctuationProblems.Contains(x.author_id)).Execute())
            {
                tweetsTable.Attach(tweet);
                TweetsLocal.Where(twt => twt.tweet_id == tweet.tweet_id).First().body += ".";
                tweet.body += ".";
            }            
            twitterContext.SaveChanges(SaveChangesMode.Batch);
        
                        
            //Deleting all tweets from "Tweet" table
            foreach (var ent in TweetsLocal)
            {
                tweetsTable.Delete(ent);
                StatisticsLocal.Where(auth => auth.author_id == ent.author_id).First().tweets_count -= 1; 
            }
            twitterContext.SaveChanges(SaveChangesMode.Batch);

            //Statistics after deletion of tweets:
            Console.WriteLine(separator + "After deletion of all tweets our \"Statistics\" table looks like:" + separator);
            Console.WriteLine("Author ID | Followers count | Tweets count" + separator);
            foreach (var stat in (from st in statisticsTable select st).Execute())
                Console.WriteLine(stat.author_id + "  |        " + stat.followers_count + "       |      " + stat.tweets_count);
            

            Console.WriteLine(separator + "Deleting keyspace: \"" + keyspaceName + "\"");
            session.DeleteKeyspaceIfExists(keyspaceName);
            Console.WriteLine("Done! Press any key to exit..");
            Console.ReadKey();
        }


    }
}
