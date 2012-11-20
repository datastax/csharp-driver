using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Net;
using System.Data.Common;
using System.Data.Objects;
using System.Data.EntityClient;
using System.Data.Metadata.Edm;
using System.Reflection;
using System.Linq;
using Cassandra.Native;
namespace Cassandra.Data.LinqTest
{
    public class TweetsContext : CqlContext
    {
        public TweetsContext(CassandraSession connection, bool releaseOnClose = false, string keyspaceName = null)
            : base(connection, releaseOnClose, keyspaceName)
        {
            AddTables();
        }
        public TweetsContext(string connectionString,string keyspaceName = null)
            : base(connectionString,keyspaceName)
        {
            AddTables();
        }

        private void AddTables()
        {
            AddTable<Tweets>();
        }
    }

    public class Tweets
    {
        [PartitionKey]
        public Guid tweet_id;        
        public string author;
        public string body;
        [RowKey]
        public bool isok;
        [RowKey]
        public int Key;
    }

    public class BasicTests : IUseFixture<Dev.SettingsFixture>, IDisposable
    {
        public BasicTests()
        {
        }

        TweetsContext ents = null;
        string keyspaceName = "Tweets" + Guid.NewGuid().ToString("N");

        public void SetFixture(Dev.SettingsFixture setFix)
        {
            var connectionString = setFix.Settings["CassandraConnectionString"];
            
            try
            {
                ents = new TweetsContext(connectionString, keyspaceName);
            }
            catch (Exception ex)
            {
                using(ents = new TweetsContext(connectionString, null))
                    ents.CreateKeyspaceIfNotExists(keyspaceName);
                ents = new TweetsContext(connectionString, keyspaceName);
            }

            ents.CreateTablesIfNotExist();
        }

        public void Dispose()
        {
            ents.DeleteKeyspaceIfExists(keyspaceName);
            ents.Dispose();
        }

        class X
        {
            public string x;
            public int y;
        }

        [Fact]
        public void Test1()
        {
            var table = ents.GetTable<Tweets>();
        
            int RowsNo = 2000;
            List<Tweets> entL = new List<Tweets>();
            for (int i = 0; i < RowsNo; i++)
            {
                var ent = new Tweets() { tweet_id = Guid.NewGuid(), author = "test" + i.ToString(), body = "body" + i.ToString(), isok = (i % 2 == 0) };
                table.AddNew(ent, CqlEntityTrackingMode.KeepAtachedAfterSave);
                entL.Add(ent);                
            }
            ents.SaveChanges(CqlSaveChangesMode.Batch);
            
            var cnt = table.Count().Execute();
            Dev.Assert.Equal(RowsNo,cnt);
            
            foreach(var ent in entL)
                table.Delete(ent);
            
            ents.SaveChanges(CqlSaveChangesMode.Batch);
            
            var cnt2 = table.Count().Execute();
            Dev.Assert.Equal(0, cnt2);
        }


        [Fact]
        public void Test2()
        {
            var table = ents.GetTable<Tweets>();
            int RowsNb = 3000;
            
            for( int i = 0; i < RowsNb; i++)            
                table.AddNew( new Tweets(){ tweet_id = Guid.NewGuid(), Key = i, isok = i%2 == 0, author = "author" + i.ToString(), body = "bla bla bla"});            

            var evens = (from ent in table where ent.isok == true select ent).Execute();
            Assert.True(evens.All(ev => ev.Key % 2 == 0));
        }



        //[Fact]
        public void TestBuffering()
        {           
            var table = ents.GetTable<Tweets>();           

            var q2 = (from e in table where table.Token(e.Key) <= 0 select e).Take(10).OrderBy((e) => e.Key).ThenByDescending((e) => e.isok);

            var qxx = q2.ToString();
            int RowsNb = 10;
            for (int i = 0; i < RowsNb; i++)
            {
                var ent = new Tweets() { tweet_id = Guid.NewGuid(), author = "author" + i.ToString(), isok =i%2 == 0, body = "blablabla", Key = i};
                table.AddNew(ent, CqlEntityTrackingMode.KeepAtachedAfterSave);
            }
            var ent2 = new Tweets() { tweet_id = Guid.NewGuid(), author = "author" + RowsNb+1, isok = false, body = "blablabla", Key = RowsNb + 1 };
            table.AddNew(ent2, CqlEntityTrackingMode.KeepAtachedAfterSave);
            ents.SaveChanges(CqlSaveChangesMode.OneByOne);

            table.Attach(ent2, CqlEntityUpdateMode.ModifiedOnly);
            ent2.author = "Koko";
            ents.SaveChanges(CqlSaveChangesMode.OneByOne);


            var iq = table.Count();
            var c = iq.Execute();


            foreach (var r in (from e in table where e.Key == 0 select e).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where new int[] { 0, 1, 2 }.Contains(e.Key) select new { x = e.Key, y = e.tweet_id }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.Key == 0 select new { e.Key }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.Key == 0 select new { e.Key, e.isok }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.Key == 0 select new X() { x = e.author, y = e.Key }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.Key == 0 select e.author).Execute())
            {
                var x = r;
            }
        }
    }
}
