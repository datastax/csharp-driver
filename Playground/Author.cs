using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data;
using Cassandra;

namespace Playground
{
    public class Author
    {
        [PartitionKey]
        public string author_id;
        
        public List<string> followers;

        public void displayFollowers()
        {
            if (followers != null)
                foreach (var follower in this.followers)
                    Console.Write(follower + "  ");
            else
                Console.WriteLine("Nobody!");
        }
    }

}
