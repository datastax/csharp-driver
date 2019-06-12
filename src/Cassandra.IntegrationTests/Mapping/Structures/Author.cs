//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.LinqMethods;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Mapping.Structures
{
    /// <summary>
    /// Lower case meta tags used so class can be used easily by Linq and CqlPoco
    /// </summary>
    [Table("author")]
    public class Author
    {
        [PartitionKey]
        [Column("authorid")]
        public string AuthorId;

        [Column("followers")]
        public List<string> Followers;

        public void AssertEquals(Author actualAuthor)
        {
            Assert.AreEqual(AuthorId, actualAuthor.AuthorId);
            Assert.AreEqual(Followers, actualAuthor.Followers);
        }

        public static Author GetRandom(string prefix = "")
        {
            List<string> followers = new List<string>()
            {
                Guid.NewGuid().ToString(),
                Guid.NewGuid().ToString(),
                Guid.NewGuid().ToString(),
                Guid.NewGuid().ToString()
            };
            return new Author()
            {
                AuthorId = prefix + Guid.NewGuid().ToString(),
                Followers = followers,
            };
        }

        public static void AssertListsContainTheSame(List<Author> expectedAuthors, List<Author> actualAuthors)
        {
            foreach (var actualAuthor in actualAuthors)
            {
                AssertListContains(expectedAuthors, actualAuthor);
            }
        }

        public static void AssertListContains(List<Author> actualAuthors, Author expectedAuthor)
        {
            foreach (var actualAuthor in actualAuthors)
            {
                if (actualAuthor.AuthorId == expectedAuthor.AuthorId)
                {
                    expectedAuthor.AssertEquals(actualAuthor);
                    return;
                }
            }
            Assert.Fail("Expected author with id: " + expectedAuthor.AuthorId + " was not found!");
        }

        public static List<Author> GetRandomList(int count)
        {
            List<Author> authors = new List<Author>();
            for(int i=0; i< count; i++)
            {
                Author author = Author.GetRandom(i + "_");
                authors.Add(author);
            }
            return authors;
        }
    }
}
