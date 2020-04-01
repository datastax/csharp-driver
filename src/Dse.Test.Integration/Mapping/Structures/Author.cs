//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using Dse.Data.Linq;
using NUnit.Framework;
#pragma warning disable 618

namespace Dse.Test.Integration.Mapping.Structures
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
