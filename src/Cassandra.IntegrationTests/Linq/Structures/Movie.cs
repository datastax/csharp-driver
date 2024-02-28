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
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using NUnit.Framework.Internal;

#pragma warning disable 618

namespace Cassandra.IntegrationTests.Linq.Structures
{
    [AllowFiltering]
    [Table(Movie.TableName)]
    public class Movie
    {
        public const string TableName = "coolMovies";

        [Column("mainGuy")]
        public string MainActor;

        [PartitionKey(2)]
        [Column("movie_maker")]
        public string MovieMaker;

        [PartitionKey(1)]
        [Column("unique_movie_title")]
        public string Title;

        [Column("list")]
        public List<string> ExampleSet = new List<string>();

        [ClusteringKey(1)]
        [Column("director")]
        public string Director { get; set; }

        [Column("yearMade")]
        public int? Year { get; set; }

        public Movie()
        {

        }

        public Movie(string title, string director, string mainActor, string movieMaker, int year)
        {
            Title = title;
            Director = director;
            MainActor = mainActor;
            MovieMaker = movieMaker;
            Year = year;
        }

        public static void AssertEquals(Movie expectedMovie, Movie actualMovie)
        {
            Assert.AreEqual(expectedMovie.MainActor, actualMovie.MainActor);
            Assert.AreEqual(expectedMovie.MovieMaker, actualMovie.MovieMaker);
            Assert.AreEqual(expectedMovie.Title, actualMovie.Title);
            Assert.AreEqual(expectedMovie.ExampleSet, actualMovie.ExampleSet);
            Assert.AreEqual(expectedMovie.Director, actualMovie.Director);
            Assert.AreEqual(expectedMovie.Year, actualMovie.Year);
        }

        public static void AssertListContains(List<Movie> expectedMovies, Movie actualMovie)
        {
            Assert.IsTrue(ListContains(expectedMovies, actualMovie));
        }

        public static bool ListContains(List<Movie> expectedMovies, Movie actualMovie)
        {
            foreach (var expectedMovie in expectedMovies)
            {
                using (new TestExecutionContext.IsolatedContext())
                {
                    try
                    {
                        AssertEquals(actualMovie, expectedMovie);
                        return true;
                    }
                    catch (AssertionException)
                    {
                    }
                }
            }
            return false;
        }

        public static Movie GetRandomMovie()
        {
            Movie movie = new Movie
            {
                Title = "SomeMovieTitle_" + Randomm.RandomAlphaNum(10),
                Director = "SomeMovieDirector_" + Randomm.RandomAlphaNum(10),
                MainActor = "SomeMainActor_" + Randomm.RandomAlphaNum(10),
                MovieMaker = "SomeFilmMaker_" + Randomm.RandomAlphaNum(10),
                Year = Randomm.RandomInt(),
            };
            return movie;
        }

        public static List<Movie> GetDefaultMovieList()
        {
            List<Movie> movieList = new List<Movie>();
            movieList.Add(new Movie("title3", "actor1", "director3", "maker3", 1988));
            movieList.Add(new Movie("title5", "actor1", "director4", "maker5", 1988));
            movieList.Add(new Movie("title1", "actor1", "director1", "maker1", 1988));
            movieList.Add(new Movie("title2", "actor2", "director2", "maker2", 1988));
            movieList.Add(new Movie("title4", "actor2", "director4", "maker4", 1988));
            return movieList;
        }
        
        private static readonly IDictionary<string, Func<Movie, object>> ColumnMappings =
            new Dictionary<string, Func<Movie, object>>
            {
                { "director", entity => entity.Director },
                { "list", entity => entity.ExampleSet },
                { "mainGuy", entity => entity.MainActor },
                { "movie_maker", entity => entity.MovieMaker },
                { "unique_movie_title", entity => entity.Title },
                { "yearMade", entity => entity.Year }
            };

        private static readonly IDictionary<string, DataType> ColumnToDataTypes =
            new Dictionary<string, DataType>
            {
                { "director", DataType.GetDataType(typeof(string)) },
                { "list", DataType.GetDataType(typeof(List<string>)) },
                { "mainGuy", DataType.GetDataType(typeof(string)) },
                { "movie_maker", DataType.GetDataType(typeof(string)) },
                { "unique_movie_title", DataType.GetDataType(typeof(string)) },
                { "yearMade", DataType.GetDataType(typeof(int?)) },
                { "[applied]", DataType.GetDataType(typeof(bool)) }
            };

        public static RowsResult GetEmptyRowsResult()
        {
            return new RowsResult(Movie.ColumnMappings.Keys.ToArray());
        }
        
        public static RowsResult GetEmptyAppliedInfoRowsResult()
        {
            return new RowsResult(ColumnToDataTypes.Select(kvp => (kvp.Key, kvp.Value)).ToArray());
        }

        public static RowsResult CreateAppliedInfoRowsResultWithoutMovie(bool applied)
        {
            var result = new RowsResult("[applied]");
            return (RowsResult) result.WithRow(applied);
        }

        public RowsResult CreateAppliedInfoRowsResult()
        {
            return AddAppliedInfoRow(Movie.GetEmptyAppliedInfoRowsResult());
        }
        
        public RowsResult CreateRowsResult()
        {
            return AddRow(Movie.GetEmptyRowsResult());
        }
        
        public RowsResult AddRow(RowsResult result)
        {
            return (RowsResult) result.WithRow(GetParameters());
        }

        public static (string, DataType)[] GetColumns()
        {
            return Movie.ColumnToDataTypes.Select(kvp => (kvp.Key, kvp.Value)).ToArray();
        }

        public (DataType, object)[] GetParametersWithTypes(bool withNulls = true)
        {
            var parameters = Movie.ColumnMappings.Values.Select(func => func(this)).Zip(ColumnToDataTypes, (obj, v2) => (v2.Value, obj));
            if (!withNulls)
            {
                parameters = parameters.Where(o => o.obj != null);
            }

            return parameters.ToArray();
        }

        public object[] GetParameters(bool withNulls = true)
        {
            var parameters = Movie.ColumnMappings.Values.Select(func => func(this));
            if (!withNulls)
            {
                parameters = parameters.Where(o => o != null);
            }

            return parameters.ToArray();
        }

        public static RowsResult CreateRowsResult(IEnumerable<Movie> data)
        {
            return data.Aggregate(Movie.GetEmptyRowsResult(), (current, c) => c.AddRow(current));
        }
        
        public RowsResult AddAppliedInfoRow(RowsResult result)
        {
            return (RowsResult) result.WithRow(
                Movie.ColumnMappings.Values.Select(func => func(this)).Concat(new object [] { false }).ToArray());
        }
    }
}
