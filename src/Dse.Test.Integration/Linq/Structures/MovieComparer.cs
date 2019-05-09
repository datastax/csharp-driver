// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cassandra.IntegrationTests.Linq.Structures
{
    public class MovieComparer : IComparer, IComparer<Movie>
    {
        public int Compare(Movie x, Movie y)
        {
            if (x == null || y == null)
            {
                if (x == null && y == null)
                {
                    return 0;
                }

                return -1;
            }

            if (x.Director != y.Director)
            {
                return -1;
            }

            if (x.MovieMaker != y.MovieMaker)
            {
                return -1;
            }

            if (x.Title != y.Title)
            {
                return -1;
            }

            if (x.Year != y.Year)
            {
                return -1;
            }

            if (x.MainActor != y.MainActor)
            {
                return -1;
            }

            var intersectCount = x.ExampleSet.Intersect(y.ExampleSet).Count();
            if (intersectCount != x.ExampleSet.Count || intersectCount != y.ExampleSet.Count)
            {
                return -1;
            }

            return 0;
        }

        public int Compare(object x, object y)
        {
            return Compare((Movie)x, (Movie)y);
        }
    }
}