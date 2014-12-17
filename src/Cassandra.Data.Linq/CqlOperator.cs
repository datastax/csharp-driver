using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data.Linq
{
    public sealed class CqlOperator
    {
        /// <summary>
        /// Represents the CQL add assign (+=) operator for collections
        /// </summary>
        public static T Append<T>(T value) where T: IEnumerable
        {
            return default(T);   
        }

        /// <summary>
        /// Represents the CQL prepend operator for collections (col1 = ? + col1)
        /// </summary>
        public static T Prepend<T>(T value) where T: IEnumerable
        {
            return default(T);
        }

        /// <summary>
        /// Represents the CQL remove item operator for collections (col1 = col1 - ?)
        /// </summary>
        public static T SubstractAssign<T>(T value) where T: IEnumerable
        {
            return default(T);
        }
    }
}
