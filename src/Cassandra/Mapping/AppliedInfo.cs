using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Mapping
{
    /// <summary>
    /// When using Lightweight transactions, it provides information whether the change was applied or not.
    /// </summary>
    public class AppliedInfo<T>
    {
        /// <summary>
        /// Determines if the change was applied.
        /// </summary>
        public bool Applied { get; set; }

        /// <summary>
        /// Gets or sets the existing data that prevented
        /// </summary>
        public T Existing { get; set; }

        /// <summary>
        /// Creates a new instance marking the change as applied 
        /// </summary>
        public AppliedInfo(bool applied)
        {
            Applied = applied;
        }

        /// <summary>
        /// Creates a new instance marking the change as not applied and provides information about the existing data.
        /// </summary>
        /// <param name="existing"></param>
        public AppliedInfo(T existing)
        {
            Applied = false;
            Existing = existing;
        }
    }
}
