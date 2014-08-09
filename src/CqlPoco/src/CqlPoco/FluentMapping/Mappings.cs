using System;
using CqlPoco.Mapping;
using CqlPoco.Utils;

namespace CqlPoco.FluentMapping
{
    /// <summary>
    /// A class for defining how to map multiple POCOs via a fluent-style interface.  Inheritors should use the 
    /// <see cref="For{TPoco}"/> method inside their constructor to define mappings.
    /// </summary>
    public class Mappings
    {
        internal LookupKeyedCollection<Type, ITypeDefinition> Definitions;

        public Mappings()
        {
            Definitions = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
        }

        public Map<TPoco> For<TPoco>()
        {
            Type pocoType = typeof (TPoco);

            ITypeDefinition map;
            if (Definitions.TryGetItem(typeof (TPoco), out map) == false)
            {
                map = new Map<TPoco>();
                Definitions.Add(map);
            }

            return (Map<TPoco>) map;
        }
    }
}