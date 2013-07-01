//
//      Copyright (C) 2012 DataStax Inc.
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
﻿using System.IO;

namespace Cassandra
{
    public partial class RowSet
    {
        public delegate string CellEncoder(object val);                

        

        public void PrintTo(TextWriter stream,
            string delim = "\t|",
            string rowDelim = "\r\n",
            bool printHeader = true,
            bool printFooter = true,
            string separ = "-------------------------------------------------------------------------------",
            string lasLFrm = "Returned {0} rows.",
            CellEncoder cellEncoder = null             
            )
        {
            if (printHeader)
            {
                bool first = true;
                foreach (var column in Columns)
                {
                    if (first) first = false;
                    else
                        stream.Write(delim);

                    stream.Write(column.Name);
                }
                stream.Write(rowDelim);
                stream.Write(separ);
                stream.Write(rowDelim);
            }
            int i = 0;
            foreach (var row in GetRows())
            {
                bool first = true;
                for (int j = 0; j < Columns.Length; j++)
                {
                    if (first) first = false;
                    else
                        stream.Write(delim);

                    if (row[j] is System.Array || (row[j].GetType().IsGenericType && row[j] is System.Collections.IEnumerable))
                        cellEncoder = delegate(object collection)
                        {                            
                            string result = "<Collection>";
                            if(collection.GetType() == typeof(byte[]))
                                result+=CqlQueryTools.ToHex((byte[])collection);
                            else
                                foreach (var val in (collection as System.Collections.IEnumerable))
                                    result += val.ToString() + ",";
                            return result.Substring(0, result.Length - 1) + "</Collection>";
                        };
                    
                    stream.Write(cellEncoder == null ? row[j] : cellEncoder(row[j]));
                }
                stream.Write(rowDelim);
                i++;
            }
            if (printFooter)
            {
                stream.Write(separ);
                stream.Write(rowDelim);
                stream.Write(string.Format(lasLFrm, i));
                stream.Write(rowDelim);
            }            
        }
    }
}
