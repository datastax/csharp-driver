//
//  Copyright (c) 2014, Kevin Thompson
//  All rights reserved.
//  
//  Redistribution and use in source and binary forms, with or without
//  modification, are permitted provided that the following conditions are met:
//  
//  1. Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer. 
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//  
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
//  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

using System;
using System.Reflection;

namespace Cassandra.DataStax.Auth.Sspi
{
    [AttributeUsage( AttributeTargets.Field )]
    internal class EnumStringAttribute : Attribute
    {
        public EnumStringAttribute( string text )
        {
            this.Text = text;
        }

        public string Text { get; private set; }
    }

    internal class EnumMgr
    {
        public static string ToText( Enum value )
        {
            FieldInfo field = value.GetType().GetField( value.ToString() );

            if (field == null)
            {
                return null;
            }

            EnumStringAttribute[] attribs = (EnumStringAttribute[])field.GetCustomAttributes( typeof( EnumStringAttribute ), false );

            if( attribs == null || attribs.Length == 0 )
            {
                return null;
            }
            else
            {
                return attribs[0].Text;
            }
        }

        public static T FromText<T>( string text )
        {
            FieldInfo[] fields = typeof( T ).GetFields();

            EnumStringAttribute[] attribs;

            foreach( FieldInfo field in fields )
            {
                attribs = (EnumStringAttribute[])field.GetCustomAttributes( typeof( EnumStringAttribute ), false );

                foreach( EnumStringAttribute attrib in attribs )
                {
                    if( attrib.Text == text )
                    {
                        return (T)field.GetValue( null );
                    }
                }
            }

            throw new ArgumentException( "Could not find a matching enumeration value for the text '" + text + "'." );
        }
    }
}
