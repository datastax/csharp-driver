git pull
call "C:\Program Files (x86)\Microsoft Visual Studio 11.0\VC\vcvarsall.bat"
msbuild Cassandra.MyTest.sln /t:Clean /p:Configuration=Release
msbuild Cassandra.MyTest.sln /p:Configuration=Release
mkdir "Nuget\lib"
copy Cassandra\bin\Release\Cassandra.dll Nuget\lib\Cassandra.dll
copy Cassandra.Data\bin\Release\Cassandra.Data.dll Nuget\lib\Cassandra.Data.dll
copy Cassandra.Data.Linq\bin\Release\Cassandra.Data.Linq.dll Nuget\lib\Cassandra.Data.Linq.dll
copy Cassandra.DSE\bin\Release\Cassandra.DSE.dll Nuget\lib\Cassandra.DSE.dll

copy Cassandra\bin\Release\Cassandra.xml Nuget\lib\Cassandra.xml
copy Cassandra.Data\bin\Release\Cassandra.Data.xml Nuget\lib\Cassandra.Data.xml
copy Cassandra.Data.Linq\bin\Release\Cassandra.Data.Linq.xml Nuget\lib\Cassandra.Data.Linq.xml
copy Cassandra.DSE\bin\Release\Cassandra.DSE.xml Nuget\lib\Cassandra.DSE.xml
"tools\nuget" pack "nuget\CassandraCSharpDriver.nuspec"
rem rmdir \S \Q "Nuget\lib"
rem msbuild Documentation.sln \p:Configuration=Release
