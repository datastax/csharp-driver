:: dotnet cli has a bug for which it fails to build related projects if the project and package name are different
:: https://github.com/dotnet/cli/issues/3040
:: this batch file is made to workaround that bug
@ECHO OFF &SETLOCAL
SET "file=src\Cassandra\project.json"
SET /a Line#ToSearch=3
SET "Replacement=  "name": "CassandraCSharpDriver","

:: Replace the package name to "CassandraCSharpDriver"
(FOR /f "tokens=1*delims=:" %%a IN ('findstr /n "^" "%file%"') DO (
    SET "Line=%%b"
    IF %%a equ %Line#ToSearch% SET "Line=%Replacement%"
    SETLOCAL ENABLEDELAYEDEXPANSION
    ECHO(!Line!
    ENDLOCAL
))>"%file%.new"
TYPE "%file%.new"
MOVE "%file%.new" "%file%"

:: Pack the driver
dotnet pack src\Cassandra -c Release


:: Replace it back to "Cassandra"
SET "Replacement=  "name": "CassandraCSharpDriver","
(FOR /f "tokens=1*delims=:" %%a IN ('findstr /n "^" "%file%"') DO (
    SET "Line=%%b"
    IF %%a equ %Line#ToSearch% SET "Line=%Replacement%"
    SETLOCAL ENABLEDELAYEDEXPANSION
    ECHO(!Line!
    ENDLOCAL
))>"%file%.new"
TYPE "%file%.new"
MOVE "%file%.new" "%file%"