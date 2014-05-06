@echo off
%WINDIR%\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe build.proj /v:m %*

REM package
REM %WINDIR%\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe build.proj /v:m /t:package /p:BUILD_NUMBER=YYY /p:PACKAGE_VERSION=2.0.0-beta2