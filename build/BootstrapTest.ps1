<#
	Sets Up the runtime environment so that integration tests can be launched successfully.
#>

# import dependencies
. ./TestUtils.ps1
. ./ComponentUtils.ps1
. ./EnvGlobals.ps1

AntBootstrap
NUnitBootstrap
