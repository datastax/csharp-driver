
$env:HOME='C:\Users\Admin'
$env:HOME_WSL='/mnt/c/Users/Admin'

. $env:HOME\Documents\WindowsPowerShell\Microsoft.PowerShell_profile.ps1

wsl bash --login -c "/mnt/c/Users/Admin/ccm_environment.sh $Env:CASSANDRA_VERSION"
wsl bash --login -c "cp ~/environment.txt /mnt/c/Users/Admin"
      
      
$data = get-content "$Env:HOME\environment.txt"
$data = $data -replace "`n","`r`n"
$data | foreach {
    $v1,$v2 = $_.split("=",2)
    echo "1: $v1 2: $v2"
    Set-Item "env:$v1" $v2
}
$env:CASS_VERSION_SNI='dse-6.7'

$env:PATH += ";$env:JAVA_HOME\bin"
$env:SIMULACRON_PATH="$env:HOME\simulacron.jar"
$env:CCM_USE_WSL = "true"
$env:CCM_SSL_PATH = "/root/ssl"

ls $env:HOME

if ( $Env:CASSANDRA_VERSION -Match "dse-*" )
{
    $Env:DSE_BRANCH=$Env:CCM_BRANCH
    $Env:DSE_VERSION=$Env:CCM_VERSION
    $Env:DSE_INITIAL_IPPREFIX="127.0.0."
    $Env:DSE_IN_REMOTE_SERVER="false"
                
    if ( $DSE_VERSION -Match '6.0')
    {
        echo "Setting DSE 6.0 install-dir"
        $Env:DSE_PATH=$Env:CCM_INSTALL_DIR
    }
          
    echo $Env:DSE_VERSION
    echo $Env:DSE_PATH
    echo $Env:DSE_INITIAL_IPPREFIX
    echo $Env:DSE_IN_REMOTE_SERVER
}

echo $Env:CSHARP_VERSION

# Define Cassandra runtime
echo "========== Setting Server Version =========="
$Env:CASSANDRA_VERSION_ORIGINAL=$Env:CASSANDRA_VERSION
$Env:CASSANDRA_VERSION=$Env:CCM_CASSANDRA_VERSION

#echo "========== Copying ssl files to $HOME/ssl =========="
wsl bash --login -c "cp -r $env:HOME_WSL/ccm/ssl `$HOME/ssl"
      
if ( $Env:CASSANDRA_VERSION_ORIGINAL -Match $Env:CASS_VERSION_SNI )
{      
    $Env:SNI_ENABLED="true"
    $Env:SINGLE_ENDPOINT_PATH="$Env:HOME/proxy/run.ps1"
    $Env:SNI_CERTIFICATE_PATH="$Env:HOME/proxy/certs/client_key.pfx"
    $Env:SNI_CA_PATH="$Env:HOME/proxy/certs/root.crt"
}
      
echo $Env:SNI_ENABLED
echo $Env:SINGLE_ENDPOINT_PATH
echo $Env:SNI_CERTIFICATE_PATH
echo $Env:SNI_CA_PATH
      
ls "$Env:HOME/proxy/"
      
mkdir saxon
Invoke-WebRequest -OutFile saxon/saxon9he.jar -Uri https://repo1.maven.org/maven2/net/sf/saxon/Saxon-HE/9.8.0-12/Saxon-HE-9.8.0-12.jar
      
# Install the required packages
dotnet restore src

# Run the tests
dotnet test src/Dse.Test.Integration/Dse.Test.Integration.csproj -v n -f $Env:DOTNET_VERSION -c Release --filter "(TestCategory!=long)&(TestCategory!=memory)" --logger "xunit;LogFilePath=../../TestResult_xunit.xml" -- RunConfiguration.TargetPlatform=x64

$testError=$LASTEXITCODE
      
java -jar saxon/saxon9he.jar -o:TestResult.xml TestResult_xunit.xml tools/JUnitXml.xslt
      
#Fail the build if there was an error
if ( $testError -ne 0 )
{
    exit 1
}