#!groovy

def initializeEnvironment() {
  env.DRIVER_DISPLAY_NAME = 'Cassandra C# Driver'
  env.DRIVER_METRIC_TYPE = 'oss'
  if (env.GIT_URL.contains('riptano/csharp-driver')) {
    env.DRIVER_DISPLAY_NAME = 'private ' + env.DRIVER_DISPLAY_NAME
    env.DRIVER_METRIC_TYPE = 'oss-private'
  } else if (env.GIT_URL.contains('csharp-dse-driver')) {
    env.DRIVER_DISPLAY_NAME = 'DSE C# Driver'
    env.DRIVER_METRIC_TYPE = 'dse'
  }

  env.GIT_SHA = "${env.GIT_COMMIT.take(7)}"
  env.GITHUB_PROJECT_URL = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
  env.GITHUB_BRANCH_URL = "${GITHUB_PROJECT_URL}/tree/${env.BRANCH_NAME}"
  env.GITHUB_COMMIT_URL = "${GITHUB_PROJECT_URL}/commit/${env.GIT_COMMIT}"

  if (env.OS_VERSION.split('/')[0] == 'win') {    
    env.HOME = 'C:\\Users\\Admin'
    env.HOME_WSL = '/mnt/c/Users/Admin'

    powershell label: 'Copy SSL files', script: '''
      wsl bash --login -c "cp -r $env:HOME_WSL/ccm/ssl `$HOME/ssl"
    '''
    
    powershell label: 'Download Apache Cassandra&reg; or DataStax Enterprise', script: '''
      rm $Env:HOME\\environment.txt
      rm $Env:HOME\\driver-environment.ps1

      wsl bash --login -c "$Env:CCM_ENVIRONMENT_SHELL_WINDOWS $Env:SERVER_VERSION"
      wsl bash --login -c "cp ~/environment.txt $ENV:HOME_WSL"
      
      $data = get-content "$Env:HOME\\environment.txt"
      $data = $data -replace "`n","`r`n"
      $newData = ""
	    $data | foreach {
          $v1,$v2 = $_.split("=",2)
          echo "1: $v1 2: $v2"
          $newData += "`r`n`$Env:$v1='$v2'"
      }
      $newData += "`r`n`$Env:CASSANDRA_VERSION=`$Env:CCM_CASSANDRA_VERSION"
      "$newData" | Out-File -filepath $Env:HOME\\driver-environment.ps1
    '''
    
    if (env.SERVER_VERSION.split('-')[0] == 'dse') {
      powershell label: 'Update environment for DataStax Enterprise', script: '''
          . $Env:HOME\\driver-environment.ps1

          $newData = "`r`n`$Env:DSE_BRANCH=`"$Env:CCM_BRANCH`""
          $newData += "`r`n`$Env:DSE_VERSION=`"$Env:CCM_VERSION`""
          $newData += "`r`n`$Env:DSE_INITIAL_IPPREFIX=`"127.0.0.`""
          $newData += "`r`n`$Env:DSE_IN_REMOTE_SERVER=`"false`""

          "$newData" | Out-File -filepath $Env:HOME\\driver-environment.ps1 -append
      '''
    }
    
    if (env.SERVER_VERSION == env.SERVER_VERSION_SNI_WINDOWS) {
      powershell label: 'Update environment for SNI proxy tests', script: '''
        $newData = "`r`n`$Env:SNI_ENABLED=`"true`""
        $newData += "`r`n`$Env:SINGLE_ENDPOINT_PATH=`"$Env:HOME/proxy/run.ps1`""
        $newData += "`r`n`$Env:SNI_CERTIFICATE_PATH=`"$Env:HOME/proxy/certs/client_key.pfx`""
        $newData += "`r`n`$Env:SNI_CA_PATH=`"$Env:HOME/proxy/certs/root.crt`""

        "$newData" | Out-File -filepath $Env:HOME\\driver-environment.ps1 -append
      '''
    }

    powershell label: 'Set additional environment variables for windows tests', script: '''
      $newData = "`r`n`$Env:PATH+=`";$env:JAVA_HOME\\bin`""
      $newData += "`r`n`$Env:SIMULACRON_PATH=`"$Env:SIMULACRON_PATH_WINDOWS`""
      $newData += "`r`n`$Env:CCM_USE_WSL=`"true`""
      $newData += "`r`n`$Env:CCM_SSL_PATH=`"/root/ssl`""

      "$newData" | Out-File -filepath $Env:HOME\\driver-environment.ps1 -append
    '''

    powershell label: 'Display .NET and environment information', script: '''
      # Load CCM and driver configuration environment variables
      cat $Env:HOME\\driver-environment.ps1
      . $Env:HOME\\driver-environment.ps1

      dotnet --version

      gci env:* | sort-object name
    '''
  } else {
    sh label: 'Copy SSL files', script: '''#!/bin/bash -le
      cp -r ${HOME}/ccm/ssl $HOME/ssl
    '''

    sh label: 'Download Apache Cassandra&reg; or DataStax Enterprise', script: '''#!/bin/bash -le
      . ${CCM_ENVIRONMENT_SHELL} ${SERVER_VERSION}

      echo "CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION}" >> ${HOME}/environment.txt
    '''
    
    if (env.SERVER_VERSION.split('-')[0] == 'dse') {
      env.DSE_FIXED_VERSION = env.SERVER_VERSION.split('-')[1]
      sh label: 'Update environment for DataStax Enterprise', script: '''#!/bin/bash -le
        rm ${HOME}/.ccm/config
        cat > ${HOME}/.ccm/config << CONF_EOL
[repositories]
cassandra = https://repo.aws.dsinternal.org/artifactory/apache-mirror/cassandra
dse = http://repo-public.aws.dsinternal.org/tar/enterprise/dse-%s-bin.tar.gz
ddac = http://repo-public.aws.dsinternal.org/tar/enterprise/ddac-%s-bin.tar.gz
CONF_EOL

        cat >> ${HOME}/environment.txt << ENVIRONMENT_EOF
CCM_PATH=${HOME}/ccm
DSE_INITIAL_IPPREFIX=127.0.0.
DSE_IN_REMOTE_SERVER=false
CCM_CASSANDRA_VERSION=${DSE_FIXED_VERSION} # maintain for backwards compatibility
CCM_VERSION=${DSE_FIXED_VERSION}
CCM_SERVER_TYPE=dse
DSE_VERSION=${DSE_FIXED_VERSION}
CCM_IS_DSE=true
CCM_BRANCH=${DSE_FIXED_VERSION}
DSE_BRANCH=${DSE_FIXED_VERSION}
JDK=1.8
ENVIRONMENT_EOF
      '''
    }

    if (env.SERVER_VERSION == env.SERVER_VERSION_SNI && env.DOTNET_VERSION != 'mono') {
      sh label: 'Update environment for SNI proxy tests', script: '''#!/bin/bash -le
        # Load CCM and driver configuration environment variables
        set -o allexport
        . ${HOME}/environment.txt
        set +o allexport

        cat >> ${HOME}/environment.txt << ENVIRONMENT_EOF
SNI_ENABLED=true
SINGLE_ENDPOINT_PATH=${HOME}/proxy/run.sh
SNI_CERTIFICATE_PATH=${HOME}/proxy/certs/client_key.pfx
SNI_CA_PATH=${HOME}/proxy/certs/root.crt
ENVIRONMENT_EOF
      '''
    }

    sh label: 'Display .NET and environment information', script: '''#!/bin/bash -le
      # Load CCM and driver configuration environment variables
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      if [ ${DOTNET_VERSION} = 'mono' ]; then
        mono --version
      else
        dotnet --version
      fi
      printenv | sort
    '''
  }
}

def initializeEnvironmentStep() {
  initializeEnvironment()
  if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
    notifySlack()
  }
}

def installDependencies() {
  if (env.OS_VERSION.split('/')[0] == 'win') {
    powershell label: 'Download saxon', script: '''
      [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
      mkdir saxon
      Invoke-WebRequest -OutFile saxon/saxon9he.jar -Uri https://repo1.maven.org/maven2/net/sf/saxon/Saxon-HE/9.8.0-12/Saxon-HE-9.8.0-12.jar
    '''
  } else {
    sh label: 'Download saxon', script: '''#!/bin/bash -le
      mkdir saxon
      curl -L -o saxon/saxon9he.jar https://repo1.maven.org/maven2/net/sf/saxon/Saxon-HE/9.8.0-12/Saxon-HE-9.8.0-12.jar
    '''

    if (env.DOTNET_VERSION == 'mono') {
      sh label: 'Install required packages for mono builds', script: '''#!/bin/bash -le
        # Define alias for Nuget
        nuget() {
          mono /usr/local/bin/nuget.exe "$@"
        }
        export -f nuget

        nuget install NUnit.Runners -Version 3.6.1 -OutputDirectory testrunner
      '''
    }
  }
}

def buildDriver() {
  if (env.OS_VERSION.split('/')[0] == 'win') {
    powershell label: "Install required packages and build the driver for ${env.DOTNET_VERSION}", script: '''
        dotnet restore src
        dotnet restore src
      '''
  } else {
    if (env.DOTNET_VERSION == 'mono') {
      sh label: 'Build the driver for mono', script: '''#!/bin/bash -le
        export BuildMonoOnly=True
        export RunCodeAnalyzers=False
        export MSBuildSDKsPath=/opt/dotnet/sdk/$(dotnet --version)/Sdks
        msbuild /t:restore /v:m /p:RestoreDisableParallel=true src/Cassandra.sln || true
        msbuild /t:restore /v:m /p:RestoreDisableParallel=true src/Cassandra.sln
        msbuild /p:Configuration=Release /v:m /p:RestoreDisableParallel=true /p:DynamicConstants=LINUX src/Cassandra.sln || true
        msbuild /p:Configuration=Release /v:m /p:RestoreDisableParallel=true /p:DynamicConstants=LINUX src/Cassandra.sln
      '''
    } else {
      sh label: "Work around nuget issue", script: '''#!/bin/bash -le
        mkdir -p /tmp/NuGetScratch
        chmod -R ugo+rwx /tmp/NuGetScratch
      '''
      sh label: "Install required packages and build the driver for ${env.DOTNET_VERSION}", script: '''#!/bin/bash -le
        dotnet restore src || true
        dotnet restore src
      '''
    }
  }
}

def executeTests(perCommitSchedule) {
  
  if (perCommitSchedule) {
    env.DOTNET_TEST_FILTER = "(TestCategory!=long)&(TestCategory!=memory)&(TestCategory!=realclusterlong)"
    env.MONO_TEST_FILTER = "cat != long && cat != memory && cat != realclusterlong"
  } else {
    env.DOTNET_TEST_FILTER = "(TestCategory!=long)&(TestCategory!=memory)"
    env.MONO_TEST_FILTER = "cat != long && cat != memory"    
  }  
  
  if (env.OS_VERSION.split('/')[0] == 'win') {
    catchError {
      powershell label: "Execute tests for ${env.DOTNET_VERSION}", script: '''
        . $env:HOME\\Documents\\WindowsPowerShell\\Microsoft.PowerShell_profile.ps1
        . $Env:HOME\\driver-environment.ps1
        dotnet test src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj -v n -f $Env:DOTNET_VERSION -c Release --filter $Env:DOTNET_TEST_FILTER --logger "xunit;LogFilePath=../../TestResult_xunit.xml" -- RunConfiguration.TargetPlatform=x64
      '''
    }
    powershell label: 'Convert the test results using saxon', script: '''
      java -jar saxon/saxon9he.jar -o:TestResult.xml TestResult_xunit.xml tools/JUnitXml.xslt
    '''
  } else {
    if (env.DOTNET_VERSION == 'mono') {
      catchError {
        sh label: 'Execute tests for mono', script: '''#!/bin/bash -le
          # Load CCM and driver configuration environment variables
          set -o allexport
          . ${HOME}/environment.txt
          set +o allexport

          mono ./testrunner/NUnit.ConsoleRunner.3.6.1/tools/nunit3-console.exe src/Cassandra.IntegrationTests/bin/Release/net462/Cassandra.IntegrationTests.dll --where "$MONO_TEST_FILTER" --labels=All --result:"TestResult_nunit.xml"
        '''
      }
      sh label: 'Convert the test results using saxon', script: '''#!/bin/bash -le
        java -jar saxon/saxon9he.jar -o:TestResult.xml TestResult_nunit.xml tools/nunit3-junit.xslt
      '''
    } else {
      catchError {
        sh label: "Execute tests for ${env.DOTNET_VERSION}", script: '''#!/bin/bash -le
          # Load CCM and driver configuration environment variables
          set -o allexport
          . ${HOME}/environment.txt
          set +o allexport

          dotnet test src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj -v n -f ${DOTNET_VERSION} -c Release --filter $DOTNET_TEST_FILTER --logger "xunit;LogFilePath=../../TestResult_xunit.xml"
        '''
      }
      sh label: 'Convert the test results using saxon', script: '''#!/bin/bash -le
        java -jar saxon/saxon9he.jar -o:TestResult.xml TestResult_xunit.xml tools/JUnitXml.xslt
      '''
    } 
  }
}


@NonCPS
def getChangeLog() {
  def log = ""
  def changeLogSets = currentBuild.changeSets
  for (int i = 0; i < changeLogSets.size(); i++) {
      def entries = changeLogSets[i].items
      for (int j = 0; j < entries.length; j++) {
          def entry = entries[j]
          log += "  * ${entry.msg} by ${entry.author} <br>"
      }
  }
  return log;
}

def describePerCommitStage() {
  script {
    currentBuild.displayName = "#${env.BUILD_NUMBER} - Per-Commit (${env.GIT_SHA})"
    currentBuild.description = "Changelog:<br>${getChangeLog()}".take(250)
  }
}

def describeScheduledTestingStage() {
  script {
    def type = params.CI_SCHEDULE.toLowerCase().capitalize()
    def serverVersionDescription = "almost all server version(s) in the matrix"
    def osVersionDescription = 'Ubuntu 18.04 LTS'
    if (env.OS_VERSION == 'win/cs') {
      osVersionDescription = 'Windows 10'
    }    
    currentBuild.displayName = "#${env.BUILD_NUMBER} - ${type} (${osVersionDescription})"
    currentBuild.description = "${type} scheduled testing for ${serverVersionDescription} on ${osVersionDescription}"
  }
}