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

      if (env.SERVER_VERSION.split('-')[1] == '6.0') {
        powershell label: 'Update environment for DataStax Enterprise v6.0.x', script: '''
        . $Env:HOME\\driver-environment.ps1

        echo "Setting DSE 6.0 install-dir"
        "`r`n`$Env:DSE_PATH=`"$Env:CCM_INSTALL_DIR`"" | Out-File -filepath $Env:HOME\\driver-environment.ps1 -append
        '''
      }
    }
    
    // sni tests disabled on windows
    /*
    if (env.SERVER_VERSION == env.SERVER_VERSION_SNI_WINDOWS) {
      powershell label: 'Update environment for SNI proxy tests', script: '''
        $newData = "`r`n`$Env:SNI_ENABLED=`"true`""
        $newData += "`r`n`$Env:SINGLE_ENDPOINT_PATH=`"$Env:HOME/proxy/run.ps1`""
        $newData += "`r`n`$Env:SNI_CERTIFICATE_PATH=`"$Env:HOME/proxy/certs/client_key.pfx`""
        $newData += "`r`n`$Env:SNI_CA_PATH=`"$Env:HOME/proxy/certs/root.crt`""

        "$newData" | Out-File -filepath $Env:HOME\\driver-environment.ps1 -append
      '''
    }
    */

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
      sh label: 'Update environment for DataStax Enterprise', script: '''#!/bin/bash -le
        # Load CCM environment variables
        set -o allexport
        . ${HOME}/environment.txt
        set +o allexport

        cat >> ${HOME}/environment.txt << ENVIRONMENT_EOF
CCM_PATH=${HOME}/ccm
DSE_BRANCH=${CCM_BRANCH}
DSE_INITIAL_IPPREFIX=127.0.0.
DSE_IN_REMOTE_SERVER=false
ENVIRONMENT_EOF
      '''

      if (env.SERVER_VERSION.split('-')[1] == '6.0') {
        sh label: 'Update environment for DataStax Enterprise v6.0.x', script: '''#!/bin/bash -le
          # Load CCM and driver configuration environment variables
          set -o allexport
          . ${HOME}/environment.txt
          set +o allexport

          echo "DSE_PATH=${CCM_INSTALL_DIR}" >> ${HOME}/environment.txt
        '''
      }
    }

    if (env.SERVER_VERSION == env.SERVER_VERSION_SNI) {
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
      '''
  } else {
    if (env.DOTNET_VERSION == 'mono') {
      sh label: 'Build the driver for mono', script: '''#!/bin/bash -le
        msbuild /t:restore /v:m src/Cassandra.sln
        msbuild /p:Configuration=Release /v:m /p:DynamicConstants=LINUX src/Cassandra.sln
      '''
    } else {
      sh label: "Work around nuget issue", script: '''#!/bin/bash -le
        mkdir -p /tmp/NuGetScratch
        chmod -R ugo+rwx /tmp/NuGetScratch
      '''
      sh label: "Install required packages and build the driver for ${env.DOTNET_VERSION}", script: '''#!/bin/bash -le
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

          mono ./testrunner/NUnit.ConsoleRunner.3.6.1/tools/nunit3-console.exe src/Cassandra.IntegrationTests/bin/Release/net452/Cassandra.IntegrationTests.dll --where "$MONO_TEST_FILTER" --labels=All --result:"TestResult_nunit.xml"
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

def notifySlack(status = 'started') {
  // Set the global pipeline scoped environment (this is above each matrix)
  env.BUILD_STATED_SLACK_NOTIFIED = 'true'

  def buildType = 'Commit'
  if (params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') {
    buildType = "${params.CI_SCHEDULE.toLowerCase().capitalize()}"
  }

  def color = 'good' // Green
  if (status.equalsIgnoreCase('aborted')) {
    color = '808080' // Grey
  } else if (status.equalsIgnoreCase('unstable')) {
    color = 'warning' // Orange
  } else if (status.equalsIgnoreCase('failed')) {
    color = 'danger' // Red
  }

  def message = """Build ${status} for ${env.DRIVER_DISPLAY_NAME} [${buildType}]
<${env.GITHUB_BRANCH_URL}|${env.BRANCH_NAME}> - <${env.RUN_DISPLAY_URL}|#${env.BUILD_NUMBER}> - <${env.GITHUB_COMMIT_URL}|${env.GIT_SHA}>"""
  if (params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') {
    message += " - ${params.CI_SCHEDULE_DOTNET_VERSION} - ${env.OS_VERSION}"
  }
  if (!status.equalsIgnoreCase('Started')) {
    message += """
${status} after ${currentBuild.durationString - ' and counting'}"""
  }

  slackSend color: "${color}",
            channel: "#csharp-driver-dev-bots",
            message: "${message}"
}

def submitCIMetrics(buildType) {
  long durationMs = currentBuild.duration
  long durationSec = durationMs / 1000
  long nowSec = (currentBuild.startTimeInMillis + durationMs) / 1000
  def branchNameNoPeriods = env.BRANCH_NAME.replaceAll('\\.', '_')
  def durationMetric = "okr.ci.csharp.${env.DRIVER_METRIC_TYPE}.${buildType}.${branchNameNoPeriods} ${durationSec} ${nowSec}"

  timeout(time: 1, unit: 'MINUTES') {
    withCredentials([string(credentialsId: 'lab-grafana-address', variable: 'LAB_GRAFANA_ADDRESS'),
                     string(credentialsId: 'lab-grafana-port', variable: 'LAB_GRAFANA_PORT')]) {
      withEnv(["DURATION_METRIC=${durationMetric}"]) {
        sh label: 'Send runtime metrics to labgrafana', script: '''#!/bin/bash -le
          echo "${DURATION_METRIC}" | nc -q 5 ${LAB_GRAFANA_ADDRESS} ${LAB_GRAFANA_PORT}
        '''
      }
    }
  }
}

def describePerCommitStage() {
  script {
    currentBuild.displayName = "Per-Commit"
    currentBuild.description = '''Per-Commit build and testing of against mono, netcoreapp2.0, and netcoreapp2.1 (Linux only)
<ul>
  <li>mono/netcoreapp2.0 - Apache Cassandara&reg; v3.11.x and DataStax Enterprise v6.7.x</li>
  <li>netcoreapp2.1 - Apache Cassandara&reg; v2.2.x, v3.11.x and DataStax Enterprise v5.1.x, v6.7.x</li>
</ul>'''
  }
}

def describeScheduledTestingStage() {
  script {
    def type = params.CI_SCHEDULE.toLowerCase().capitalize()
    currentBuild.displayName = "${type} schedule (${env.DOTNET_VERSION} | ${env.OS_VERSION})"

    def serverVersionDescription = "${params.CI_SCHEDULE_SERVER_VERSION.replaceAll(' ', ', ')} server version(s) in the matrix"
    def osVersionDescription = 'Ubuntu 18.04 LTS'
    if (env.OS_VERSION == 'win/cs') {
      osVersionDescription = 'Windows 10'
    }
    currentBuild.description = "${type} scheduled testing for, ${serverVersionDescription} against ${env.DOTNET_VERSION} .NET version on ${osVersionDescription}"
  }
}

def describeAdhocTestingStage() {
  script {
    def serverType = params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION.split('-')[0]
    def serverDisplayName = 'Apache Cassandara&reg;'
    def serverVersion = " v${serverType}"
    if (serverType == 'ALL') {
      serverDisplayName = "all ${serverDisplayName} and DataStax Enterprise server versions"
      serverVersion = ''
    } else {
      try {
        serverVersion = " v${env.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION.split('-')[1]}"
      } catch (e) {
        ;; // no-op
      }
      if (serverType == 'ddac') {
        serverDisplayName = "DataStax Distribution of ${serverDisplayName}"
      } else if (serverType == 'dse') {
        serverDisplayName = 'DataStax Enterprise'
      }
    }

    def netVersionInformation = ".NET ${params.ADHOC_BUILD_AND_EXECUTE_TESTS_DOTNET_VERSION}"
    if (params.ADHOC_BUILD_AND_EXECUTE_TESTS_NODEJS_VERSION == 'ALL') {
      netVersionInformation = 'all .NET versions'
    }

    def osInformation = 'Ubuntu 18.04 LTS'
    if (params.ADHOC_BUILD_AND_EXECUTE_TESTS_OS_VERSION.split('/')[0] == 'win') {
      osInformation = 'Windows 10'
    }

    currentBuild.displayName = "${params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION} against ${netVersionInformation} on ${osInformation}"
    currentBuild.description = "Testing ${serverDisplayName} ${serverVersion} against ${netVersionInformation} on ${osInformation}"
  }
}

// branch pattern for cron
def branchPatternCron = ~"((\\d+(\\.[\\dx]+)+)|master)"

pipeline {
  agent none

  // Global pipeline timeout
  options {
    disableConcurrentBuilds()
    timeout(time: 10, unit: 'HOURS')
    buildDiscarder(logRotator(artifactNumToKeepStr: '10', // Keep only the last 10 artifacts
                              numToKeepStr: '50'))        // Keep only the last 50 build records
  }

  parameters {
    choice(
      name: 'ADHOC_BUILD_TYPE',
      choices: ['BUILD', 'BUILD-AND-EXECUTE-TESTS'],
      description: '''<p>Perform a adhoc build operation</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>BUILD</strong></td>
                          <td>Performs a <b>Per-Commit</b> build</td>
                        </tr>
                        <tr>
                          <td><strong>BUILD-AND-EXECUTE-TESTS</strong></td>
                          <td>Performs a build and executes the integration and unit tests</td>
                        </tr>
                      </table>''')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_DOTNET_VERSION',
      choices: ['mono', 'netcoreapp2.0', 'netcoreapp2.1', 'net452', 'net461'],
      description: '.NET version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION',
      choices: ['2.1',       // Legacy Apache Cassandara�
                '2.2',       // Legacy Apache Cassandara�
                '3.0',       // Previous Apache Cassandara�
                '3.11',      // Current Apache Cassandara�
                '4.0',       // Development Apache Cassandara�
                'dse-5.1',   // Legacy DataStax Enterprise
                'dse-6.0',   // Previous DataStax Enterprise
                'dse-6.7',   // Current DataStax Enterprise
                'dse-6.8', // Development DataStax Enterprise
                'dse-6.8.0', // Current DataStax Enterprise
                'ALL'],
      description: '''Apache Cassandara&reg; and DataStax Enterprise server version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>
                      <table style="width:100%">
                        <col width="15%">
                        <col width="85%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>2.1</strong></td>
                          <td>Apache Cassandara&reg; v2.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>2.2</strong></td>
                          <td>Apache Cassandara&reg; v2.2.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.0</strong></td>
                          <td>Apache Cassandara&reg; v3.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.11</strong></td>
                          <td>Apache Cassandara&reg; v3.11.x</td>
                        </tr>
                        <tr>
                          <td><strong>4.0</strong></td>
                          <td>Apache Cassandara&reg; v4.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-5.1</strong></td>
                          <td>DataStax Enterprise v5.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.0</strong></td>
                          <td>DataStax Enterprise v6.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.7</strong></td>
                          <td>DataStax Enterprise v6.7.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.8</strong></td>
                          <td>DataStax Enterprise v6.8.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.8.0</strong></td>
                          <td>DataStax Enterprise v6.8.0 </td>
                        </tr>
                      </table>''')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_OS_VERSION',
      choices: ['ubuntu/bionic64/csharp-driver', 'win/cs'],
      description: '''<p>Operating system to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong></p>
                      <table style="width:100%">
                        <col width="20%">
                        <col width="80%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>ubuntu/bionic64/csharp-driver</strong></td>
                          <td>Ubuntu 18.04 LTS x86_64</td>
                        </tr>
                        <tr>
                          <td><strong>win/cs</strong></td>
                          <td>Windows 10</td>
                        </tr>
                      </table>''')
    choice(
      name: 'CI_SCHEDULE',
      choices: ['DO-NOT-CHANGE-THIS-SELECTION', 'WEEKNIGHTS', 'WEEKENDS'],
      description: 'CI testing schedule to execute periodically scheduled builds and tests of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    choice(
      name: 'CI_SCHEDULE_DOTNET_VERSION',
      choices: ['DO-NOT-CHANGE-THIS-SELECTION', 'mono', 'netcoreapp2.0', 'netcoreapp2.1', 'net452', 'net461'],
      description: 'CI testing .NET version to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    string(
      name: 'CI_SCHEDULE_SERVER_VERSION',
      defaultValue: 'DO-NOT-CHANGE-THIS-SELECTION',
      description: 'CI testing server version to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    choice(
      name: 'CI_SCHEDULE_OS_VERSION',
      choices: ['DO-NOT-CHANGE-THIS-SELECTION', 'ubuntu/bionic64/csharp-driver', 'win/cs'],
      description: 'CI testing operating system version to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
  }

  triggers {
    parameterizedCron(branchPatternCron.matcher("master").matches() ? """
      # Every weeknight (Monday - Friday) around 12:00 and 1:00 AM
      ##
      # Building on Linux
      #   - Do not build using net452 and net461
      #   - Target all Apache Cassandara� and DataStax Enterprise versions for netcoreapp2.1
      ##
      H/45 * * * * %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_DOTNET_VERSION=mono;CI_SCHEDULE_SERVER_VERSION=2.2 3.11 dse-5.1 dse-6.7;CI_SCHEDULE_OS_VERSION=ubuntu/bionic64/csharp-driver
      H 0 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_DOTNET_VERSION=mono;CI_SCHEDULE_SERVER_VERSION=2.2 3.11 dse-5.1 dse-6.7;CI_SCHEDULE_OS_VERSION=ubuntu/bionic64/csharp-driver
      H 1 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_DOTNET_VERSION=netcoreapp2.1;CI_SCHEDULE_SERVER_VERSION=ALL;CI_SCHEDULE_OS_VERSION=ubuntu/bionic64/csharp-driver
      ##
      # Building on Windows
      #   - Do not build using mono
      #   - Target Apache Cassandara� v3.11.x for netcoreapp2.1
      #   - Target Apache Cassandara� v2.1.x, v2.2.x, v3.11.x for net452
      #   - Target Apache Cassandara� v2.2.x, v3.11.x for net461
      ##
      H 2 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_DOTNET_VERSION=netcoreapp2.1;CI_SCHEDULE_SERVER_VERSION=3.11;CI_SCHEDULE_OS_VERSION=win/cs
      H 2 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_DOTNET_VERSION=net452;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.11;CI_SCHEDULE_OS_VERSION=win/cs
      H 2 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_DOTNET_VERSION=net461;CI_SCHEDULE_SERVER_VERSION=2.2 3.11;CI_SCHEDULE_OS_VERSION=win/cs

      # Every Saturday around 4:00 and 8:00 AM
      ##
      # Building on Linux
      #   - Do not build using net452 and net461
      #   - Target all Apache Cassandara� and DataStax Enterprise versions for mono and netcoreapp2.1
      ##
      H 4 * * 6 %CI_SCHEDULE=WEEKENDS;CI_SCHEDULE_DOTNET_VERSION=mono;CI_SCHEDULE_SERVER_VERSION=ALL;CI_SCHEDULE_OS_VERSION=ubuntu/bionic64/csharp-driver
      H 4 * * 6 %CI_SCHEDULE=WEEKENDS;CI_SCHEDULE_DOTNET_VERSION=netcoreapp2.1;CI_SCHEDULE_SERVER_VERSION=ALL;CI_SCHEDULE_OS_VERSION=ubuntu/bionic64/csharp-driver
      # Building on Windows
      #   - Do not build using mono
      #   - Target all Apache Cassandara� versions for netcoreapp2.1, net452 and net461
      ##
      H 8 * * 6 %CI_SCHEDULE=WEEKENDS;CI_SCHEDULE_DOTNET_VERSION=netcoreapp2.1;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 4.0;CI_SCHEDULE_OS_VERSION=win/cs
      H 8 * * 6 %CI_SCHEDULE=WEEKENDS;CI_SCHEDULE_DOTNET_VERSION=net452;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 4.0;CI_SCHEDULE_OS_VERSION=win/cs
      H 8 * * 6 %CI_SCHEDULE=WEEKENDS;CI_SCHEDULE_DOTNET_VERSION=net461;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 4.0;CI_SCHEDULE_OS_VERSION=win/cs
    """ : "")
  }

  environment {
    DOTNET_CLI_TELEMETRY_OPTOUT = '1'
    SERVER_VERSION_SNI = 'dse-6.7'
    SERVER_VERSION_SNI_WINDOWS = '3.11'
    SIMULACRON_PATH = '/home/jenkins/simulacron.jar'
    SIMULACRON_PATH_WINDOWS = 'C:\\Users\\Admin\\simulacron.jar'
    CCM_ENVIRONMENT_SHELL = '/usr/local/bin/ccm_environment.sh'
    CCM_ENVIRONMENT_SHELL_WINDOWS = '/mnt/c/Users/Admin/ccm_environment.sh'
  }

  stages {
    stage('Per-Commit') {
      when {
        beforeAgent true
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
          expression { params.CI_SCHEDULE == 'DO-NOT-CHANGE-THIS-SELECTION' }
          not { buildingTag() }
        }
      }

      environment {
        OS_VERSION = 'ubuntu/bionic64/csharp-driver'
      }

      matrix {
        axes {
          axis {
            name 'SERVER_VERSION'
            values '2.2',     // latest 2.2.x Apache Cassandara�
                   '3.0',     // latest 3.0.x Apache Cassandara�
                   '3.11',    // latest 3.11.x Apache Cassandara�
                   'dse-5.1', // latest 5.1.x DataStax Enterprise
                   'dse-6.7', // latest 6.7.x DataStax Enterprise
                   'dse-6.8.0' // 6.8.0 current DataStax Enterprise
          }
          axis {
            name 'DOTNET_VERSION'
            values 'mono', 'netcoreapp2.1'
          }
        }
        excludes {
          exclude {
            axis {
              name 'DOTNET_VERSION'
              values 'mono'
            }
            axis {
              name 'SERVER_VERSION'
              values '2.2', '3.0', 'dse-5.1', 'dse-6.8.0'
            }
          }
        }

        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describePerCommitStage()
            }
          }
          stage('Install-Dependencies') {
            steps {
              installDependencies()
            }
          }
          stage('Build-Driver') {
            steps {
              buildDriver()
            }
          }
          stage('Execute-Tests') {
            steps {
              executeTests(true)
            }
            post {
              always {
                junit testResults: '**/TestResult.xml'
              }
            }
          }
        }
      }
      post {
        always {
          node('master') {
            submitCIMetrics('commit')
          }
        }
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }

    stage('Scheduled-Testing') {
      when {
        beforeAgent true
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
          expression { params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION' }
          not { buildingTag() }
        }
      }

      environment {
        DOTNET_VERSION = "${params.CI_SCHEDULE_DOTNET_VERSION}"
        OS_VERSION = "${params.CI_SCHEDULE_OS_VERSION}"
      }

      matrix {
        axes {
          axis {
            name 'SERVER_VERSION'
            values '2.1',     // Legacy Apache Cassandara�
                   '2.2',     // Legacy Apache Cassandara�
                   '3.0',     // Previous Apache Cassandara�
                   '3.11',    // Current Apache Cassandara�
                   '4.0',     // Development Apache Cassandara�
                   'dse-5.1', // Legacy DataStax Enterprise
                   'dse-6.0', // Previous DataStax Enterprise
                   'dse-6.7', // Current DataStax Enterprise
                   'dse-6.8',  // Development DataStax Enterprise
                   'dse-6.8.0'  // Current DataStax Enterprise
          }
        }
        when {
          beforeAgent true
          allOf {
            expression { return params.CI_SCHEDULE_SERVER_VERSION.split(' ').any { it =~ /(ALL|${env.SERVER_VERSION})/ } }
          }
        }

        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describeScheduledTestingStage()
            }
          }
          stage('Install-Dependencies') {
            steps {
              installDependencies()
            }
          }
          stage('Build-Driver') {
            steps {
              buildDriver()
            }
          }
          stage('Execute-Tests') {
            steps {
              executeTests(false)
            }
            post {
              always {
                junit testResults: '**/TestResult.xml'
              }
            }
          }
        }
      }
      post {
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }

    stage('Adhoc-Testing') {
      when {
        beforeAgent true
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD-AND-EXECUTE-TESTS' }
          expression { (params.ADHOC_BUILD_AND_EXECUTE_TESTS_OS_VERSION == 'ubuntu/bionic64/csharp-driver' &&
                        params.ADHOC_BUILD_AND_EXECUTE_TESTS_DOTNET_VERSION != 'net452' &&
                        params.ADHOC_BUILD_AND_EXECUTE_TESTS_DOTNET_VERSION != 'net461') ||
                       (params.ADHOC_BUILD_AND_EXECUTE_TESTS_OS_VERSION == 'win/cs' &&
                        params.ADHOC_BUILD_AND_EXECUTE_TESTS_DOTNET_VERSION != 'mono') }
          not { buildingTag() }
        }
      }

        environment {
          DOTNET_VERSION = "${ADHOC_BUILD_AND_EXECUTE_TESTS_DOTNET_VERSION}"
          OS_VERSION = "${params.ADHOC_BUILD_AND_EXECUTE_TESTS_OS_VERSION}"
        }

      matrix {
        axes {
          axis {
            name 'SERVER_VERSION'
            values '2.1',     // Legacy Apache Cassandara�
                   '2.2',     // Legacy Apache Cassandara�
                   '3.0',     // Previous Apache Cassandara�
                   '3.11',    // Current Apache Cassandara�
                   '4.0',     // Development Apache Cassandara�
                   'dse-5.1', // Legacy DataStax Enterprise
                   'dse-6.0', // Previous DataStax Enterprise
                   'dse-6.7', // Current DataStax Enterprise
                   'dse-6.8'  // Development DataStax Enterprise
          }
        }
        when {
          beforeAgent true
          allOf {
            expression { params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION ==~ /(ALL|${env.SERVER_VERSION})/ }
          }
        }

        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Describe-Build') {
            steps {
              describeAdhocTestingStage()
            }
          }
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
            }
          }
          stage('Install-Dependencies') {
            steps {
              installDependencies()
            }
          }
          stage('Build-Driver') {
            steps {
              buildDriver()
            }
          }
          stage('Execute-Tests') {
            steps {
              executeTests(false)
            }
            post {
              always {
                junit testResults: '**/TestResult.xml'
              }
            }
          }
        }
      }
    }
  }
}
