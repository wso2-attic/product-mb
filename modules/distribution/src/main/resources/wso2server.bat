@echo off

REM ---------------------------------------------------------------------------
REM        Copyright 2005-2009 WSO2, Inc. http://www.wso2.org
REM
REM  Licensed under the Apache License, Version 2.0 (the "License");
REM  you may not use this file except in compliance with the License.
REM  You may obtain a copy of the License at
REM
REM      http://www.apache.org/licenses/LICENSE-2.0
REM
REM  Unless required by applicable law or agreed to in writing, software
REM  distributed under the License is distributed on an "AS IS" BASIS,
REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM  See the License for the specific language governing permissions and
REM  limitations under the License.

rem ---------------------------------------------------------------------------
rem Main Script for WSO2 Carbon
rem
rem Environment Variable Prequisites
rem
rem   CARBON_HOME   Home of CARBON installation. If not set I will  try
rem                   to figure it out.
rem
rem   JAVA_HOME       Must point at your Java Development Kit installation.
rem
rem   JAVA_OPTS       (Optional) Java runtime options used when the commands
rem                   is executed.
rem ---------------------------------------------------------------------------

rem ----- if JAVA_HOME is not set we're not happy ------------------------------
:checkJava

if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
goto checkServer

:noJavaHome
echo "You must set the JAVA_HOME variable before running CARBON."
goto end

rem ----- Only set CARBON_HOME if not already set ----------------------------
:checkServer
rem %~sdp0 is expanded pathname of the current script under NT with spaces in the path removed
if "%CARBON_HOME%"=="" set CARBON_HOME=%~sdp0..
SET curDrive=%cd:~0,1%
SET wsasDrive=%CARBON_HOME:~0,1%
if not "%curDrive%" == "%wsasDrive%" %wsasDrive%:

rem find CARBON_HOME if it does not exist due to either an invalid value passed
rem by the user or the %0 problem on Windows 9x
if not exist "%CARBON_HOME%\bin\version.txt" goto noServerHome

set AXIS2_HOME=%CARBON_HOME%
goto updateClasspath

:noServerHome
echo CARBON_HOME is set incorrectly or CARBON could not be located. Please set CARBON_HOME.
goto end

rem ----- update classpath -----------------------------------------------------
:updateClasspath

setlocal EnableDelayedExpansion
cd %CARBON_HOME%
set CARBON_CLASSPATH=
FOR %%C in ("%CARBON_HOME%\bin\*.jar") DO set CARBON_CLASSPATH=!CARBON_CLASSPATH!;".\bin\%%~nC%%~xC"

set CARBON_CLASSPATH="%JAVA_HOME%\lib\tools.jar";%CARBON_CLASSPATH%;

FOR %%D in ("%CARBON_HOME%\lib\commons-lang*.jar") DO set CARBON_CLASSPATH=!CARBON_CLASSPATH!;".\lib\%%~nD%%~xD"

rem ----- Process the input command -------------------------------------------

rem Slurp the command line arguments. This loop allows for an unlimited number
rem of arguments (up to the command line limit, anyway).

rem ----- set default cassandra startup mode ----------------------------------
set DISABLE_CASSANDRA_STARTUP=true

rem ----- set default setting of zookeeper ------------------------------------
SETLOCAL EnableExtensions
SET ZOO_PROPKEY=start_zk_server
SET ZOO_PROPVAL=false
SET ZOO_FILE=%CARBON_HOME%\repository\conf\etc\zoo.cfg
SET JAVAENV_PROPKEY=CLIENT_JVMFLAGS
SET JAASCONF_PATH=%CARBON_HOME%\repository\conf\security\jaas.conf
SET JAASCONF_PATH=%JAASCONF_PATH:\=\\%
SET JAVAENV_PROPVAL="\"-Djava.security.auth.login.config=%JAASCONF_PATH%\""
SET JAVAENV_FILE=%CARBON_HOME%\repository\conf\security\java.env
FINDSTR /B %ZOO_PROPKEY% %ZOO_FILE% >nul
IF %ERRORLEVEL% EQU 1 GOTO noworkzoo
MOVE /Y "%ZOO_FILE%" "%ZOO_FILE%.bak">nul
FOR /F "USEBACKQ tokens=*" %%A IN (`TYPE "%ZOO_FILE%.bak" ^|FIND /N /I "%ZOO_PROPKEY%"`) DO (
  SET LINE=%%A
)
FOR /F "tokens=1,2* delims=]" %%S in ("%LINE%") DO SET LINE=%%S
SET /A LINE=%LINE:~1,6%
SET /A COUNT=1
FOR /F "USEBACKQ tokens=*" %%A IN (`FIND /V "" ^<"%ZOO_FILE%.bak"`) DO (
  IF "!COUNT!" NEQ "%LINE%" (
      ECHO %%A>>"%ZOO_FILE%"
  ) ELSE (
      ECHO %ZOO_PROPKEY%=%ZOO_PROPVAL%>>"%ZOO_FILE%"
rem      ECHO Updated %ZOO_FILE% with value %ZOO_PROPKEY%=%ZOO_PROPVAL%
  )
  SET /A COUNT+=1
)
FINDSTR /B %JAVAENV_PROPKEY% %JAVAENV_FILE% >nul
IF %ERRORLEVEL% EQU 1 GOTO noworkjavaenv
MOVE /Y "%JAVAENV_FILE%" "%JAVAENV_FILE%.bak">nul
FOR /F "USEBACKQ tokens=*" %%A IN (`TYPE "%JAVAENV_FILE%.bak" ^|FIND /N /I "%JAVAENV_PROPKEY%"`) DO (
  SET LINE=%%A
)
FOR /F "tokens=1,2* delims=]" %%S in ("%LINE%") DO SET LINE=%%S
SET /A LINE=%LINE:~1,6%
SET /A COUNT=1
FOR /F "USEBACKQ tokens=*" %%A IN (`FIND /V "" ^<"%JAVAENV_FILE%.bak"`) DO (
  IF "!COUNT!" NEQ "%LINE%" (
      ECHO %%A>>"%JAVAENV_FILE%"
  ) ELSE (
      ECHO %JAVAENV_PROPKEY%=%JAVAENV_PROPVAL%>>"%JAVAENV_FILE%"
rem      ECHO Updated %JAVAENV_FILE% with value %JAVAENV_PROPKEY%=%JAVAENV_PROPVAL%
  )
  SET /A COUNT+=1
)
GOTO end
:noworkzoo
echo Didn't find matching string %ZOO_PROPKEY% in %ZOO_FILE%. No work to do.
:noworkjavaenv
echo Didn't find matching string %JAVAENV_PROPKEY% in %JAVAENV_FILE%. No work to do.
pause
:end

:setupArgs
if ""%1""=="""" goto doneStart

if ""%1""==""-run""     goto commandLifecycle
if ""%1""==""--run""    goto commandLifecycle
if ""%1""==""run""      goto commandLifecycle

if ""%1""==""-restart""  goto commandLifecycle
if ""%1""==""--restart"" goto commandLifecycle
if ""%1""==""restart""   goto commandLifecycle

if ""%1""==""debug""    goto commandDebug
if ""%1""==""-debug""   goto commandDebug
if ""%1""==""--debug""  goto commandDebug

if ""%1""==""version""   goto commandVersion
if ""%1""==""-version""  goto commandVersion
if ""%1""==""--version"" goto commandVersion

if ""%1""==""cassandra"" goto commandCassandraProfile

if ""%1""==""zookeeper"" goto commandZookeeperProfile

shift
goto setupArgs

rem ----- commandCassandraProfile -------------------------------------------------------

:commandCassandraProfile
echo Starting WSO2 MessageBroker - Profile Cassandra
set DISABLE_CASSANDRA_STARTUP=false
goto findJdk

rem ----- commandZookeeperProfile -------------------------------------------------------

:commandZookeeperProfile
echo Starting WSO2 MessageBroker - Profile ZooKeeper
SETLOCAL EnableExtensions
SET ZOO_PROPKEY=start_zk_server
SET ZOO_PROPVAL=true
SET ZOO_FILE=%CARBON_HOME%\repository\conf\etc\zoo.cfg
SET JAVAENV_PROPKEY=SERVER_JVMFLAGS
SET JAASCONF_PATH=%CARBON_HOME%\repository\conf\security\jaas.conf
SET JAASCONF_PATH=%JAASCONF_PATH:\=\\%
SET JAVAENV_PROPVAL="\"-Djava.security.auth.login.config=%JAASCONF_PATH%\""
SET JAVAENV_FILE=%CARBON_HOME%\repository\conf\etc\java.env
FINDSTR /B %ZOO_PROPKEY% %ZOO_FILE% >nul
IF %ERRORLEVEL% EQU 1 GOTO noworkzoo
MOVE /Y "%ZOO_FILE%" "%ZOO_FILE%.bak">nul
FOR /F "USEBACKQ tokens=*" %%A IN (`TYPE "%ZOO_FILE%.bak" ^|FIND /N /I "%ZOO_PROPKEY%"`) DO (
  SET LINE=%%A
)
FOR /F "tokens=1,2* delims=]" %%S in ("%LINE%") DO SET LINE=%%S
SET /A LINE=%LINE:~1,6%
SET /A COUNT=1
FOR /F "USEBACKQ tokens=*" %%A IN (`FIND /V "" ^<"%ZOO_FILE%.bak"`) DO (
  IF "!COUNT!" NEQ "%LINE%" (
      ECHO %%A>>"%ZOO_FILE%"
  ) ELSE (
      ECHO %ZOO_PROPKEY%=%ZOO_PROPVAL%>>"%ZOO_FILE%"
rem      ECHO Updated %ZOO_FILE% with value %ZOO_PROPKEY%=%ZOO_PROPVAL%
  )
  SET /A COUNT+=1
)
FINDSTR /B %JAVAENV_PROPKEY% %JAVAENV_FILE% >nul
IF %ERRORLEVEL% EQU 1 GOTO noworkjavaenv
MOVE /Y "%JAVAENV_FILE%" "%JAVAENV_FILE%.bak">nul
FOR /F "USEBACKQ tokens=*" %%A IN (`TYPE "%JAVAENV_FILE%.bak" ^|FIND /N /I "%JAVAENV_PROPKEY%"`) DO (
  SET LINE=%%A
)
FOR /F "tokens=1,2* delims=]" %%S in ("%LINE%") DO SET LINE=%%S
SET /A LINE=%LINE:~1,6%
SET /A COUNT=1
FOR /F "USEBACKQ tokens=*" %%A IN (`FIND /V "" ^<"%JAVAENV_FILE%.bak"`) DO (
  IF "!COUNT!" NEQ "%LINE%" (
      ECHO %%A>>"%JAVAENV_FILE%"
  ) ELSE (
      ECHO %JAVAENV_PROPKEY%=%JAVAENV_PROPVAL%>>"%JAVAENV_FILE%"
rem      ECHO Updated %JAVAENV_FILE% with value %JAVAENV_PROPKEY%=%JAVAENV_PROPVAL%
  )
  SET /A COUNT+=1
)
GOTO end
:noworkzoo
echo Didn't find matching string %ZOO_PROPKEY% in %ZOO_FILE%. No work to do.
:noworkjavaenv
echo Didn't find matching string %JAVAENV_PROPKEY% in %JAVAENV_FILE%. No work to do.
pause
:end
goto findJdk

rem ----- commandVersion -------------------------------------------------------
:commandVersion
shift
type "%CARBON_HOME%\bin\version.txt"
type "%CARBON_HOME%\bin\wso2carbon-version.txt"
goto end

rem ----- commandDebug ---------------------------------------------------------
:commandDebug
shift
set DEBUG_PORT=%1
if "%DEBUG_PORT%"=="" goto noDebugPort
if not "%JAVA_OPTS%"=="" echo Warning !!!. User specified JAVA_OPTS will be ignored, once you give the --debug option.
set JAVA_OPTS=-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=%DEBUG_PORT%
echo Please start the remote debugging client to continue...
goto findJdk

:noDebugPort
echo Please specify the debug port after the --debug option
goto end


:doneStart
if "%OS%"=="Windows_NT" @setlocal
if "%OS%"=="WINNT" @setlocal

rem ---------- Handle the SSL Issue with proper JDK version --------------------
rem find the version of the jdk
:findJdk

set CMD=RUN %*

:checkJdk16
"%JAVA_HOME%\bin\java" -version 2>&1 | findstr /r "1.[6|7]" >NUL
IF ERRORLEVEL 1 goto unknownJdk
goto jdk16

:unknownJdk
echo Starting WSO2 Carbon (in unsupported JDK)
echo [ERROR] CARBON is supported only on JDK 1.6 and 1.7
goto jdk16

:jdk16
goto runServer

rem ----------------- Execute The Requested Command ----------------------------

:runServer
cd %CARBON_HOME%

rem ---------- Add jars to classpath ----------------

set CARBON_CLASSPATH=.\lib;%CARBON_CLASSPATH%

set JAVA_ENDORSED=".\lib\endorsed";"%JAVA_HOME%\jre\lib\endorsed";"%JAVA_HOME%\lib\endorsed"

set CMD_LINE_ARGS=-Xbootclasspath/a:%CARBON_XBOOTCLASSPATH% -Xms1024m -Xmx1024m -Xmn300m -XX:MaxPermSize=256m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:SurvivorRatio=8               -XX:MaxTenuringThreshold=1 -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:ParallelGCThreads=8 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="%CARBON_HOME%\repository\logs\heap-dump.hprof"  -Dcom.sun.management.jmxremote -javaagent:%CARBON_HOME%\repository\components\plugins\jamm_0.2.5.wso2v2.jar -classpath %CARBON_CLASSPATH% %JAVA_OPTS% -Djava.endorsed.dirs=%JAVA_ENDORSED% -DandesConfig=andes-config.xml -Ddisable.cassandra.server.startup=%DISABLE_CASSANDRA_STARTUP% -Dcarbon.registry.root=/ -Dcarbon.home="%CARBON_HOME%" -Dwso2.server.standalone=true -Djava.command="%JAVA_HOME%\bin\java" -Djava.opts="%JAVA_OPTS%" -Djava.io.tmpdir="%CARBON_HOME%\tmp" -Dcatalina.base="%CARBON_HOME%\lib\tomcat" -Dwso2.carbon.xml=%CARBON_HOME%\repository\conf\carbon.xml -Dwso2.registry.xml="%CARBON_HOME%\repository\conf\registry.xml" -Dwso2.user.mgt.xml="%CARBON_HOME%\repository\conf\user-mgt.xml" -Dwso2.transports.xml="%CARBON_HOME%\repository\conf\mgt-transports.xml" -Djava.util.logging.config.file="%CARBON_HOME%\repository\conf\log4j.properties" -Dcarbon.config.dir.path="%CARBON_HOME%\repository\conf" -Dcarbon.logs.path="%CARBON_HOME%\repository\logs" -Dcomponents.repo="%CARBON_HOME%\repository\components" -Dconf.location="%CARBON_HOME%\repository\conf" -Dcom.atomikos.icatch.file="%CARBON_HOME%\lib\transactions.properties" -Dcom.atomikos.icatch.hide_init_file_path="true" -Dorg.apache.jasper.runtime.BodyContentImpl.LIMIT_BUFFER=true -Dcom.sun.jndi.ldap.connect.pool.authentication=simple -Dcom.sun.jndi.ldap.connect.pool.timeout=3000 -Dorg.terracotta.quartz.skipUpdateCheck=true -Dcarbon.classpath=%CARBON_CLASSPATH% -Dfile.encoding=UTF8 -Dzookeeper.jmx.log4j.disable=true

:runJava
echo JAVA_HOME environment variable is set to %JAVA_HOME%
echo CARBON_HOME environment variable is set to %CARBON_HOME%
"%JAVA_HOME%\bin\java" %CMD_LINE_ARGS% org.wso2.carbon.bootstrap.Bootstrap %CMD%
if "%ERRORLEVEL%"=="121" goto runJava
:end
goto endlocal

:endlocal

:END
