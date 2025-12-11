@echo off
REM --- SETTINGS ---
SET CONDA_PATH=D:\anaconda
SET ENV_NAME=premsql
SET APP_FILE=app.py
SET FLASK_HOST=127.0.0.1
SET FLASK_PORT=5000

REM --- 1. START DATABASE AND EXTERNAL API SERVICES (CUSTOMIZE THESE LINES) ---

echo.
echo Attempting to start MySQL Server...
REM !!! IMPORTANT: REPLACE THE LINE BELOW with your specific MySQL startup command !!!
REM --------------------------------------------------------------------------------
REM Example 1 (MySQL Service): net start MySQL80
REM Example 2 (XAMPP/WAMP): call "C:\xampp\mysql_start.bat"
REM Example 3 (Direct MySQLd path): "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqld" --console
REM --------------------------------------------------------------------------------
REM Replace the placeholder with one of the above, or your specific command.
REM If MySQL is already running as a service, comment out or remove the line below.
REM net start MySQL80
echo MySQL startup command skipped/placeholder used. Ensure MySQL is running manually.
timeout /t 2 /nobreak >nul


echo.
echo Attempting to start External Text-to-SQL REST API (e.g., a local PremSQL instance)...
REM !!! OPTIONAL: If you host your own PremSQL API, include the command here !!!
REM start "External API" /B python your_api_server.py
echo External API startup skipped/placeholder used.
timeout /t 2 /nobreak >nul


REM --- 2. ACTIVATE CONDA ENVIRONMENT ---
echo.
echo Activating Conda environment: %ENV_NAME%
call "%CONDA_PATH%\Scripts\activate.bat" %ENV_NAME%
IF ERRORLEVEL 1 (
    echo.
    echo ERROR: Failed to activate Conda environment. Check CONDA_PATH and ENV_NAME.
    pause
    EXIT /B 1
)


REM --- 3. START FLASK APPLICATION ---
echo.
echo Starting Flask Text-to-SQL Agent on http://%FLASK_HOST%:%FLASK_PORT%/
REM Use /B to run the Python process in the background of this window
start "Flask Server" /B python %APP_FILE%

REM Give the server a few seconds to start up
timeout /t 5 /nobreak >nul

REM --- 4. LAUNCH WEB PAGE IN BROWSER ---
echo.
echo Launching web page...
start http://%FLASK_HOST%:%FLASK_PORT%/

REM --- 5. KEEP WINDOW OPEN ---
echo.
echo Flask server is running.
echo CHECK THIS WINDOW FOR ERRORS.
echo CLOSE THIS WINDOW to stop the Flask server.
pause