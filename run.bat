@echo off
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot
set HADOOP_HOME=C:\hadoop
set PATH=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;%PATH%
cd /d "C:\Users\Poorvi Purohit\Desktop\6th sem\RTBDA\urban_env_risk"

echo ============================================================
echo   Urban Environmental Risk Intelligence System
echo ============================================================
echo.
echo [1/4] Running batch layer for all 26 cities...
echo       (This takes 1-2 minutes, please wait)
echo.

py -3.11 batch_layer/batch_processing.py

echo.
echo [2/4] Starting stream simulator...
start "=== STREAM SIMULATOR ===" cmd /k "set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot && set HADOOP_HOME=C:\hadoop && set PATH=%%JAVA_HOME%%\bin;%%HADOOP_HOME%%\bin;%%PATH%% && cd /d "C:\Users\Poorvi Purohit\Desktop\6th sem\RTBDA\urban_env_risk" && py -3.11 data/stream_simulator.py"

timeout /t 5 /nobreak >nul

echo [3/4] Starting speed layer...
start "=== SPEED LAYER ===" cmd /k "set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot && set HADOOP_HOME=C:\hadoop && set PATH=%%JAVA_HOME%%\bin;%%HADOOP_HOME%%\bin;%%PATH%% && cd /d "C:\Users\Poorvi Purohit\Desktop\6th sem\RTBDA\urban_env_risk" && py -3.11 speed_layer/speed_processing.py"

timeout /t 3 /nobreak >nul

echo [4/4] Starting serving layer...
start "=== SERVING LAYER ===" cmd /k "cd /d "C:\Users\Poorvi Purohit\Desktop\6th sem\RTBDA\urban_env_risk" && py -3.11 serving_layer/app.py"

echo.
echo ============================================================
echo   All components started!
echo   Open browser: http://localhost:5000
echo ============================================================
pause
