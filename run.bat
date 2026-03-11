@echo off
REM Pipeline launcher – always uses C:\Python313\python.exe
REM Usage:  run.bat [any main.py args]
REM  e.g.:  run.bat --store metro --city karachi --scrape-only
REM         run.bat --summary
C:\Python313\python.exe "%~dp0main.py" %*
