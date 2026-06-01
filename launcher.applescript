set appDir to "/Users/kartikeya/Tech/maa_app"
set activateScript to appDir & "/.venv/bin/activate"
set appScript to appDir & "/app.py"
set appPort to "8501"

-- Check if already running on the port
set serverRunning to false
try
	do shell script "lsof -i :" & appPort & " | grep LISTEN"
	set serverRunning to true
end try

-- Create venv and install deps if missing
set venvDir to appDir & "/.venv"
set venvExists to false
try
	do shell script "test -f " & quoted form of activateScript
	set venvExists to true
end try

if not venvExists then
	display dialog "First-time setup: creating virtual environment and installing dependencies. This may take a minute." buttons {"OK"} default button "OK" with title "MAA App Setup"
	try
		do shell script "/bin/bash -c 'python3 -m venv " & quoted form of venvDir & " && source " & quoted form of activateScript & " && pip install -r " & quoted form of (appDir & "/requirements.txt") & " > /tmp/maa_app_setup.log 2>&1'"
	on error errMsg
		display alert "Setup failed" message errMsg & return & "Check /tmp/maa_app_setup.log for details." as critical
		return
	end try
end if

-- Start if not running
if not serverRunning then
	do shell script "/bin/bash -c 'source " & quoted form of activateScript & " && streamlit run " & quoted form of appScript & " > /tmp/maa_app.log 2>&1 &'"

	-- Poll until ready (up to 20 seconds)
	set ready to false
	repeat 20 times
		delay 1
		try
			do shell script "lsof -i :" & appPort & " | grep LISTEN"
			set ready to true
			exit repeat
		end try
	end repeat

	if not ready then
		display alert "MAA App failed to start" message "Check /tmp/maa_app.log for details." as critical
		return
	end if
end if

open location "http://localhost:" & appPort

set choice to button returned of (display dialog "MAA App is running." & return & "URL: http://localhost:" & appPort buttons {"Stop Server", "Keep Running"} default button "Keep Running" with title "MAA App")

if choice is "Stop Server" then
	try
		do shell script "kill $(lsof -ti :" & appPort & ") 2>/dev/null"
		display notification "MAA App server stopped." with title "MAA App"
	end try
end if
