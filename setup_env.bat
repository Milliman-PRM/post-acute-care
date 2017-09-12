rem ### CODE OWNERS: Kyle Baird
rem
rem ### OBJECTIVE:
rem   Configure environment for use so it can be utilized by multiple systems
rem
rem ### DEVELOPER NOTES:
rem   <none>

rem ### LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Setting up post-acute-care env
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Running from %~f0

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling last promoted pipeline_components_env.bat
call "S:\PRM\Pipeline_Components_Env\pipeline_components_env.bat"

echo.
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Redirecting POST_ACUTE_CARE_HOME to local copy
SET POST_ACUTE_CARE_HOME=%~dp0%
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: POST_ACUTE_CARE_HOME is now %POST_ACUTE_CARE_HOME%

echo.
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Prepending local copy of python library to PYTHONPATH
set PYTHONPATH=%POST_ACUTE_CARE_HOME%python;%PYTHONPATH%
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: PYTHONPATH is now %PYTHONPATH%

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Finished setting up post-acute-care env
