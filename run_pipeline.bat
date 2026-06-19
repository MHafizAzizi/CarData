@echo off
REM ===========================================================================
REM CarData pipeline: scrape (all makes) -> migrate CSV->DB -> clean/normalize.
REM Mirrors the Mudah Rent Analysis run_pipeline flow.
REM
REM Double-click to run (numbered category prompt). Or pass category to skip:
REM     run_pipeline.bat cars
REM     run_pipeline.bat motorcycles
REM     run_pipeline.bat both
REM
REM Recheck (recheck.py) is a separate daily cadence and is NOT run here.
REM Schema must be v9 first: python migrations\run_migrations.py --category both
REM Both categories clean with --enrich-types: unmapped (make, model) pairs
REM print at the end and need a row added to the category's mapping CSV
REM (data\reference\motorcycles_model_types.csv / cars_model_types.csv).
REM Cars only fall back to the CSV when the API car_type is junk
REM ('4 Wheels'/'Others'), so unmapped car pairs are rare.
REM ===========================================================================
cd /d "%~dp0"

set "CAT=%~1"
if not "%CAT%"=="" goto :resolve

echo.
echo Select category:
echo   1. cars
echo   2. motorcycles
echo   3. both
echo.
set /p "SEL=Enter 1, 2, or 3 [default: 3]: "
if "%SEL%"=="" set "SEL=3"
if "%SEL%"=="1" set "CAT=cars"
if "%SEL%"=="2" set "CAT=motorcycles"
if "%SEL%"=="3" set "CAT=both"
if "%CAT%"=="" (
    echo Invalid selection "%SEL%". Enter 1, 2, or 3.
    goto :err
)

:resolve
if /i "%CAT%"=="both" (
    call :run cars       || goto :err
    call :run motorcycles || goto :err
) else if /i "%CAT%"=="cars" (
    call :run cars       || goto :err
) else if /i "%CAT%"=="motorcycles" (
    call :run motorcycles || goto :err
) else (
    echo Unknown category "%CAT%". Use cars, motorcycles, or both.
    goto :err
)

echo.
echo Pipeline complete.
pause
exit /b 0

:run
set "C=%~1"
echo.
echo ==================================================
echo   %C% : STEP 1/3 scrape
echo ==================================================
python "src\1_scrape.py" --category %C% --all-makes --smart || exit /b 1
echo.
echo ==================================================
echo   %C% : STEP 2/3 migrate
echo ==================================================
python "src\2_migrate.py" --category %C% || exit /b 1
echo.
echo ==================================================
echo   %C% : STEP 3/3 clean
echo ==================================================
if /i "%C%"=="cars" (
    python "src\3_clean.py" --category %C% --enrich-variants --enrich-types || exit /b 1
) else (
    python "src\3_clean.py" --category %C% --enrich-types || exit /b 1
)
exit /b 0

:err
echo.
echo *** Pipeline FAILED. See the step above. ***
pause
exit /b 1
