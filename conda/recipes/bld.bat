maturin build -vv -j %CPU_COUNT% --release --strip --manylinux off --interpreter=%PYTHON%

FOR /F "delims=" %%i IN ('dir /s /b target\wheels\*.whl') DO set datafusion_wheel=%%i

%PYTHON% -m pip install --no-deps %datafusion_wheel% -vv

cargo-bundle-licenses --format yaml --output THIRDPARTY.yml
