import subprocess

def run_script(script):
    result = subprocess.run(['python', script])
    if result.returncode != 0:
        print(f'Error occurred while running {script}')
        return False
    return True

# 运行UpdateDatabaseV4
if not run_script('UpdateDatabaseV4.py'):
    exit(1)

# 运行SelectionTest
if not run_script('SelectionTest.py'):
    exit(1)

print("所有程序成功运行")
