import subprocess
import time

p1 = subprocess.Popen(["python", "bot.py"])
p2 = subprocess.Popen(["python", "main.py"])

try:
    p1.wait()
    p2.wait()
except KeyboardInterrupt:
    p1.terminate()
    p2.terminate()
