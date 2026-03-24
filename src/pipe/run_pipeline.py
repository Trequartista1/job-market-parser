import subprocess
import sys

print("🚀 START PIPELINE\n")

print("1. Парсим work.ua...")
subprocess.run([sys.executable, "../../src/parsers/workua_parser.py"])

print("2. Парсим robota.ua...")
subprocess.run([sys.executable, "../../src/parsers/rabotaua_parser.py"])

print("3. Обрабатываем данные...")
subprocess.run([sys.executable, "../../src/processing/df_handling.py"])

print("\n✅ PIPELINE DONE")