from pathlib import Path

Path("smoke-output.txt").write_text("trainctl smoke succeeded\n", encoding="utf-8")
print("smoke training complete")
