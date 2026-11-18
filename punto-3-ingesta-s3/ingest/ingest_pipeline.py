import asyncio
import os
import sys

# ── Pipeline registry ────────────────────────────────────────────────────────
PIPELINES = {
    "1": ("EC2  → S3", "ingest_ec2.py"),
    "2": ("RDS  → S3", "ingest_rds.py"),
    "3": ("API  → S3", "ingest_api.py"),
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON   = sys.executable

# ── Runner ───────────────────────────────────────────────────────────────────
async def run_pipeline(label: str, script: str) -> bool:
    """Run a single ingest script as a subprocess, streaming its output."""
    script_path = os.path.join(BASE_DIR, script)
    print(f"[{label}] starting...")

    proc = await asyncio.create_subprocess_exec(
        PYTHON, script_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await proc.communicate()

    for line in stdout.decode(errors="replace").splitlines():
        print(f"[{label}] {line}")

    if stderr:
        for line in stderr.decode(errors="replace").splitlines():
            print(f"[{label}][ERR] {line}")

    if proc.returncode == 0:
        print(f"[{label}] finished")
    else:
        print(f"[{label}] failed  (exit code {proc.returncode})")

    return proc.returncode == 0

# ── Menu ─────────────────────────────────────────────────────────────────────
def prompt_selection() -> list[str]:
    print("\n╔══════════════════════════════╗")
    print("║   S3 Ingestion Pipeline      ║")
    print("╠══════════════════════════════╣")
    for key, (label, script) in PIPELINES.items():
        print(f"║  {key}  {label:<24}║")
    print("║  all  Run all concurrently   ║")
    print("╚══════════════════════════════╝")
    print("You can also combine, e.g: 1 3  or  1,2")

    raw = input("\nChoice: ").strip().lower()

    if raw == "all":
        return list(PIPELINES.keys())

    # Accept space- or comma-separated keys like "1 3" or "1,2"
    keys = [k.strip() for k in raw.replace(",", " ").split()]
    valid = [k for k in keys if k in PIPELINES]
    invalid = [k for k in keys if k not in PIPELINES]

    if invalid:
        print(f"⚠  Unknown option(s) ignored: {invalid}")
    if not valid:
        print("No valid option selected. Exiting.")
        sys.exit(1)

    return valid

# ── Main ─────────────────────────────────────────────────────────────────────
async def main() -> None:
    selected = prompt_selection()

    print(f"\nRunning: {[PIPELINES[k][0] for k in selected]}\n")

    tasks = [
        run_pipeline(PIPELINES[k][0], PIPELINES[k][1])
        for k in selected
    ]

    results = await asyncio.gather(*tasks)

    passed = sum(results)
    total  = len(results)
    print(f"\n{'─'*40}")
    print(f"Done — {passed}/{total} pipelines succeeded.")

if __name__ == "__main__":
    asyncio.run(main())
