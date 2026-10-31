# How to test and run Hydra Stream

Everything runs on one Windows laptop. Open **PowerShell** in the project folder
(`C:\Users\LENOVO\Downloads\hydra-stream`). If `go` isn't found, open a new terminal (Go was
installed after older terminals started) or run `$env:Path = "C:\Program Files\Go\bin;$env:Path"`.

| Step | Command | Takes | What "good" looks like |
|---|---|---|---|
| 1. Unit + cluster tests | `go test ./...` | ~1 min | every line starts with `ok` |
| 2. Chaos suite (real processes) | `.\scripts\hydra.ps1 chaos` | ~2–3 min | table with `PASS` on all 6 rows, `0` in LOST / DUPS / ORDER, exit code 0 |
| 3. Live demo + dashboard | `.\scripts\hydra.ps1 demo` | starts in ~10 s | browser opens http://127.0.0.1:8080 with ~6k msg/s and `residency PASS` |
| 4. Residency audit by hand | `.\bin\residency_audit.exe -online` (while the demo runs) | ~2 s | `Verdict: PASS`, all manifests `verified` |

Stop the demo with **Ctrl+C** in its terminal (it stops every broker it started).
If a run was killed abruptly and brokers are left behind: `Get-Process hydra-* | Stop-Process -Force`.

## Things to try on the dashboard (step 3)
1. **Slow link** IN ↔ EU, 150 ms → that edge turns amber (~300 ms) and a dotted purple path shows
   global traffic going *via US*; throughput stays flat. Then **Restore link**.
2. **Isolate region** on EU → both EU links show "cut off". EU's resident stream keeps flowing
   (its data never leaves EU); the global partition EU led is re-elected elsewhere (event log).
   Then **Reconnect**.
3. **Kill region** on IN → IN's resident stream stops with "no ack for N s" (by law it can't fail
   over abroad); EU, US and the global topic keep going; the audit stays PASS.
   Then **Restart region** — IN catches up on its own.
4. **Kill** a single broker (e.g. the one marked *controller*) → a new controller appears in the
   region card and the event log within about a second. Then **Start** it again.
5. **Heal everything** resets all faults.

## Prove the residency audit catches a violation (step 4, demo running)
```powershell
Copy-Item -Recurse data\in-1\topics\in-orders data\eu-2\topics\in-orders   # plant IN data on an EU broker
.\bin\residency_audit.exe -online                                         # FAIL, exit code 1, violations listed
Remove-Item -Recurse data\eu-2\topics\in-orders                           # clean up
.\bin\residency_audit.exe -online                                         # PASS again
```
The dashboard's audit badge also turns red within 5 s while the planted copy exists.

## Smaller pieces
- One scenario only: `.\bin\hydra-chaos.exe -only kill-region` (build first: `go build -o bin/ ./cmd/...`).
- Results of every chaos run: `results\chaos-<time>.md` and `.json`.
- Race detector (needs gcc; MSYS2 is installed here):
  `$env:Path = "C:\msys64\ucrt64\bin;$env:Path"; $env:CGO_ENABLED = "1"; go test -race ./...`
- CLI against the running demo: `.\bin\hydra.exe topics -b 127.0.0.1:9201`,
  `.\bin\hydra.exe consume -b 127.0.0.1:9301 -topic prices -partition 0 -max 5`.
