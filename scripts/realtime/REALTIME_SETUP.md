# TOS Excel RTD → Python Realtime Advisor

The complete setup path for connecting thinkorswim desktop real-time
data to the LLM advisor via Excel RTD bridge.

## What you'll get when done

```
TOS (real-time quotes) → Excel (RTD live cells) → CSV (every 10s)
  → Python daemon → DuckDB → setup detector → LLM advisor
  → Telegram → your phone vibrates with a trade idea
```

Total setup time: ~30-60 min (one-time).

## Step 1: enable RTD in TOS

1. In TOS desktop, **Setup → Application Settings → General → Other**
2. Find **"Enable RTD link"** checkbox → check it
3. Restart TOS

(If you don't see this option, your TOS version may need updating. Schwab
re-enabled RTD in 2024 after the TDA → Schwab merge.)

## Step 2: build the Excel sheet

Open Excel (real Office, not LibreOffice — RTD is COM-based) and create
a workbook with two sheets: `RTD` and `Output`.

### Sheet 1: `RTD` — the live cells

In `A1` paste the header row (one entry per column), and in `A2` paste
the RTD formula row. Adjust the symbols to your needs.

| Column | Cell A1 (header)   | Cell A2 (RTD formula)                    |
|--------|--------------------|------------------------------------------|
| A      | timestamp          | =NOW()                                   |
| B      | SPX_LAST           | =RTD("tos.rtd",,"LAST","SPX")            |
| C      | SPX_BID            | =RTD("tos.rtd",,"BID","SPX")             |
| D      | SPX_ASK            | =RTD("tos.rtd",,"ASK","SPX")             |
| E      | SPX_ATM_CALL_IV    | =RTD("tos.rtd",,"IMPL_VOL",".SPX260528C7520") |
| F      | SPX_ATM_PUT_IV     | =RTD("tos.rtd",,"IMPL_VOL",".SPX260528P7520") |
| G      | SPX_PC_RATIO       | =RTD("tos.rtd",,"PUT_CALL_RATIO","SPX")  |
| H      | SPX_FLIP           | <a static value or formula — explained below> |
| I      | NDX_LAST           | =RTD("tos.rtd",,"LAST","NDX")            |
| J      | NDX_FLIP           | (same approach as SPX_FLIP)              |
| K      | XSP_LAST           | =RTD("tos.rtd",,"LAST","XSP")            |
| L      | XSP_FLIP           | (same)                                   |
| M      | RUT_LAST           | =RTD("tos.rtd",,"LAST","RUT")            |
| N      | RUT_FLIP           | (same)                                   |
| O      | VIX_LAST           | =RTD("tos.rtd",,"LAST","VIX")            |

**FLIP column**: this is the gamma-flip-strike level computed in your
DuckDB by `compute_index_gex.py`. You have two options:

- **Static**: Query the latest flip strike each morning, type it into Excel.
  Update manually (e.g. before US open).
- **Auto** (more work): Use a Python script that writes the flip strike
  back to a small CSV (`/tmp/flip_levels.csv`), then have an Excel
  add-in pull it via `=GETPIVOTDATA` or similar.

For starter, type it in manually each morning. Run:

```bash
python3 -c "
import duckdb
con = duckdb.connect('quant-research-v1/data/quant.duckdb', read_only=True)
for sym in ['^SPX', '^NDX', '^XSP', '^RUT']:
    r = con.execute(f'''
        SELECT gamma_flip_strike FROM index_gex_snapshots
        WHERE symbol = ? AND dte_bucket = '1DTE'
        ORDER BY snapshot_time DESC LIMIT 1
    ''', [sym]).fetchone()
    if r: print(f'{sym} flip: {r[0]:.0f}')
"
```

Type the resulting numbers into H2 / J2 / L2 / N2.

### ATM strike must be updated each day

The IV cells use a contract code like `.SPX260528C7520`. The `260528` is
the expiry date (YY MM DD) and `7520` is the strike. **You need to
update this each morning** to ATM of the current day. Strike granularity:
SPX = 5 wide near ATM, NDX = 25 wide, XSP = 0.50 wide.

Tip: keep a separate cell for "today's date" and "ATM strike",
then build the contract code with `=".SPX"&TEXT(B1,"yymmdd")&"C"&B2`
type formula. Manual once per day.

### Sheet 2: `Output` — what the macro writes

This sheet is empty. The macro will dump `RTD!A1:O2` into a CSV.

## Step 3: VBA macro that exports to CSV every 10s

Press **Alt+F11** to open the VBA editor.

In **ThisWorkbook** module (left panel), paste:

```vba
Private Sub Workbook_Open()
    ' Start the CSV writer 5 seconds after workbook opens
    Application.OnTime Now + TimeValue("00:00:05"), "WriteSnapshot"
End Sub

Private Sub Workbook_BeforeClose(Cancel As Boolean)
    ' Cancel any pending OnTime calls
    On Error Resume Next
    Application.OnTime EarliestTime:=Now, Procedure:="WriteSnapshot", Schedule:=False
End Sub
```

In **Module1** (Insert → Module if needed), paste:

```vba
Public Const CSV_PATH As String = "C:\TOS\snapshot.csv"
Public Const INTERVAL_SECONDS As Integer = 10

Sub WriteSnapshot()
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Sheets("RTD")
    Dim lastCol As Integer
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column
    Dim header As String
    Dim values As String
    Dim i As Integer
    header = ""
    values = ""
    For i = 1 To lastCol
        If i > 1 Then
            header = header & ","
            values = values & ","
        End If
        header = header & ws.Cells(1, i).Value
        Dim v As Variant
        v = ws.Cells(2, i).Value
        If IsError(v) Or IsEmpty(v) Then
            values = values & ""
        ElseIf TypeName(v) = "Date" Then
            ' Format as ISO 8601 UTC
            values = values & Format(v, "yyyy-mm-ddThh:mm:ss") & "Z"
        Else
            values = values & v
        End If
    Next i

    ' Write to CSV (overwrite each time — Python takes the latest row)
    Dim fnum As Integer
    fnum = FreeFile
    Open CSV_PATH For Output As #fnum
    Print #fnum, header
    Print #fnum, values
    Close #fnum

    ' Schedule next run
    Application.OnTime Now + TimeValue("00:00:" & Format(INTERVAL_SECONDS, "00")), "WriteSnapshot"
End Sub
```

**Save as macro-enabled workbook** (.xlsm). Allow macros when prompted.
On open, the macro auto-starts and writes `C:\TOS\snapshot.csv` every 10s.

## Step 4: Run the Python daemon

On the same Windows machine (where the CSV lives):

```bash
# Install python deps once
pip install duckdb requests pyyaml

# Get the repo on Windows (clone or copy)
cd C:\path\to\quant-stack

# Test the CSV is being read
python scripts\realtime\csv_watcher.py --csv-path C:\TOS\snapshot.csv
# Should print "upserted N fields" every 10s
```

If you have the data flowing into DuckDB, run the full daemon:

```bash
python scripts\realtime\daemon.py --csv-path C:\TOS\snapshot.csv
```

## Step 5: Telegram bot

In Telegram:
1. Message **@BotFather**, send `/newbot`
2. Pick a name (e.g. `my_quant_bot`) and a username
3. BotFather replies with a bot token like `123456:ABC-def...`
4. Open a chat with your new bot, send it anything
5. Visit `https://api.telegram.org/bot<BOT_TOKEN>/getUpdates` in a browser
6. Find your `chat_id` in the JSON (look for `"chat":{"id": 1234567890}`)

Write `scripts/realtime/telegram.yaml`:

```yaml
bot_token: "123456:ABC-def..."
chat_id: 1234567890
```

(Or set env vars `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`.)

Test:

```bash
python scripts\realtime\notify_telegram.py --test
```

Your phone should buzz with "✅ test message from quant-stack realtime advisor".

## Step 6: Run everything together

```bash
python scripts\realtime\daemon.py --csv-path C:\TOS\snapshot.csv
```

The daemon will:
1. Poll CSV every 2s, upsert into DuckDB
2. Every 30s run setup detection
3. On setup hit: call LLM, send Telegram
4. Print everything to stdout

Leave it running during market hours (9:30 ET - 16:00 ET = 21:30 - 04:00
Asia/Shanghai depending on DST).

## What setups will fire today

Based on V1 detector rules:

| Setup type | Trigger |
|---|---|
| `gamma_flip_break` | SPX/NDX/XSP/RUT spot drops below gamma_flip_strike (vol amplifier ON) |
| `gamma_flip_recover` | spot recovers above flip (vol amplifier OFF) |
| `skew_spike` | put_iv - call_iv jumps from < 25pp to >= 30pp in 10 min |
| `vol_burst` | recent 5min realized vol > 2x trailing 30min vol |
| `vix_spike` | VIX up >5% in 10 min |

Each fires at most once per 15min window (dedup).

## Troubleshooting

**RTD formulas show #N/A**: TOS RTD link is off, or TOS isn't running.
Re-check Setup → General → Enable RTD.

**Excel is slow**: too many RTD cells. The recommended ~15 cells should
be fine. If you add more, expect TOS to slow down.

**No setups firing**: detector V1 is conservative on purpose. If you go
hours without a setup, the day is just quiet. You can lower thresholds in
`setup_detector.py` (SKEW_SPIKE_THRESHOLD etc.) if you want more noise.

**LLM JSON parse fails**: check stderr; usually means DeepSeek returned
markdown. The advisor strips ```json fences but may miss edge cases.
File a bug with the response text.

## What does NOT work via this bridge

- **Real-time chain data** (every strike, all DTEs). RTD has limits and TOS
  slows down with > ~500 cells. Use Polygon if you need full chain.
- **Order placement**: TOS won't accept orders from external apps. Manual
  click in TOS for now. (Schwab API needed for auto-order.)
- **L2 / depth book**: TOS doesn't expose L2 via RTD.

## Limitations of V1

- Setups only fire on threshold cross. A persistent SHORT-gamma regime
  that stays put won't fire after the initial break.
- LLM advisor uses DeepSeek; costs ~$0.001 per setup. At 5-20 setups/day
  that's $0.01-0.10/day. Negligible.
- No position tracking. The advisor doesn't know what you have on.
  You'll need to manage that mentally or extend with a position file.
