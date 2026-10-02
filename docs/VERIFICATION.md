# 分組驗證與發布安全方法

本文件只定義測試方法與 gates，不保存執行日期、測試數、實際結果、source manifests、真實目標或逐次 review 報告。本機／真 provider 執行的命令、stdout/stderr、版本、skip/blocked 原因與完整採證只存 repo 外受限目錄。公開 GitHub-hosted CI 僅使用匿名 fixtures，其 Actions logs、summary 與 coverage artifacts 可公開查看，不包含本機歷史或真 provider 資料，也不 commit 回 repository。不得以本指南存在宣稱任何 gate 已通過。

需求與不變量見 [PRD](../PRD.md)、[DESIGN](../DESIGN.md)；source/tests 導航見 [IMPLEMENTATION](../IMPLEMENTATION.md)。新增 workflow 須另遵守 [ADDING-A-WORKFLOW](ADDING-A-WORKFLOW.md)，產品 code-review 規則見 [CODE-REVIEW](CODE-REVIEW.md)。

## 1. 共通方法與判定

- 執行真實 engine、Store、filesystem、schema validator 與業務驗收，只在外部 Pi/RPC/provider/GitHub/API 或明確批准的 OS File.Sync dependency boundary 替代。`engine.Options.SyncFile`／`contract.Options.SyncFile` 是逐 instance、建構後固定的正常 DI，nil 使用真 `os.File.Sync`，不是產品 fast mode，也沒有 CLI/env 開關。成功替身限 Sync 非受測性質的明列案例；synthetic Sync failure 是補充採證，不取代既有真 OS faults。Durability、storage/journal failure、持久化順序及 fsync queue 測試保留真 Sync；Read／Write／Close／Rename／cleanup、path／digest／Ref 驗證不替代。不要 stub Step、reducer、內部轉換或流程 helper。
- 相同行為使用 table-driven tests；race／並行情境用 channel、RPC、socket、filesystem barrier 控制順序，不以 sleep 推定 accepted、join 或 cleanup。Timer 只作 deadline 來源及失敗保險。
- 每個 test 為自有 subprocess 註冊獨立 Close/Wait/lifeline cleanup，assertion 失敗也不能誤傷外部 sentinel 或留下 fixture。Temp discovery 不掃描／清空使用者真實 sessions。
- 分開列 local unit、subprocess、race、bundled Pi、live-provider／hub、各業務 E2E、build、獨立 review、release scan。任一 required case 失敗或 blocked 不以其他組成功代替。
- Opt-in skip 不算 PASS；parent PASS 但 descendants 全 skip 也不能算對應情境完成。Helper guard、no-test-files package、compile-only example、fuzz seed 與獨立 scenario 必須區分；seed corpus 不等於 fuzz mutation campaign。
- 權限故障可能因 root 身分而無法觸發，PTY 有平台限制；逐項標明原因，不假稱無 skip。Source 與 dependency 版本變動後，舊結果不能自動沿用。
- Code／scale-failure／simplicity／correctness 審閱與執行採證分開。未完成、timeout 或沒有結果的 review tool 不算通過；補審也不假稱原工具已執行成功。產品 `code-review` 不因此增加 reviewer/OCR 階段。

## 2. Local suite 與 build gate

前提：Go 1.25.0、macOS arm64、git、Python 3.13 與 golangci-lint 2.11.4；完整環境依各測試所需工具確認。以下是可執行方法，不是執行紀錄；輸出／binary 放 repo 外的自有目錄。

```sh
go mod verify
# Full normal and race/atomic coverage each use count=1, in exclusive partitions.
python3 -B testdata/ci/suite.py inventory --plan "$OWNED_DIR/plan.json"
python3 -B testdata/ci/suite.py matrix --plan "$OWNED_DIR/plan.json"
# Run each returned (mode, group) exactly once. Targeted modes use count=3.
python3 -B testdata/ci/suite.py run --plan "$OWNED_DIR/plan.json" \
  --directory "$OWNED_DIR/sets" --mode "$MODE" --group "$GROUP"
go vet -p 1 ./...
go build -p 1 ./...
python3 -B testdata/live/review_test.py
python3 -B testdata/ci/suite_test.py
python3 -B testdata/ci/terms_test.py
python3 -B testdata/ci/terms.py   # needs the private term list; SKIP is not a pass
git diff --check

RELEASE_DIR="/path/to/owned/release-dir"
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 \
  go build -p 1 -trimpath -buildvcs=false -o "$RELEASE_DIR/pi-workflow-controller" ./cmd/pi-workflow-controller
```

Gate：各必要 case 的預期行為成立，無 race；實際執行交付 binary 的 `list`，並用未知 workflow 核對 exit 2、無 task/Pi 啟動。從 source repo 外及不同可信 cwd 核對 embedded 資源不依賴原始路徑。Binary、logs、coverage/profile 不納入 Git。CGO-disabled macOS binary 不等於其他平台全靜態相容。

### GitHub Actions CI

[CI workflow](../.github/workflows/ci.yml) 在 `main` push、PR 與手動觸發時執行。使用 `macos-15` arm64，並檢查 runner、主機與 Go target 架構；不以 Linux 的 compile-only 結果代替 Darwin filesystem／PTY tests。

- 獨立 jobs：format/vet/lint＋Python verifier/CI 工具 tests、Go inventory、有限 matrix 各一般/race/targeted 組、module/build/CLI smoke、最終 collection/coverage。Go 版本讀取 `go.mod`；actions 固定 commit，golangci-lint 固定版本。
- Repository token 僅 `contents: read`，checkout 不保留 credentials；不使用 `pull_request_target`、外部 coverage service、模型 secrets 或自動 repository writes。
- `PWC_BUNDLED_PI=0`、`PWC_LIVE_PI=0`；這些 opt-in 情境在 summary 明列排除，verbose test logs 保留逐項 skip 與原因。其餘 required command 失敗即 job 失敗，沒有 `continue-on-error` 或自動重試掩蓋失敗。
- `suite.py` 使用 Go 原生 `go list -p 1`／`go test -list .` 建立當前平台的 top-level inventory，再以 anchored `-run` 選擇互斥分組。Triage 分 pure、Store、其他 local；RPC 為 m1–3、m4、m5、m6、m7 與完整正向補集。其他 package 各一組；engine queue 的 default stress 與完整其餘 child 補集分開，不先重跑整個 parent。
- `-list` **不列動態 leaf**。新 top-level 進 package/local 完整補集，新 RPC/queue child 由完整互斥 selectors 接住。CI 驗 run/terminal、group 聯集、唯一 leaf 歸屬、unfinished、parent-only、skip identity/reason 及 normal/race 聯集一致；無 test files 的 package 在 coverage 模式可由 Go 輸出 package PASS，仍只算無案例的編譯／coverage 記錄，不當作 case PASS。跨 shard 的共享 parent只列結構重複，不算重跑 leaf。不在 repo 放每案 expected registry；因此未來 CI 不自行證明已刪除的歷史 case 曾存在。修改測試時仍須在 repo 外逐案對照前次完整 raw，並 review 原 values、精確 assertions、順序／ownership／cleanup。
- 全集合 normal、race/atomic coverage 各 `count=1`、`-p 1`。另以 normal/race `count=3` 分帳驗 fresh delivery handoff、兩個 reframe/history recovery 情境、settle-abortgate cleanup priority、slow intake、supplement exhausted、mixed timeout/fatal、Host cleanup、engine queue/finalization I/O/StageConfirm。Targeted 不產生或混入 coverage。歷史 FAIL 與未定位原因必須保留，重跑 PASS 不是根因修復。
- Matrix `max-parallel=2`、`fail-fast=false`；每個 Go command 保留 `15m` watchdog，每 job `20m`（含 setup、artifact、cleanup 餘裕）。這不是提高原 Go timeout；主要減量来自互斥分組。校準須採同 tree 各組 normal/race/targeted 的實際 wall time，保留至少五分鐘 job 餘裕；超出組預算需重新切分，不 skip 或全開並行。本機預設串行，異硬體 hosted runner 仍須另外觀測，不由本機耗時宣稱 remote CI 已驗。
- 每個 full race group 有唯一 `atomic` profile。以同 compiler/平台/來源 `-race -run '^$'` reference 建立每 package block/statement inventory；reference 不算 test PASS、counts 不加入。每份 profile 必須含全部零 hit blocks，拒 missing/extra/重複 profile、path/inode alias、symlink、截斷、mode、座標及 statement 衝突；同 block 的整數 counts 相加，不 OR 或平均。
- 只有所有 normal/race/targeted、quality、build jobs 成功，且 collection 與 reference inventories 完整，才 merge 並執行 `go tool cover -func/-html`。失敗 run 的 raw JSON/診斷與部分 profiles 以測試 artifacts 留存，不包裝成成功 coverage。
- Coverage 是 Go statement 統計，不是 branch coverage、不保證涵蓋全部 subprocess，也不量測 Python／TypeScript。沒有最低百分比或 patch coverage gate；成功百分比不能代替 skipped integration gates。
- Coverage artifact 明列 `coverage.out`、`functions.txt`、`coverage.html` 與匿名 collection `summary.json`；分組 artifacts 另存 raw JSON、stderr、command/source identity、elapsed 及各 race profile，保留 14 天。不包含 binary、temp HOME、session history 或 auth files。Actions 自身的 logs／retention 由 GitHub 設定管理。
- Binary smoke 僅在 owned empty cwd 執行 `list` 與 `run unknown "CI smoke"`，分別驗 exit 0/2、cwd inode/path/content 不變。`env -i`、不含 Pi 的 PATH、自有 HOME/TMP/cache/logs 均在 cwd 外；不執行合法 workflow、不以此聲稱 Pi/provider/live 能力。
- 發布來源重建另外使用明列 regular entries 的 USTAR，驗 source payload SHA/mode/size、無 PAX/xattrs/`._`/symlink/`.git`，還原至 fresh owned 目錄後同 target CGO-disabled build，再做上述 binary smoke。不得把工程 snapshots、archive 或 binary commit 回 repo。

### Runtime／RPC／process

入口：[runtime_test.go](../internal/runtime/runtime_test.go)、[transport_test.go](../internal/runtime/transport_test.go)、[codec_test.go](../internal/runtime/codec_test.go)。共用外部 fixture：[protocol](../internal/testutil/protocol/protocol.go)。

| 方法／情境 | 必須成立的 gate |
|---|---|
| 任意 chunk、同 chunk 多 frame、LF/CRLF、U+2028/U+2029、大於 Scanner 預設的合法 frame | 按 LF 還原；invalid JSON、必要欄位／assistant content 錯誤、oversize、EOF 半筆為 ProtocolFailed，不截斷後繼續 parse |
| Response 亂序、event 先於 ack、waiter 取消、process exit | 唯一 reader/writer、request ID 精確關聯、事件不丟失；pending waiters 解除，無無界 goroutine/queue |
| Startup readiness 比普通 RPC timeout 慢，但仍在 startup budget；parent typed cancellation | 首次 readiness 使用剩餘 startup deadline，保留 parent cause，部分啟動 cleanup/Wait 不遺漏 |
| Prompt rejection、完整 write 後 ack timeout、pipe failure、缺 token／duplicate token／錯 lineage | 保留 accepted=no/unknown，不自動重送；舊 turn 的 settled 不完成本次 dispatch |
| 同 handle 多次執行、累積 history、event/entry append lag、長時間沒有新 entries | 增量 cursor、已匹配 hashes 釋放；backlog limit 不作 lifetime quota，delta 不驅動輪詢，fallback/backoff 仍可完成 |
| Retry success、耗盡、length、sticky abort／compaction failure | 只解除對應舊 provider error，不讓新 terminal failure 被抹掉；未證明屬於恢復的 stop 不補成功 |
| Stage 後 epoch/model/thinking/session/branch 改變 | Confirm 拒絕失效 receipt；既有 drift/manual-compaction 防禦保留，不擴張成任意人工介入支援 |
| Context-owned pipe deadline 先於 context timer 公布；真正較早 RPC timeout | 前者保留 context typed cause，後者仍為 RPCUnresponsive |
| Health probe 關閉／真 protocol fault、慢 observer、stderr 滿 | Intentional lifetime cancellation 不製造假 fatal；真 fault 不吞，reader/probe/observer/drain 有界且可 join |
| Concurrent Close、active native tool、foreign sentinel、ownership mismatch／recovering | 唯一 Wait；確認退出後先刪自有 discovery 再 bounded observer drain；他人資源保留，未確認項目明列 |

### Store／JSON／schema

入口：[contract_test.go](../internal/contract/contract_test.go)、[schema_test.go](../internal/contract/schema_test.go)、[json_test.go](../internal/contract/json_test.go)。全部用真 temp filesystem 與 registry。

- 缺檔、非 regular file、invalid UTF-8、duplicate keys、trailing JSON、過深／超限、schema mismatch、wrong nonce、跨 attempt 舊 contract 必須拒絕。
- Schema Draft/format assertions、跨檔 embedded IDs/anchors/dynamic refs、dormant refs 均離線解析；不能啟用 network/host loader，原始 schema resources/digests 不因 compiler aliases 改寫。
- Numeric schema representability/panic guards 與 json.Number 精度保留；不能宣稱消除 [DESIGN 的極端數值限制](../DESIGN.md)。不要在一般 gate 中執行可能耗盡主機資源的大型 probe。
- 頂層 files count 在 envelope item validation 前檢查；absolute/非 canonical/`..`/symlink/FIFO/directory/duplicate inode/ancestor collision 拒絕，無效 candidate 路徑不誤提升成 storage failure。
- 真 Stage 後改 candidate，Publish 仍使用 Stage bytes；讀取中變更來源／destination、staging tamper 必須拒絕。Discard 冪等，不刪 published/history。
- Root alias/WIP symlink 正規化後 request/Ref/renderer 一致，LaunchCWD 不變；attempt/artifact no-follow 不放寬。Exclusive rename 不覆蓋既有 published，即使空目錄也拒絕。
- 雙 digest、identity、canonical path、file digests 都檢查；重算 manifest 不能繞過 Ref anchor。Manifest read limit 依實際生成 bytes，不能因 escaping 或 policy 算式溢位拒絕合法輸入。
- Temp file 在 Write/Sync/Close/rename 故障時只清自有檔案；caller 的 typed cancellation 保留。用 OS filesystem fault/barrier，而非 mock read/write helpers；另以 constructor SyncFile DI 補驗 Sync 結果傳遞、descriptor Close、temp ownership 與失敗不授權，不把 synthetic 成功當成斷電 durability 證明。ID/token/attempt number 預約失敗不重用，並行 Store/attempt 不互相污染。

### Engine／persistence／finalization

入口：[engine_test.go](../internal/engine/engine_test.go)、[persistence_test.go](../internal/engine/persistence_test.go)、[deadline_test.go](../internal/engine/deadline_test.go)、[protocol_test.go](../internal/engine/protocol_test.go)、[resource_test.go](../internal/engine/resource_test.go)。

| 方法／情境 | 必須成立的 gate |
|---|---|
| 順序 Ref 交接、不同完成順序的 parallel、schema 修正＋reviewer reject、fresh/reused session、nested retry | 真實 workflow control flow；宣告順序回傳，首次之外的 retry budget 正確，ancestor 未結束前不提前定案 invocation |
| 重複 names/keys、同 handle lease、Preparing/request/waiting/rejected 等失敗 | Preflight 拒絕不建 attempt；Preparing 起消耗額度，不把未派送次數退還，Busy 不干擾現有 lease |
| Inputs、feedback refs、Decision、Decode/ReadContract、Retry/Parallel/final Result | 所有入口使用同一 committed resolver；跨 run、偽造 metadata/hash、未提交 published、被改寫來源都拒絕 |
| Publish 後阻斷 journal 或必要 snapshot | Publish != commit，無 committed Ref，不派送下游，sticky fatal 不能被 callback 吞掉 |
| Runtime fatal ingress、queue overflow、Store read fault、run limit、同時活動 siblings | 不受 journal 阻塞的 first-root latch；真正來源保留 failure，collateral 為取消且 Cause 指根因，本次 accepted=yes 不被根因的 no 覆蓋 |
| Typed signals、bare context error、wrapped/nested callback、FailFast/CollectAll | 依 disposition 分類，FailFast join 並保留根因；普通 branch error 可處理，未知 bare context 不偽稱使用者取消 |
| 真 attempt/run timers 與 Preparing→Confirm、startup、deadline/outcome lock 競態 | Timeout 不因事件重置；鎖定前 deadline 拒絕成功，鎖定後 late cancel/deadline 不改 outcome |
| 阻擋 journal/snapshot、callback 尚未返回 | Root stop 與 outcome lock 啟動 Pi cleanup 不等 I/O 或 callback；資源 callback 仍須等 workflow join/Pi exit |
| Unconfirmed Close、partial acquisition、AddCleanup 註冊失敗／多個 callbacks | 不釋 live slot、不刪活動 workspace；失敗註冊由 caller 收尾，成功註冊按 ownership 反向清理並保留 errors |
| 在 result.json、RunFinalizing、cleanup.json、RunFinished、最終 snapshot/close 各點造成 I/O failure | 鎖定前不能成功；鎖定後所有 finalization errors 可見，StatePersisted=false 不假冒 durable state；取消保留 130/143，其餘收尾失敗不能 0 |
| 較早 producer、較晚 session 結束、artifact ID 與非固定檔名、JSON-only、缺少 Final | FinalSelection 選定正確 producer；未知 key/file、非 artifact、未 commit/tamper Ref 拒絕；result.json.final/Report.Final 一致 |
| Failed-but-valid incomplete report、root stop／callback fatal、Store 關閉後 formatter | 普通失敗可交付合法 final，不改 outcome；fatal 不強迫解析，formatter 不讀檔／不重開 Store、不猜 session |

### CLI／TUI

入口：[CLI tests](../cmd/pi-workflow-controller/protocol_test.go)、[Darwin PTY tests](../cmd/pi-workflow-controller/protocol_darwin_test.go)、[TUI model](../internal/tui/model_test.go)、[format](../internal/tui/format_test.go)、[safe text](../internal/tui/safe_test.go)。

- 真 RPC subprocess＋Store 驗證 CLI grammar、自動 task 路徑、Prompt 原樣落地、不 shell 展開；stdin/stdout 只有一端 TTY 時為 plain。Mixed-stream stdout PTY 可關閉 ONLCR，延後 drain 至真實 run 終態以製造 backpressure，直接檢查未經 CR 正規化的 application output；雙 TTY 的 cooked/raw/restoration cases 另保留原 terminal flags。
- 真 PTY 堵滿後送 q／Ctrl+C，包括非取消輸入洪流；未恢復 output drain 前就要觀察取消登記與 engine cleanup。Paste/terminal reply 中的文字不能被當作取消鍵。
- Readonly/active write error、input fault、stdout broken pipe/SIGPIPE：FD-preserving interception、獨立 Run.Cancel、全部 joins、stdout report failure 的 stderr fallback。不能只依 Tea renderer 回 error。
- 最終 Report 前嘗試 restore，termios/alt screen 回復；terminal Snapshot 不代替 Report。Permanent blocked I/O 不列固定 wall-clock 保證，stdout/stderr 同時不可用也無完整顯示保證。
- Parallel/retry activation/latest feedback/monotonic elapsed/spinner 顯示及 metadata copy ownership；ANSI/OSC/C1 等逐欄位 sanitize。摘要不是 input quota，所有 cleanup/finalization errors 不截斷成成功。
- Report 的 final producer、unconfirmed exit、unavailable、StatePersisted=false、所有 warnings 與取消 exit precedence 一起驗。

## 3. Bundled Pi／localhost provider gate

```sh
PWC_BUNDLED_PI=1 go test -race -count=1 ./internal/runtime ./internal/engine ./internal/workflows
```

使用[共用 harness](../internal/testutil/bundled/bundled.go)與 [runtime](../internal/runtime/bundled_test.go)／[engine](../internal/engine/bundled_test.go)／[workflow](../internal/workflows/registry_test.go) tests。需 installed Pi 0.84.3、deployed WebUI 及 Python；harness 的 executable/extension 探測位置是建構前提，執行前讀 source 核對，不假定所有安裝 layout 通用，亦非產品 CLI flags。

- 真 Pi executable，不以 nonbundled source 或 RPC substitute 代替。Launcher 保留 process identity；RPC trace 不改 bytes，額外 commands 經唯一 writer/request IDs。
- Temp HOME/agent/discovery、清除 inherited credentials、localhost provider；明確載入 deployed WebUI 與 testdata fixture，不修改全域 settings。停用測試 extension 的額外自動命名外連。這組不證明使用者現有 hub 可見性。
- Fixture 只用外部 input／message_start／session_before_compact／原生 dialog hooks，不替換內部演算法、不偽造 get_state/get_entries/settled；candidate 由真 Pi tool 寫出，不由 HTTP server 直接造檔。

| 必要情境 | 觸發方法／必要證據 |
|---|---|
| Startup exact binding、memory entry 非 durability、同 handle 再派送／新 handle 隔離 | 首個 provider response barrier 前觀察 token entry，但不得先取得完成 receipt；再核對各 dispatch 的 token/entries/settled/history |
| Busy rejection、handled/transform 丟 token | 受控 provider 保持活動；input hook 只改本 case 的輸入，核對 ack/no token、typed failure、Close、不 resend |
| select/confirm/input/editor | 各原生 ctx.ui hook 產生 dialog，matching cancelled response、InteractionRequired、Close，不自動批准 |
| Terminal stop/error/length、provider retry success/exhaustion/sleep cancellation | localhost HTTP/SSE boundary 回應，核對真 events 與最終 assistant；length fixture 不誤觸可恢復 overflow |
| Overflow/compaction continuation、本次 compaction abort/failure | 小 context／warm history／summary provider；先看到本次 token 才用非 manual cancellation hook，另以 summary error 觸發 failure，不能拿派送前 manual compaction 或 RPC abort 代替 |
| Receipt→Stage→Confirm→publication、candidate/Stage 已存在時 cancel/timeout | 外部 barrier 委派真 Execute/Confirm，真 Store 固定 bytes／committed membership；取消不發布 |
| Process/pipe failure、partial startup、active native bash、外部 sentinel | 精確 barrier 操作自有 process/pipe；驗 Wait、tool EOF/退出、sentinel 存活、own discovery 移除、history 保留 |

任意壞 frame/chunk/queue overflow 等刻意違反 RPC 的案例留在 substitute suite，不要求真 Pi 自己產生壞協定。人工 hub steering/model/session mutation 不列必要 bundled gate，既有防禦回歸可保留。

## 4. Shared discovery／live-provider／hub gate

此組需明確授權目標、成本與環境。正式預設資源載入、真 provider、共享 discovery，不能由 isolated bundled group 代稱。

### 每次 persisted Pi 啟動前

1. 唯讀核對目標 deployed WebUI recovery 實作與版本，尤其 parent `pid` 判活與 `.recovering` claim；不只看 `piPid` 或 hub 的 Online 標記。
2. 檢查共享 discovery。可能 delete/resume 他人 stale session、dead/unverifiable parent、異常 claim 或判準不明時 blocked，不代刪、不清空、不替他人 resume。無 truthy pid 的 hub-state 等 JSON 只按實際 recovery 規則略過。
3. Preflight 不是跨 process 鎖；獨立 task 不是 discovery 隔離。在受控時段循序執行，不和其他 shared-discovery tests 平行。Resume/recovery probe 也重新預檢。
4. 不直接讀 secrets 或把 credentials 放 argv/logs/docs；認證配置不等於 inference 成功。不停用 TLS、不提升 trust、不改 Pi/hub/全域定義。

### 執行與 gates

```sh
PWC_LIVE_PI=1 go test -race -count=1 ./internal/engine \
  -run '^TestEngineLiveCancellationTimeoutRecovery$'
```

此[測試](../internal/engine/live_test.go)是 opt-in，不是產品 workflow flag。另執行獲授權的正式 binary smoke 與各 workflow E2E：

- 真 provider/model/thinking、Pi session/discovery identity 精確相符，合法 echo/業務 contract 由 Controller 驗收。
- Hub 只讀本次自有 sessions/status/history，先用本次 Controller ownership 過濾，再取內容；不對他人 session 做 kill/resume/switch，執行中不 prompt/steer/Stop。
- Accepted barrier 後的 SIGINT/SIGTERM、Controller Cancel、真正 Step timeout 都不能被 candidate 存在覆蓋；工具啟動使用 socket nonce/ancestry barrier，不以任意 bash 呼叫冒充活躍工具測試。
- Cleanup 後確認 Pi/Wait、自有 discovery、native bash 工具停止、外部 sentinel 仍存活、history 保留；安全預檢後再啟動專用 Pi，舊自有 session 不被 recovery 復活、保留 history 不被改寫。
- 實際 binary 在不同 cwd 收到完整 Report。Hub GET 可見性不等於 browser 視覺或人工操作驗證；磁碟版本不保證已啟動 backend 記憶體版本，不為採證重啟他人服務。
- 任一模型 schema/內容失敗須保留為失敗，不放寬 gate；live 可能花費且不保證每次模型配合。

## 5. Code-review 業務 gates

使用真 engine/Store/Git/HTTP backend fixtures，只替代外部 API/Pi/provider。入口見 [workflow_test.go](../internal/workflows/review/workflow_test.go)、[acquire_test.go](../internal/workflows/review/acquire_test.go)、[check_test.go](../internal/workflows/review/check_test.go)、[report_test.go](../internal/workflows/review/report_test.go)、[resources_test.go](../internal/workflows/review/resources_test.go)、[safety_test.go](../internal/workflows/review/safety_test.go)。

- 固定 Prepare、三個必要 reviewer、Validation；Prepare exit 後才開 reviewers、barrier 證明真 parallel/join，必要分支失敗產出 incomplete 時仍非零。Root cancellation/fatal 不開 Validation。
- Pin/base/head/unique merge-base/diff/context ID、exact Prepare Ref、role、來源 snapshot 原 bytes、requirements ID 全覆蓋、evidence 的 head 路徑與 inclusive 行範圍必須一致。
- Findings 全有 disposition，confirmed target 須自我 confirmed，merged 不能指 excluded 或自行循環。需求的原 statement 不縮為較弱子命題，未確認前提不能假稱 satisfied。
- Partial fetch fixture 真正支援 blob filtering/lazy hydration；tracked/untracked/ignored/snapshot mutation 拒絕。Submodule gitlink 保留但不執行外部 transport，LFS pointer 不當作外部內容。
- Git config/hooks/external diff/textconv/submodule recursion 安全限制、command deadline/stdout cap、partial acquisition cancellation/Wait、checkout ownership/replacement 拒絕及 AddCleanup 失敗責任。
- Deterministic renderer 在同一 Validation Step 執行，JSON identity/files/artifact ID、symlink/collision、完整原 statement/Refs 與 Go 逐值附錄驗收一致。異 cwd／binary 離 source 的完整 embed tree 可讀。
- Timeout 分層：縮短外部 runtime context 的 fixture 可驗 branch failure/incomplete，但不冒稱等過產品完整 Step timer；真正 Step timer 另由 engine bundled/live gate 驗。
- 此 workflow 的真 Pi/provider/獲授權 PR E2E 另行進行，不能以 smoke-echo 或 substitute 代替。範例目標使用 `https://github.com/owner/repo/pull/123`，執行時的真目標只記 repo 外。
- 真 E2E 核對所有角色/read skills/resources/refs/report/cleanup、global resources 不變、未執行被審 code/tests/build/scripts 或外部 writes。這是採證方法，不是 OS sandbox／全面 syscall/network 稽核或模型推理正確率保證。

## 6. 發布前 privacy／安全 scan

發布候選只包含通用 source/docs、匿名可重現 fixtures/tests 與公開產品配置。以下是檢查策略，不會自動刪除資料、重寫歷史或授權發布。

1. **列出實際發布集合。** 唯讀查看 working tree、index、untracked/ignored files、symlinks 及 artifact packaging 規則。不要只掃 README 或只相信 ignore；已 tracked 的 secrets/report 不因 ignore 而消失。
2. **掃內容與路徑。** 以文字／secret scanner 搜尋 credentials/private keys、auth headers、email、公司名稱/domains、真 PR/ticket URLs、絕對 user/source 路徑、真 task/run/session IDs、PID、實測日期、commit/source hashes、RPC traces/history、reports/manifests。數值／格式命中須人工分類，公開 dependency/model 版本、匿名 fixtures 與 runtime field names 不自動當洩漏。
3. **檢查所有格式。** 包含 Markdown、Go tests、JSON、embedded skills/scripts/resources、hidden files、binary/archives；不只掃副檔名。不要執行未知 repo 程式碼來做安全掃描，也不把私有內容上傳外部 scanner。
4. **檢查本地 links。** 對每份 Markdown 的相對路徑依文件所在目錄解析；確認檔案存在、anchors 有效，沒有已刪報告或私有備份回指。範例使用 placeholders／`github.com/owner/repo`，不使用真任務識別或版本 hash。
5. **另查 Git history。** 工作樹乾淨不代表歷史安全；掃即將發布的所有可達 commits/branches/tags、path names 與 blobs，另查交付 archive 是否夾帶 `.git`、bundle、ignored binary/logs。若需移除私有歷史，先取得獨立授權並在 repo 外確認受限備份，不能只刪當前文件便宣稱歷史已清。
6. **內部名詞 gate。** 公開 repo 不得含公司內部系統、架構、主機與環境名稱。詞表含這些名詞本身，因此只存 repo 外私有目錄（`$PWC_TRIAGE_SKILLS_DIR/denylist.txt`），不得複製進 repo。格式：`[hard]`／`[soft]` 分段，每行一個不分大小寫、以字詞邊界比對的 regex，`#` 開頭為註解；hard 命中即失敗，soft 命中列出供人工判斷。以 `python3 -B testdata/ci/terms.py` 掃 tracked 檔案的 index（staged）與 working tree 兩個版本、未忽略的 untracked 檔案、symlink 目標及路徑名，只輸出位置與詞表行號；預設位置找不到詞表時結果為 SKIP（exit 3），**不算通過**；明確指定的 `--list` 不存在、未知段落標頭或沒有任何 hard pattern 都是失敗。`\b` 把底線與數字視為字詞字元，`term_x`、`term01` 這類識別字形式不會被 `term` 命中，詞表須自行列出。CI 沒有詞表，只跑 `terms_test.py` 的匿名 parser／比對測試；實際 gate 在本機發布前執行。
7. **人工覆核與 release gate。** 對 scanner 命中逐項核對，無法確定即 blocked；確認無敏感資料、broken local links 或誤導性驗收宣稱再發布。安全 scan 不等於 tests/build/review 通過；每組單獨判定。

實際 scan finding、檔案 manifests、執行命令/輸出、日期、SHA 及稽核結果只寫 repo 外受限紀錄，不將本機報告、真實 traces/history/email 或私有備份位置加入 commit。若發現洩漏，不將原文複製到 issue/PR，依授權處理。

## 7. 保留的限制

Numeric validator 極端可表示性／boolean inversion／error-tree 放大與非協作取消、完整 snapshot 重寫 I/O、永久 blocked stdout/filesystem 的非固定 wall-clock、任意 detached 子孫、Controller abrupt death，均保留 [DESIGN](../DESIGN.md) 的限制。不承諾 sandbox、supervisor、crash resume、exactly-once、外部副作用 rollback 或同 UID 惡意隔離。未知／未執行情境須明列，不以「全部通過」掩蓋。
