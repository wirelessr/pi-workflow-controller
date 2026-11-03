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
- `suite.py` 使用 Go 原生 `go list -p 1`／`go test -list .` 建立當前平台的 top-level inventory，再以 anchored `-run` 選擇互斥分組。每個 package 各一組；engine queue 的 default stress 與完整其餘 child 補集分開，不先重跑整個 parent。
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

入口：[runtime_test.go](../internal/runtime/runtime_test.go)、[transport_test.go](../internal/runtime/transport_test.go)、[codec_test.go](../internal/runtime/codec_test.go)、[command_test.go](../internal/runtime/command_test.go)。共用外部 fixture：[protocol](../internal/testutil/protocol/protocol.go)、[pathexec](../internal/testutil/pathexec/pathexec.go)（gh／git 固定入口經 PATH wrapper 執行測試 binary）。

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
| gh／git 命令：繼承的 `GIT_*`／`GH_*` env、固定 argv 與 git 固定 `-c`（credential、gc、maintenance、fsmonitor）、共用的 `GitConfigIsolation`、空或以 `-` 開頭的 endpoint、非零 exit、stdout 超限、deadline、取消、成功後殘留子孫 | 繼承 env 被移除，固定值與 caller env 在後；`GitConfigIsolation` 等於 review 原本的四個值，呼叫端改動副本不影響下一次；不合法 endpoint 在啟動 gh 前拒絕；錯誤不含 argv／stderr；超限、deadline 與 cause 保留；process group 在 Wait 前被殺 |

### Git repository

入口：[gitrepo_test.go](../internal/gitrepo/gitrepo_test.go)。以真 temp filesystem 與真 git 建立 worktree；只有失敗與輸入拒絕的情境以記錄型 Runner 代替 git。

| 方法／情境 | 必須成立的 gate |
|---|---|
| Root 為相對、不存在、檔案、symlink、group／world 可存取，或 `repository.git`／`checkout` 已存在 | 拒絕且既有內容不變；中途失敗只保留本次已建立的 `repository.git`；symlink 祖先正規化後記錄 identity |
| Pinned fetch 的 SHA 非 40 位小寫 hex、ref 不以 `refs/` 開頭、remote 為空或以 `-` 開頭 | 在任何 git 執行前拒絕；Runner 錯誤原樣回傳；fetch 後 commit 不符即失敗 |
| Branch fetch 的 remote 或 branch 為空或以 `-` 開頭、ref 不以 `refs/` 開頭；branch 含 refspec 字元（`:`、`*`、`..`、`@{`）或以 `.lock` 結尾；branch 不存在；來源前進後再 fetch | 前三項在任何 git 執行前拒絕；ref-name 由真 git 的 `check-ref-format` 判定，不合法時不執行 fetch、不寫任何 ref；固定 argv 與 Runner 錯誤原樣回傳；結果非 40 位小寫 hex 即失敗；再 fetch 取得新 tip |
| Root、`repository.git`、`checkout` 被替換或 `checkout` 換成 symlink | 回 ownership mismatch，不刪替換內容或外部檔案；還原後可完成 cleanup |
| Cleanup 前已取消、兩次刪除之間取消、重複呼叫、`checkout` 已不存在 | 保留 cause；再次呼叫完成剩餘 registration；成功後冪等；repository objects 與其他歷史保留 |

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
- 私有 skill 目錄（[skilldir_test.go](../internal/contract/skilldir/skilldir_test.go)，真 temp filesystem 與真 FIFO）：空的或不是單一路徑段、重複的子樹宣告，以及位於子樹內的 sidecar，必須在任何 filesystem 存取前拒絕；不是單一路徑段的展開名稱在任何寫入前拒絕；非絕對路徑、缺子樹或其 `SKILL.md`、symlink 子樹／檔案／sidecar、FIFO、非 printable（含控制字元）名稱、超過單檔／總量／檔數上限必須拒絕；恰在上限、sidecar 不計入總量與檔數須接受；`.` 開頭項目整個略過，未宣告的子樹與檔案不讀入；展開依 byte order、固定小樹的寫死 digest 與 0700／0600 權限；預設期限為 1 分鐘，阻塞的開啟在期限回原訊息，外層取消回外層 cause。既有展開目錄不覆蓋由 triage 的 `TestPrepareSkillsKeepsExistingExpansion` 端到端驗證。

### Engine／persistence／finalization

入口：[engine_test.go](../internal/engine/engine_test.go)、[persistence_test.go](../internal/engine/persistence_test.go)、[deadline_test.go](../internal/engine/deadline_test.go)、[protocol_test.go](../internal/engine/protocol_test.go)、[resource_test.go](../internal/engine/resource_test.go)。

| 方法／情境 | 必須成立的 gate |
|---|---|
| 順序 Ref 交接、不同完成順序的 parallel、schema 修正＋reviewer reject、fresh/reused session、nested retry | 真實 workflow control flow；宣告順序回傳，首次之外的 retry budget 正確，ancestor 未結束前不提前定案 invocation |
| `StepWithRepair` 的 schema／acceptance repair、不可 repair 的失敗、預算 0／負數／用盡、repair 中取消、repair 無法開始、`RepairKeepsWork` 的派送文字 | 同一 handle 與 `contract-repair-*` 子 scope；repair Feedback 帶診斷並保留原 Refs；未設定 `RepairKeepsWork` 的每個派送都以原「先執行任務」指示開頭，設定者只在 repair 派送改以 repair 指示開頭，首次派送與一般 Step 不變；不建立 Retry activation；不可 repair 的失敗只派送一次；用盡回原始錯誤；被拒的發布仍 committed |
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
- Git config/hooks/external diff/textconv/submodule recursion 安全限制（含 HOME／XDG 下的全域 config 不生效）、metadata command 失敗與 deadline 的錯誤包裝及 cause、partial acquisition cancellation/Wait 及 AddCleanup 失敗責任。Stdout cap 與 checkout ownership/replacement 拒絕的機械 gate 分屬上方 Runtime 與 Git repository 節。
- Deterministic renderer 在同一 Validation Step 執行，JSON identity/files/artifact ID、symlink/collision、完整原 statement/Refs 與 Go 逐值附錄驗收一致。異 cwd／binary 離 source 的完整 embed tree 可讀。
- 組合單元 `review.Run`：外層在 root 佔用同名 Step/Attach key、Decision/Parallel 名、run 目錄與 cleanup 名時仍能在 Child scope 完成；外層 Run 限額恰需 5 sessions、3 live、5 attempts，任一不足為 run-scope LimitExceeded；不合格 name（含 `-skills`／`-work` 結尾）、URL 與停用的 Node TLS 依序在任何目錄、session、gh／git 呼叫前拒絕；Validation session 由單元在 report 驗收後關閉，關閉失敗不交付 final，incomplete 路徑保留 reviewer 的原分類。
- Timeout 分層：縮短外部 runtime context 的 fixture 可驗 branch failure/incomplete，但不冒稱等過產品完整 Step timer；真正 Step timer 另由 engine bundled/live gate 驗。
- 此 workflow 的真 Pi/provider/獲授權 PR E2E 另行進行，不能以 smoke-echo 或 substitute 代替。範例目標使用 `https://github.com/owner/repo/pull/123`，執行時的真目標只記 repo 外。
- 真 E2E 核對所有角色/read skills/resources/refs/report/cleanup、global resources 不變、未執行被審 code/tests/build/scripts 或外部 writes。這是採證方法，不是 OS sandbox／全面 syscall/network 稽核或模型推理正確率保證。

## 6. Code-change 業務 gates

使用真 engine/Store/runtime/Pi RPC 與真 git（本機 bare repo 經 `file://`），只替代 GitHub API（PATH 上的 gh wrapper）、GitHub 的 git remote 與 Pi/provider。入口見 [workflow_test.go](../internal/workflows/codechange/workflow_test.go)、[implement_test.go](../internal/workflows/codechange/implement_test.go)、[review_test.go](../internal/workflows/codechange/review_test.go) 與 [publish_test.go](../internal/workflows/codechange/publish_test.go)。GitHub API 替身依 endpoint 回答 repo、PR、compare、branch ref 與 branch 的 PR 列表，只接受 `--method GET`；PR 是假 Agent 開 PR 時寫下的記錄，其 head SHA 與 compare、ref 都在查詢當下由真 git 從本機 bare origin 算出，假 Agent 以真 git push 到該 origin。

- 前置拒絕依序（Prompt、`PWC_PI_CWD`、Node TLS、design doc、repo GET）發生在任何 worktree、git、session 與 attempt 之前；repo GET 之後、建立 worktree 之前，以同一 git 隔離環境執行 `git --no-lazy-fetch version`，不支援此選項的 git（以拒絕該選項的 PATH wrapper 模擬）以具名錯誤失敗，不建立 worktree、session 或 attempt。Worktree 建立的 git 政策（含 `--attr-source` 指向空 tree：fetch 到的 tree 內 `.gitattributes` 的 eol、`working-tree-encoding` 與 `ident` 不改變 checkout 的 bytes）、部分失敗的清理與註冊後的保留也在 `workflow_test.go`。`main()` 把 `PWC_PI_CWD` 交給 workflow 定義，由 cmd 層的 subprocess test 經 `main()` 證明。
- 實作者在 `PWC_PI_CWD`（不是 engine default）以 fresh session 執行，request 帶 worktree 絕對路徑與 input Ref；每個結束路徑都確認該 session 的 process 退出，成功以外的結束都保留 worktree。
- Receipt 只在 repair 迴圈接受最終 contract 後核對一次，只經 worktree registration 讀 HEAD，並停用 lazy fetch 與 replace refs：回報值不是 worktree HEAD、是 base 或其祖先（含以 replace ref 改寫 base 歷史的情形）、不存在，或 checkout 的 `.git` 被改指他處時，以具名錯誤失敗、不 repair、不記 Decision、不交付 Final，已 committed 的 `input` 與 `implement` 仍列在 Outputs；gaps 回報時 HEAD 不等於 base 同樣失敗。
- Schema 錯誤與 input Ref 回抄不符（Ref 的任一欄位）在 repair 時 Feedback 帶具體診斷，repair 派送使用 `RepairKeepsWork` 的文字，Agent 只 commit 一次；同一 handle、子 scope、預算用盡等 repair 機械由 engine tests 涵蓋。
- Ready 通過核對時記 `implementation-recorded`、gaps 且 HEAD 等於 base 時記 `design-gaps-reported`（各引用 `implement` Ref）。後者交付實作 contract 為 Final，以具名錯誤結束（Failed、exit 1）。A1 執行中取消為 Cancelled（130），保留 worktree。
- 發 PR 前審閱只在 ready 的 receipt 通過後執行；gaps、receipt 失敗與 A1 中取消都不建立 snapshot、不開審閱 session。Snapshot 位於 run 目錄自有的新 bare repository，以 alternates 借用 worktree repository 的 objects。Worktree 交給 Agent 之後，Controller 對其 repository 的 git 只有經 registration 的讀取（receipt 的 `rev-parse`、`rev-list` 與審閱後的 `rev-parse`），snapshot 的 git 以同一政策與環境只在自己的 repository 執行 `init` 與 `worktree add --detach <已核對 head>`（`TestImplementReady` 逐筆核對，含更正 Step 的 receipt 讀取與關閉後的 HEAD 重讀）。未 commit 與 untracked 的檔案不出現，Controller 讀過 HEAD 之後 worktree HEAD 再移動也不影響 snapshot；implementer 在 worktree repository config 設定的 filter 與 hooks 不執行（同一 filter 在該 repository 自己的 checkout 會執行，作為正向對照），commit 進 tree 的 `.gitattributes` 也不改變 snapshot 的 bytes。Snapshot 建立失敗時不開審閱 session、worktree 保留：缺少物件時 checkout 失敗、不 fetch，失敗的 `worktree add` 留下的部分由 git 自己移除；checkout 已建立而 cleanup 註冊失敗時，由 Controller 移除。審閱者在 snapshot 內以自己的 git 可讀 base 到 head 的 log 與 diff。
- 兩位審閱者以 `CollectAll` 平行（journal 的 group mode 與「一位等待時另一位已派送」的 barrier），各自 fresh session、`PWC_PI_CWD`、各自模型；task 帶 checkout、base、head，不帶 worktree，Inputs 為 input 與 implement。各自的 schema 錯誤（例如有 `line` 沒有 `path`）與內容驗收違規在同一 handle 上 repair（一般派送文字，不設 `RepairKeepsWork`），role、implement Ref 回抄與重複 finding ID 的診斷字句由 `TestCheckReview` 釘住。任一位 repair 用盡或 provider 失敗即 run 失敗，不記 `pre-pr-reviewed`、不交付 Final，另一位已接受的 findings 仍在 Outputs 且不被取消；兩位都失敗時以 basic 的失敗分類，另一位只在訊息中（兩種先後組合）。審閱者 session 的 close 未確認（runtime 回報 Wait 未完成，或 close 超過 cleanup 期限而 engine 回傳未分類的錯誤）為該審閱者 phase 與 handle 的 `CleanupFailed`，已接受的 findings 保留。A2 中取消為 Cancelled（130），已接受的 findings 仍在 Outputs；`pre-pr-reviewed` Decision 失敗時 Outputs 同樣保留、不交付 Final。兩位都確認退出後，Controller 經 registration 重讀 worktree HEAD：未 commit 的改動不影響，HEAD 移動是具名錯誤（`errHeadMoved`），不記 `pre-pr-reviewed`、findings 保留。每個結束路徑都確認三個 session 的 process 退出、worktree 保留；snapshot 在成功時由 workflow 於兩位 strict close 與 HEAD 重讀之後、Decision 之前移除（workflow 返回時已不存在；Decision 失敗時返回也已不存在，證明移除在 Decision 之前），失敗與取消時由 run 的 cleanup 移除。A2 之後 implementer 的 session 仍開著，交給更正 Step。額度：A2 的 live 與 sessions 任一少一即 run-scope `LimitExceeded`。
- 更正 Step 只在 A2 成功後執行，沿用實作者的 handle（一個 session 服務 A1 與更正，journal 只記一次 `SessionClosed`，位於 `pull-request-verified` 之後、`RunFinalizing` 之前）；task 的 role 與 A1 同為 `implementer`、帶 worktree，Inputs 為 input 與 implement，Feedback 的 message 固定、Refs 為兩份 findings。Schema 錯誤與處置驗收（漏一則 finding、不存在的 finding、重複，以角色加 ID 對應；診斷字句由 `TestCheckDispositions` 釘住，`TestPublishRepair` 只跑 schema 與一種處置違規）在同一 handle 上 repair，repair 派送使用 `RepairKeepsWork` 的文字且仍帶兩份 findings Refs；Agent 只 push 與開 PR 一次，被拒 attempt 的 receipt（含錯誤的 head）不核對，PR GET 只發生一次。
- PR receipt 在 repair 迴圈接受最終 contract 後核對一次，順序為本機再網路，不 repair、不重試（`TestPublishReceiptRejected` 每條檢查一例，核對錯誤類別、診斷與未 repair）：回報的 head 不是 worktree HEAD；`committed` 而 head 是實作 head 或其祖先；`no_change` 而 head 不是實作 head（`no_change` 要求回報的 head 等於實作 head，且仍是 PR 當下 head 或其祖先）；PR GET 失敗（含 404：gh 的 stderr 不保留，分不出不存在與其他錯誤）、回應不是 PR 物件、缺 `draft` 或缺 `merged`、`head.sha` 不是 40 位小寫 hex（在用於 compare 路徑或記錄之前檢查，與 review 對 PR head 的檢查不再有差異；`pull-request` schema 的 `head_sha` pattern 與此相同，不更嚴）；URL 或 number 不符；base repo 不是目標 repo；closed、merged 或 draft；base 不是 default branch；head 在 fork；head branch 不是回報的 branch；回報的 head 不是 PR head 或其祖先（behind，或 ahead 但 merge base 不是回報的 head）；compare 失敗（回報的 head 不在 GitHub）或 status 不認得。不符為 `errReceipt`，查詢失敗或無法讀取為 `errUnverifiable`，都是 Failed（exit 1）。PR head 等於回報的 head 時不查 compare；較新時查一次並記錄兩者。`committed` 與 `no_change` 都可成功；成功時 Final 是 `publish`，`pull-request` 只記錄 PR 的 URL、number、head branch、GitHub 當下的 head、回報的 head 與 compare 狀態，所有 gh 呼叫都是 GET。
- 更正 Step 起的失敗交付 `external-objects` 為 Final 並回傳原始錯誤（分類不變），範圍包含 repair 用盡、provider 失敗、receipt 不符或無法核對（`TestPublishFailureRecordsExternalObjects`、`TestPublishReceiptRejected` 的第一例逐欄位比對記錄），以及核對通過之後的 `pull-request` Attach 失敗、`pull-request-verified` Decision 失敗、嚴格關閉的逾時或未確認、關閉後 HEAD 移動（`TestPublishTailFailure`；這些路徑的移除旗標都未設定，worktree 保留）。記錄 session 是否確認退出、經 registration 讀出的 HEAD 與 branch（checkout 的 `.git` 被改指他處時仍讀 registration），以及該 branch 在 GitHub 的 tip 與其 PR；branch 未 push 為 `absent`（只認完全相同的 ref，名稱以它為前綴的其他 branch 不算）；detached HEAD 沒有 branch 可查，兩者記為 `unknown`。另記最後一份通過 schema 的更正 contract（被拒者亦同，其 Ref 列在 Outputs 的 `publish`）的回報 branch、head 與 PR，並以唯讀 GET 查該 branch 與該 PR number：Agent 在 detached HEAD 下以 refspec 推到另一個名稱並開 PR 時，只有這份回報值涵蓋它。查詢失敗記為 `unknown` 與診斷，不改主錯誤；只記錄不判斷，不關 PR、不刪 branch。取消（130）與 run-scope `LimitExceeded`（更正 Step 派送前，或已 push 與開 PR 之後 `pull-request` Attach 時）都不做這份記錄，錯誤也不附加其他失敗。
- 收尾順序：receipt 通過 → `pull-request` Attach → `pull-request-verified` → 嚴格關閉 session（逾時或未確認為實作者 phase 與 handle 的 `CleanupFailed`）→ 經 registration 重讀 HEAD（在 PR GET 期間被移動時為 `errReceipt`）→ 只在 workflow 最終成功返回處設定移除旗標。Workflow 返回時 worktree 仍在，run 的 cleanup 之後 checkout 與 registration 移除、bare repository 保留；其他結束都保留 worktree。旗標只保證 HEAD 的 commit 在核對時已在 PR branch 上（PR head 或其祖先）：未 commit 與 untracked 的檔案以及該 worktree 的 reflog 會隨移除消失，本機 branch 與 objects 留在 bare repository，脫離 process group 的 Agent 子孫不在保證內。額度：成功 test 恰以 3 live、3 sessions、6 attempts（input、A1、兩位審閱者、更正、`pull-request` Attach）執行，attempts 少一即 run-scope `LimitExceeded`。`Definition` 的 policy（3／22／80、8 小時、`Runtime.CleanupTimeout` 2 分鐘）與 Description 由 `TestDefinitionPolicy` 釘住；2 分鐘是 deadline 而不是移除的上限（`RemoveAll` 不可中斷，逾時時 checkout 可能已部分刪除、registration 殘留，終態 Succeeded 而 exit 1）。
- Receipt 核對防的是錯誤與謊報，不防同一使用者權限的 Agent 偽造：worktree registration 與 repository 目錄（HEAD、`commondir`、`shallow`、grafts、refs）都可由 Agent 寫入，改寫 `shallow`、grafts 或 `commondir` 能改變讀出的答案。「新 commit」只指 HEAD 不是 base 或其祖先，不證明該 commit 由 Agent 在本 Step 建立，也不證明它是 base 的後代。
- 未經 live 驗證：模型遵循 `code-change.implement.v1` 與回抄 Ref 的能力、gap 判斷品質；repair 派送文字是否讓模型不再重做 commit；Agent 在 CWD 之外以絕對路徑操作 worktree 時各工具（檔案工具、git、build、test、依 CWD 找設定的工具）的行為；全域的相鄰 repo 探索規則是否觸發；Agent 端 git 的 identity、簽章、hooks、LFS 與 partial clone lazy fetch 的認證；`permissions.push` 與實際 push 能力。寫入範圍只靠 Step requirements：Agent 的工具能以絕對路徑在 worktree 以外的 repository 建檔並以使用者全域 identity commit，Controller 不阻擋，也只核對 worktree 自己的 HEAD。審閱者的唯讀範圍同樣只靠 requirements，不是 Controller 的保證：Controller 只在審閱後核對 worktree HEAD 沒有移動，不偵測 snapshot、worktree 內容或其他 repository 的改動。Snapshot 的內容不是雜湊綁定的：objects 經 alternates 讀自 Agent 可寫的目錄（該目錄的 alternates 又可串到任何目錄），git 讀取時不驗證雜湊，與 receipt 同屬「同一使用者權限可偽造」的範圍；snapshot 沒有 remote，blob-less 取得下 base 之前的歷史可能缺 blob，需要它們的 git 操作會失敗。`--attr-source` 只加在 code-change 的 Controller git；review 的 checkout 尚未加，列為後續。模型遵循 `code-change.review.v1`、回抄與 findings 的品質、兩個模型在 snapshot 內的工具行為，以及大型 repository 的 snapshot 建立時間與磁碟用量，都未經 live 驗證。更正 Step 的寫入範圍（一個 branch、一個非 draft PR 只設 title／body／base／head、ticket 轉移）只靠 Step requirements。不核對的範圍與原因：其他 ref 與 default branch 的 push（其他人或自動化也會推送，無法歸因給 Agent）；PR 的 label、reviewer、assignee 與 comment（repo 自動化或 CODEOWNERS 也會加）；ticket 轉移（Controller 沒有 ticket 系統的 client，只記在 Agent 報告）；其他 branch 上另開的 PR、repair 中重做的 push 或重開的 PR（回報仍會一致）。未經 live 驗證：模型遵循 `code-change.publish.v1` 與處置的品質；A1 與更正 Step 共用 handle 時更正 Step 的 context 容量；Agent 以自己的認證實際 push、開非 draft PR 與 ticket 轉移；git 流程 skill 與常數衝突時（詢問使用者、暫存檔寫到 worktree 外）的實際行為；`RepairKeepsWork` 在外部寫入之後是否真的不重做；剛 push 後 PR 的 `head.sha` 是否延遲（不做讀取重試）；compare 對大差異的回應大小與速度；`git/matching-refs` 是否分頁（Controller 只讀一次回應；若分頁，名稱以目標 branch 為前綴的 branch 很多而目標 ref 不在第一頁時會被記為 `absent`）與 branch 的 PR 列表只讀一頁 100 筆；成功時移除大型 worktree 是否在 2 分鐘內完成。旗標設定之後 engine 仍可能因取消或 run deadline 判為 Cancelled／TimedOut 並移除 worktree，這是承認的殘餘窗口（run 目錄報告保留），沒有 test。A3 Step 逾時（45 分鐘）的路徑只由同一失敗分支涵蓋，沒有以真實逾時執行；被 run deadline 截斷時已發生的外部寫入沒有記錄。

## 7. 發布前 privacy／安全 scan

發布候選只包含通用 source/docs、匿名可重現 fixtures/tests 與公開產品配置。以下是檢查策略，不會自動刪除資料、重寫歷史或授權發布。

1. **列出實際發布集合。** 唯讀查看 working tree、index、untracked/ignored files、symlinks 及 artifact packaging 規則。不要只掃 README 或只相信 ignore；已 tracked 的 secrets/report 不因 ignore 而消失。
2. **掃內容與路徑。** 以文字／secret scanner 搜尋 credentials/private keys、auth headers、email、公司名稱/domains、真 PR/ticket URLs、絕對 user/source 路徑、真 task/run/session IDs、PID、實測日期、commit/source hashes、RPC traces/history、reports/manifests。數值／格式命中須人工分類，公開 dependency/model 版本、匿名 fixtures 與 runtime field names 不自動當洩漏。
3. **檢查所有格式。** 包含 Markdown、Go tests、JSON、embedded skills/scripts/resources、hidden files、binary/archives；不只掃副檔名。不要執行未知 repo 程式碼來做安全掃描，也不把私有內容上傳外部 scanner。
4. **檢查本地 links。** 對每份 Markdown 的相對路徑依文件所在目錄解析；確認檔案存在、anchors 有效，沒有已刪報告或私有備份回指。範例使用 placeholders／`github.com/owner/repo`，不使用真任務識別或版本 hash。
5. **另查 Git history。** 工作樹乾淨不代表歷史安全；掃即將發布的所有可達 commits/branches/tags、path names 與 blobs，另查交付 archive 是否夾帶 `.git`、bundle、ignored binary/logs。若需移除私有歷史，先取得獨立授權並在 repo 外確認受限備份，不能只刪當前文件便宣稱歷史已清。
6. **內部名詞 gate。** 公開 repo 不得含公司內部系統、架構、主機與環境名稱。詞表含這些名詞本身，因此只存 repo 外私有目錄（`$PWC_TRIAGE_SKILLS_DIR/denylist.txt`），不得複製進 repo。格式：`[hard]`／`[soft]` 分段，每行一個不分大小寫、以字詞邊界比對的 regex，`#` 開頭為註解；hard 命中即失敗，soft 命中列出供人工判斷。以 `python3 -B testdata/ci/terms.py` 掃 tracked 檔案的 index（staged）與 working tree 兩個版本、未忽略的 untracked 檔案、symlink 目標及路徑名，只輸出位置（含 index／worktree／symlink 版本標記）與詞表行號；預設位置找不到詞表時結果為 SKIP（exit 3），**不算通過**；明確指定的 `--list` 不存在、未知或帶註解的段落標頭、沒有任何 hard pattern，或 index 物件缺失都是失敗。`\b` 把底線與數字視為字詞字元，`term_x`、`term01` 這類識別字形式不會被 `term` 命中，詞表須自行列出。CI 沒有詞表，只跑 `terms_test.py` 的匿名 parser／比對測試；實際 gate 在本機發布前執行。
7. **人工覆核與 release gate。** 對 scanner 命中逐項核對，無法確定即 blocked；確認無敏感資料、broken local links 或誤導性驗收宣稱再發布。安全 scan 不等於 tests/build/review 通過；每組單獨判定。

實際 scan finding、檔案 manifests、執行命令/輸出、日期、SHA 及稽核結果只寫 repo 外受限紀錄，不將本機報告、真實 traces/history/email 或私有備份位置加入 commit。若發現洩漏，不將原文複製到 issue/PR，依授權處理。

## 8. 保留的限制

Numeric validator 極端可表示性／boolean inversion／error-tree 放大與非協作取消、完整 snapshot 重寫 I/O、永久 blocked stdout/filesystem 的非固定 wall-clock、任意 detached 子孫、Controller abrupt death，均保留 [DESIGN](../DESIGN.md) 的限制。不承諾 sandbox、supervisor、crash resume、exactly-once、外部副作用 rollback 或同 UID 惡意隔離。未知／未執行情境須明列，不以「全部通過」掩蓋。
