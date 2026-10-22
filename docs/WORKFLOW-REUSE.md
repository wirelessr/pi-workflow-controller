# Workflow 重用約束與 refactor 計畫

本文件落實 [專案憲法第七條](../AGENTS.md)，是開發、review 與接手的必要入口。規範共用機制的收斂順序，不新增 WorkflowBase、DSL、工具權限框架或外部 workflow config。

**規劃中的 API 不等於已實作。** R1 已接入共用 runtime，R2 已提供共用 report 判定，R3 已共用 publication 投影與 bounded-read loop，R4 已遷移指定 host transport consumers，R7 已將 gh／git 命令執行接入 runtime、bare repository 機械接入 gitrepo，R6 已將 review 流程抽成可組合單元，R9 已將 contract repair 迴圈抽成 engine 方法，實際入口見下表，具體名稱以完成後的 source 宣告為準。已有 engine 原語不能因本計畫重新實作。逐次執行狀態、SHA、review/gate logs 留在 repo 外指定工程紀錄，不放進本文件。

## 1. 單一規範來源與接手 gate

- `AGENTS.md` 定義最高原則；本文件定義共用邊界、相依及完成條件；各 workflow 文件定義業務 contracts。執行紀錄只記目前進度與證據，不另複製一份可能漂移的規格。
- 開工前核對目前 HEAD／working tree、實際 exports、立即 callers、既有測試及工程紀錄中已完成的單元。計畫、檔案存在或舊 PASS 不能當成某個新 tree 已完成。
- 每個單元須交代四項：**使用哪些既有能力、增加哪些業務資料／政策、是否改變共用保證、哪些舊 consumers 要一起遷移**。缺這份對照時不開始新增 helper。
- 缺少共用 API 時，先指出具體能力缺口與現有實作，做最小收斂；不得複製後改名、只因 map/slice 或 schema 不同再做一套，也不得跨 import 另一 workflow 的業務 package 解決共用問題。
- 相依未完成時停在該單元，不能在新 workflow 私藏替代版本以繞過。調整相依或保證需要先確認原因，更新本文件與工程進度；不能默選較寬鬆版本。
- 已完成項目不重做：接手以 source／採證確認後續用。若發現真實 regression，限定修復，不回復歷史版本、不重跑整份靜態規劃。

## 2. 現有能力登記與不可重造項目

實作各 R 單元後，更新此表為實際共用入口及已遷移 consumers；未完成前不要填假定函式名。

| 能力 | 現有入口／位置 | 決策 |
|---|---|---|
| Run、session、Step 與 limits | `engine/api.go`、`session.go`、`step.go`、`policy.go`；`Run.SessionIdentity` 讀 owned committed identity；`Run.WorkflowInput` 以值回傳 immutable workflow/input | 直接重用；M4 dispatch 前身份沿中性入口，不另造 session driver／會計；`callerRequest` 使用 input 投影，無需複製歷史 |
| Pi 預設 cwd | `engine.Options.PiDefaultCWD`／`Run.OpenSession` → `runtime.SessionSpec.CWD` | CLI run 讀 `PWC_PI_CWD`，engine 在 New 依 LaunchCWD 固定相對值；triage／smoke 沿 fallback，review explicit workspace 保留，僅選用的 default NUL 提前拒絕，explicit Role.CWD／fallback LaunchCWD 保留原 runtime 驗證與會計。Task workspace 與來源唯讀要求留 triage，不新增 runtime prompt 或 sandbox |
| Child、Parallel、Retry、Decision | `engine/scope.go` | 直接重用；workflow 決定批次、依賴及可重做分支 |
| OS File.Sync 依賴 | `engine.Options.SyncFile` → `engine.New` → 原 `Run`／`contract.Options.SyncFile`／`NewStore`；Store 的既有 writers 及 Stage copy | 正常逐 instance constructor DI，兩個入口的 nil 都正規化為真 `os.File.Sync`，建構後 private 固定；schemas、run/input、RunCreated 及後續寫入使用同一依賴。既有 CLI／review／triage callers 留 nil，無 fast mode／CLI/env 開關。只替代 Sync 結果，不重造 filesystem／Store／journal；真 durability/fault/order gates 保留 |
| Stage／Confirm／Publish、committed resolver | `contract/`、`engine/resolve.go`、`step.go` | 直接重用；禁止第二個 Store／commit／journal |
| Result／FinalSelection／FinalDelivery | `engine/api.go`、`resolve.go`、`run.go` | 直接重用；禁止另一個 final artifact registry |
| Context usage 與 cleanup report | `engine/session.go`、`runtime/types.go` | M3 triage 沿 SessionContextUsage／既有 strict-close handoff 接入明示容量政策；sample 非 provider admission |
| Discovery 自動 preflight | `runtime.New` 固定有效 BridgeDir；`Pi.Start` → `runtime/preflight.go` 的 `preflightDiscovery` | R1 已接線：review 私有 guard 已移除；smoke/review/triage 共用 Start，仍須部署版本核對/live 驗收 |
| 嚴格收尾確認 | `runtime.CleanupReport.ConfirmsLocalClose(expectedSessionID)` | R2 已接線：triage slice／Planner 共用；caller 先處理 CloseSessionReport error，普通 CloseSession 不改義 |
| Envelope／file consumer | `contract.FileEntry`／`Publication[T]`／`DecodePublication[T]`；review `readReviewContract`、triage `readAccepted` 經 `engine.ReadContract` 後消費 | R3 已接線：只共用投影與 fresh decode，不新增授權；review ID 索引、triage lineage/cache 留 caller |
| Published／checkout 二次讀檔 | `contract.ReadBounded`；review `readCheckFile`／`readPublishedFile`、triage `rawFile` | R3 已接線：共用 bounded loop，root/open/identity 與錯誤政策留 adapter；Store `copyStable` 的雙 digest 不合併 |
| RPC 測試 transport | `testutil/protocol.NewHost`／`Host.Events`／`Event.Reply`／`Host.Close`、`RegisterCleanup` 及 `WriteEnvelope` | review workflow/check integration、triage RPC driver 共用 host 與 late-bound cancel/close/join；原 domain scenarios 與 Store-only 分層不變 |
| Skill extraction／report renderer | `contract/reportresource.ExtractFresh`／embedded `pwc_report_io.py`；review `ExtractSkills`／renderer 與 triage `ExtractReport`／report Step | R5：兩個真 consumer 共用機械，業務模板／report binding 各自保留 |
| Workflow 組合單元 | `review.Run(ctx, r, scope, name, prURL)`；`code-review` 的 `execute` 以 `r.Root()`、`"review"` 呼叫它 | R6 已接線：舊內聯副本移除；呼叫者只用 `review.Run`，不 import review 其他業務型別或 helper；不新增引擎層 sub-workflow 原語。預期 head 與外層准入留 C3 |
| Node TLS 前置檢查 | `runtime.CheckNodeTLS`（`runtime/preflight.go`） | R6 已接線：需要此檢查的 workflow 在第一個副作用前呼叫（目前為 code-review；code-change 是規劃中的第二個 consumer）；不屬於 Pi 啟動，triage 與 smoke-echo 不呼叫 |
| gh／git 命令執行與 bare clone | `runtime.GitHubGET`／`runtime.Git`（`runtime/command.go`）；`gitrepo.Reserve` 與 `Repo.Init`／`AddRemote`／`FetchPinned`／`Cleanup`；review `acquire`、`Verify`、`Checkout.Cleanup` 使用 | R7 已接線：review 舊副本移除；只限固定 gh REST GET 與 git，不提供任意執行檔；唯讀政策留 review；可寫 fetch 與 GraphQL 由第一個使用它的 C 單元加入，不 import review 私有實作 |
| 私有 skill 目錄展開 | `contract/skilldir` 的 `Read`／`Source.Expand`／`Abandon`；triage `PrepareSkills` 使用 | R8 已接線：triage 舊內聯副本移除；子樹清單、環境變數、展開目錄名、Attach 與 record schema 由各 workflow 宣告；manifest 與上游比對留在 triage |
| Contract repair 迴圈 | `engine.Scope.StepWithRepair`；triage `TaskSession.Run` 使用 | R9 已接線：triage 舊內聯迴圈移除；規則見該方法的 doc comment |

表中的 triage consumers 指已刪除的舊版 Planner triage；共用入口仍由其他 consumers 使用。v2（`internal/workflows/triage`）目前使用 `engine.ReadContract`＋`contract.DecodePublication`（`readAccepted`、citation 檢查）、`contract.ReadBounded`（`rawFile`，與舊版相同的二次讀取 adapter）、`reportresource.ExtractFresh`（`runReport`）、`skilldir.Read`／`Source.Expand`（`PrepareSkills`）、`contract.ReadStable`＋`skilldir.Abandon`（上游比對），以及 `Scope.Attach`、`Scope.StepWithRepair`、`StepSpec.Observe`。上述相對路徑均位於 `internal/`。角色模型、PR Pin、code-location Evidence、investigation Ref/file Evidence、Source status 及 verdict 不屬於通用登記項，保留 workflow-specific 語義。

## 3. 共用 refactor 單元與順序

預設順序為 **R1 → R2 → R3 → R4，再進 triage M1**；R6、R7、R8、R9 服務 code-change（R7 先於 R6），與 triage 的 milestones 互相獨立。每項是可獨立 review／驗收的最小單元，不合成一次大重構。R4 可為前面單元的測試需求先行，但須記錄順序調整，不能因此改變依賴或跳過 R1–R3。R5 延至 M6，不預造第二個 renderer。

### R1：共用 persisted-start preflight

**已實作接線：** runtime.New 將相對 BridgeDir 依 Controller 建構 cwd 固定成絕對路徑；每次 Pi.Start 在版本檢查成功後、session 目錄／persisted child 建立前呼叫共用 guard。拒絕採既有 BridgeUnavailable，phase=preflight、Origin=Protocol、DispatchAccepted=No；parent typed cause 沿用原 startupError。已配置的 handle 保留 total-session 會計並關閉，不虛構 Wait，不配置 attempt。Guard 以 NONBLOCK／NOFOLLOW 及 bounded read 維持原 regular-file／1MiB 限制，檢查迴圈前後的取消；不聲稱 filesystem syscall 可強制中斷。Review 舊 guard 與獨立目錄解析已移除，其他 Close／commit／領域語義不改。

以下保留目標與完成條件，逐次驗證證據在 repo 外；匿名回歸不代表 live 驗收。

**目標與責任層：** 在 runtime 使用已解析的有效 `BridgeDir`，於每次 persisted child spawn 前執行共同唯讀檢查。不是每個 workflow 自行決定是否呼叫的可選 wrapper。

- 遷移 review-private 的可程式化 parent PID／`.recovering` 判準；smoke-echo、review 及後續 triage 的 Controller-owned startup 都走同一邊界。
- 只檢查，不清除、不 claim、不 resume 他人 session。不增加跨 process 鎖或 supervisor，preflight 不是鎖。
- 部署版本／實際 recovery 判準的人工核對仍保留。不能因移入 runtime 就聲稱支援所有 Pi/WebUI 版本。
- 遷移前只有 review 另有自動 preflight，其他入口需要操作前核對；現況 README 已改為共用 runtime 自動覆蓋，仍保留部署核對。R1 **不是既有全面自動保證的零行為搬移**。
- 實作前明列 smoke 新增拒絕條件、review 的失敗時點／error code/phase、OpenSession 計數與 journal 影響。不得自行退還已消耗 session 額度，或以改寫舊 Close／fatal 語義解決差異；未確認的必要語義變更先提出。

**完成出口：** 所有 Controller-owned persisted-start callers 實際受同一檢查；BridgeDir 不再由 review 另解一份；舊 private 實作移除，必要 compatibility wrapper 只能轉送。使用隔離的匿名 discovery fixtures 驗正常／阻擋／取消／錯誤、spawn 前停止、有效目錄選擇與原 startup/cleanup/accounting 行為。更新 README 與 workflow 現況說明。未授權 live 就標未驗，不用測試 bypass flag 移除產品 gate。

### R2：嚴格 local-close 確認

**已實作接線：** `CleanupReport.ConfirmsLocalClose` 共用原 SessionID 相等、WaitCompleted／ProcessExited 為真及 Unconfirmed／WaitError／KillError／DiscoveryError 為空的判定。Triage sliceStep 與 Planner close 已移除重複 predicate，先原樣處理 CloseSessionReport error，再呼叫共用方法；Planner 額外的非空 session ID 要求仍在 caller。此方法只比較傳入 ID，不另驗其有效性（兩個空 ID 仍相等）、不比較整份 Identity、不執行 cleanup；普通 CloseSession、closeOnce／會計、accepted state／handoff 政策及原錯誤不變。

以下保留目標與完成條件，匿名／live 證據仍分開。

**目標與責任層：** 在 runtime CleanupReport／engine session 邊界提供明確較強的共用判定或 opt-in helper，遷移 triage 的兩個相同判定。

- 保留預期 session identity、WaitCompleted、ProcessExited、Unconfirmed、WaitError、KillError、DiscoveryError 的要求及原錯誤 chain。
- 普通 `CloseSession` 與嚴格確認目前不是同一保證。保留舊 API 意義；review 不因 DRY 被默默改成另一種失敗策略。
- 是否已存在 accepted state、何時 handoff／重做、用哪份 state、remote async job 是否已停止，仍由 workflow 決定。

**完成出口：** triage slice／Planner 都呼叫共同嚴格判定，原重複 predicate 移除；用真 session/report 邊界測 identity、Wait、close errors、同 run 會計及 stopped/last 行為。普通 close regression 與嚴格 close regression 分開，不能拿本機確認當遠端 job 結束證明。

### R3：typed publication／FileEntry 與讀檔機械共用

R3 是單一實作／review／commit 單元，以下兩組驗收要求須一起閉合，才算 R3 完成；不再拆成前後兩個 milestones。合併施工單元不代表強制統一不同信任邊界的 reader，也不刪減原有差異矩陣、consumer 遷移或回歸要求。必要的窄 adapter 仍可保留，但須記錄具體保證與 callers。

#### 驗收一：typed publication／FileEntry 共用

**已實作接線：** FileEntry 只在 contract 宣告；Store／workflow 的本地 type aliases 保留原拼字，無第二套欄位。DecodePublication 只解碼 Data/Files，不驗 schema 或授權；兩個 workflow 仍先經 engine.ReadContract，triage 僅在原同步 pass 重用 verified bytes，每次 fresh decode。Review 的 map/ID checks 留原 caller，engine.Decode 的 UseNumber 語義不變。

**目標與責任層：** 共用 contract file 表示及經 engine committed resolver 的 typed publication 消費入口。依據 review／triage 的實際兩個 consumers 設計，不建立只有預想 callers 的泛型框架。

- 遷移兩邊同形 file entry、envelope 解碼及必要索引接線；不能只新增 helper 但保留兩套各自演化。
- Full Ref/schema binding、取消、錯誤分類及每次 decode 的可變資料隔離不變。
- Map/slice、required artifact/evidence、review 的額外 ID 規則與 triage lineage／scope 驗收，依語義留在 workflow 或窄 adapter，不變成全域 verdict。
- Triaged acceptance 只能重用同一次同步驗收的 bytes。第二 consumer 未需要相同 cache 前，不強迫加入；不得做跨 Step／session／Decision 的永久 cache。

**完成出口：** review 與 triage 都使用真共用入口；原 consumer 的錯誤／ID／Ref／Files 行為有對照 regression，cache 不跨邊界，Store-only publication 不獲 engine 授權。記錄必要的 source/API 相容性差異與實際 callers。

#### 驗收二：published-file 與 checkout reader 的安全邊界

**已實作接線：** ReadBounded 重用 contract 的 contextReader，每次最多讀 32 KiB，總量最多 limit+1，保留 partial I/O bytes 與原 cause。Review 使用每塊取消並自行將錯誤轉成 nil bytes；triage 明確保留 read 前後取消與 I/O-error 優先序，因此共用 loop 不額外介入其取消時點。兩個 adapter 保留原 root/open/stat/identity、size 與錯誤文字；不增加 NOFOLLOW 或 SameFile 到 triage，不改 Store 的 streaming/double-digest 驗收。Root 內可解析 symlink 仍依原 reader 政策，不等於 Store 允許 symlink publication。

**目標：** 減少 bounded/cancellable 讀取機械重複，但不將不同信任邊界混成一個含糊 helper。

實作前必須列出差異：
- Review reader 亦服務 checkout code，具 root containment、regular-file、NONBLOCK、開檔前後 SameFile、逐塊取消與大小限制。
- Triage `rawFile` 是二次讀取，沒有自身 NOFOLLOW／開檔前後 SameFile，取消檢查在 ReadAll 前後。
- Store 的 no-symlink、NOFOLLOW、metadata 與雙 digest 是更強的 publication 驗收，不能假設第二次 open 自動繼承。

**完成出口：** committed-file 消費保留明確授權入口，raw checkout reader 保留適用策略；只抽真正同質的底層讀取，保留需要不同的 adapter 並說明理由。驗 path／symlink／FIFO／identity change／size／取消／I/O failure，補 source/evidence 原有回歸與 I/O 成本比較。不得降低 Store 保證，也不得以增加不必要的整檔重讀換取表面 DRY。

### R4：共用 host RPC test transport

**已實作接線：** Host 擷取原非同步 listener／per-peer decoder／fan-in 機械，保有 fixture deadline、原 peer 的序列化 Reply 與冪等 Close。Close 先解除 queue 發送、停止接受，再關自有 sockets 並 join pumps；不代替 caller Cancel／join 真 engine，也不代表 process Wait。非 EOF／net.ErrClosed 的 decode/accept errors 同時保留給 Events 與 Close，取消或 queue 滿不再靜默遺失。Triage control socket 新增 fixture deadline 是明列的測試邊界改變，不是產品 timeout 政策。

三個指定 consumers 已移除私有 host pumps／connections／join 副本。`RegisterCleanup` 於 cleanup 當下呼叫 caller 的 stop，再 Close、即時回報 close error，最後才等待非 nil completion channel；保留三處取消次序、parent cleanup LIFO、原診斷及 Close 後的 10/10/8 秒 join 界線。Host 不持有 engine，未啟動 Run 不等待預建 channel。WriteEnvelope 只寫 caller 明列的 meta/data/files，不猜檔案種類、不驗授權、不更改 nil entries，允許原故障 cases 宣告不合法內容。正常 child WriteCandidate 也使用它。Engine 的同步 handoff／worker-limiter socket tests 保留逐 socket Accept/Decode barriers，runtime fixtures 使用不同 control/HTTP 協定，兩者不強制改成 eager fan-in。

**目標與責任層：** 擴充既有 `internal/testutil/protocol`，共用 listener、event pump、connection ownership、deadline、cancel/close/join 及明列 file entries 的 envelope writer。

- 至少遷移 review workflow fixture 與 triage RPC driver；review check tests 的同質部分一併接入，或列出不可共用的具體差異。
- 非 EOF decode error 不得默默消失，失敗 assertion 也要釋放自有 children／connections 並 join。
- Registry/model、scenario、role order、checkout 存活、Ref/history、故障注入與業務 assertions 仍屬各測試。不提供替代 Step／validator 的假 workflow runner。
- 保留真 engine/Store/protocol 與已建立的 Store/publication 分層；不把 Store publication 當 committed Ref，不因抽 harness 刪 case 或只驗相同總數。

**完成出口：** 指定 consumers 不再各保有相同 host pump/lifecycle；逐案 mapping、typed failure、資源 ownership／取消與完整一般/race 回歸一致。效能以整套同類情境比較，不因共用程式就預告加速幅度。

### R5：report／resource 機械部分

**已實作接線：** `contract/reportresource.ExtractFresh` 共用既有 OpenRoot、fresh directory reservation、O_EXCL copy、0700/0600 與 partial 留存；review `ExtractSkills` 與 triage `ExtractReport` 實際使用。共用 embedded `pwc_report_io.py` 保留 descriptor/NOFOLLOW/NONBLOCK、bounded JSON、duplicate keys、Ref identity、candidate nlink、exclusive report 及原 fd rewrite；review 私有副本移除，Pin／roster／模板與原 appendix 驗收不變。不提供 plugin、installer 或另一個 final registry。

Triage 已有可呼叫的 `plannerCaller.report`，以既有 Planner 的正常 Step 產獨立 report contract／artifact，前後均驗 committed state、真正歷史 assessment owner、同版 claim／delivery／evidence 與 producer，驗收後 Decision 才回 Ref。Caller 使用原 Result／FinalSelection。固定 host Python 只執行 embedded 模板，重算完整預期 bytes；不執行 Agent 可修改的 extracted 檔案，不另抄 Go 模板或另開 writer Agent。Context/ledger/checkpoint/owner registry 留 committed 工作資料，報告只投影調查內容及必要 Ref/file 追溯。

Triage producer 每個 Ref 用局部 ExitStack，讀完即關閉；candidate/request 保留原生命週期。Host CommandContext／Run／WaitDelay 保有錯誤與 Wait，named buffer 強制 stdout 64 MiB／stderr 64 KiB 限額，不透過 promoted ReaderFrom 繞過。這不是 Python RSS 上限、filesystem sandbox、兩檔 atomic 或 live 容量承諾；write/close 的所有 EIO 與替換競態未窮舉。

R5 提供真 report 操作及 shared consumer；M6 已在明示政策的新入口接 adaptive yield、歷史 failure 處置、預留額度與最終 outcome，舊 M5 state-only 出口未改。以下保留 R5 完成要求：

**完成出口：** 原 review 仍能使用同一機械能力，triage 報告也實際接入；模板、Pin／claim、verdict及追溯資料保持各自 contracts。保留 exclusive output、大小／路徑／identity檢查與已提交 Ref 驗收。沒有第二 consumer 時保持延後，不建立 renderer plugins、installer 或新 final registry。

### R7：gh／git 命令執行與 bare clone 機械共用

服務 [CODE-CHANGE](CODE-CHANGE.md)。以其 P0 為前置，先於 R6（兩者都改 `review/acquire.go`）。

**已實作接線：** runtime `command.go` 承接原 acquisition 的 process 執行：`gh`／`git` 自 PATH 啟動、每命令 20 分鐘 deadline、stdout 64 MiB 上限（cause 為 `ErrCommandOutputLimit`）、stderr 丟棄；移除繼承的 `GIT_*`、`GH_*`（保留 `GH_TOKEN`、`GH_CONFIG_DIR`）與 GitHub URL env，process group 在唯一 Wait 前 SIGKILL。錯誤仍是 untyped，字串不變，不含 argv 或 stderr。`GitHubGET` 只組固定 REST GET argv；`Git` 每次前置固定 `-c`：gh credential helper 三項，以及 `gc.auto=0`、`maintenance.auto=false`、`core.fsmonitor=false`。後三項防止 git 啟動 process-group kill 收不到的 detached 背景子孫，任何 Controller 端 git 都需要，與是否啟用全域 git config 無關。不寫入任何 git config，caller env 附在固定值之後。`internal/gitrepo` 只 import stdlib：`Reserve` 在 private root 下獨占建立 `repository.git` 與 `checkout` 並記錄 identity；`Init`／`AddRemote`／`FetchPinned` 經 caller 的 `Runner` 執行固定 git 序列，錯誤原樣回傳；`Cleanup` 以 SameFile 只刪保留的 worktree 與其 registration。Review 的唯讀 `-c` 清單、隔離 git config 的 env、detached worktree、remote 名與 URL、comments 分頁驗收、快照與 `Verify` 留在 review，`Checkout.Cleanup` 只轉送。測試的假 gh 改由 PATH wrapper 提供，仍是真 subprocess。URL userinfo 拒絕與傳輸 `-c` 未納入共用層。

**明列的行為差異：**

- Git argv：上述六個固定 `-c` 移到 argv 最前，review 的 `-c` 清單不再含它們，語義等價（`credential.helper` 的空值重設仍先於 gh helper；review 不另設 credential、gc、maintenance、fsmonitor 鍵）。它們由 runtime tests 釘住。Review 的 `gitOutput` 測試 seam 位在 `runtime.Git` 之前，所以 seam 收到的 argv 中這六項是消失，不是換位置。
- 新拒絕條件：`FetchPinned` 在執行任何 git 前拒絕非 40 位小寫 hex 的 SHA、不以 `refs/` 開頭的 ref、空或以 `-` 開頭的 remote；`GitHubGET` 在啟動 gh 前拒絕空或以 `-` 開頭的 endpoint。Review 的 remote 名、`refs/review/*` 與 `repos/...` endpoint 不會觸發。Review 在呼叫前已把 metadata 的 SHA 轉成小寫，所以對現有 caller 這些條件都不會觸發，沒有行為差異。
- 不可達路徑：沒有 repo 的 `Checkout` 字面值若呼叫 `Verify` 或快照寫入會 nil panic。Production 只經 `acquire` 建構 `Checkout`；`check_test.go` 的字面值不呼叫這兩者；`Cleanup` 對沒有 repo 的 `Checkout` 仍回 nil。

**後續：** 失敗命令目前只回報 `acquisition command failed`，不帶 exit code 或 stdout。第一個需要它們的 C 單元新增 typed exit error，並明列錯誤分類的變化；R7 不預造。

以下保留目標與完成條件。

`review/acquire.go` 內有政策無關的機械：固定 argv 的 gh／git 命令執行、deadline、stdout 上限、stderr 不落地、憑證以 `-c` 逐次帶入而不寫入 config，以及 bare repository 加 pinned fetch（`git init --bare`、`git config remote.*`、指定 SHA 的 fetch）。其 consumers 將是 review 的 acquire、新的 PR 狀態唯讀 adapter（check runs、commit statuses、review threads）與寫入型 worktree。唯讀 PR 取得的政策（禁用 hooks 與 git config、detached）屬 review，不共用；寫入型 worktree 的政策（是否啟用 hooks 與 config）另行明列。

- 範圍：只限 `gh` 與 `git` 兩個固定執行檔的命令執行，不提供任意執行檔的 `Command(exe, args)`；ADDING 第 3 節與 IMPLEMENTATION 第 2 節明確不要泛用 command executor。
- 責任層：process 執行（命令、deadline、輸出上限）放 runtime；bare repository 與 worktree 的 ownership／SameFile cleanup 不屬 process 執行，於核對後另定所屬 package。兩者都不留在 review package 讓他人 import。
- Controller 不為 Agent 配置 push 憑證，不寫入憑證相關 config；push 與開 PR 由 Agent 使用其既有認證。bare repository 內的 remote 設定與 fetch 屬機械，不算憑證。這不是 credential broker。
- 寫入型 worktree 需要 fetch 可變的 default branch，acquire 明文只 fetch 指定 SHA；這是政策差異，須列入矩陣，不默默放寬 review 的限制。
- PR 狀態 adapter 需要 GraphQL 查詢（review threads 的 resolved 狀態），現有執行固定為 REST GET。矩陣須加「GraphQL 查詢」一列；「唯讀」定義為不改變外部狀態的查詢，不是只限某個 HTTP method。
- 差異矩陣須逐項列：取消、錯誤分類、ownership（cleanup 的 SameFile 等檢查）、path／symlink、限額、穩定性、生命週期。不得挑較弱版本統一，也不得把唯讀版更強的檢查默默套到寫入版。

**完成出口：** review 實際使用共用機械，舊副本移除；既有 acquire 的錯誤分類、限額與 cleanup 保證不變。只交付政策無關的核心；可寫 worktree 的 fetch 政策與 GraphQL 查詢在 code-change 第一個使用它們的單元落地並驗收，不在本單元預造。

### R6：review 流程成為可組合單元

服務 [CODE-CHANGE](CODE-CHANGE.md)。以其 P0 與 R7 為前置。

**已實作接線：** `review.Run(ctx, r, scope, name, prURL)` 承接原本內聯的 Prepare、三位 reviewer、Validate 本體，舊副本已移除；`code-review` 的 `execute` 以 `r.Root()`、`"review"`、`in.Prompt` 呼叫 `review.Run`。測試只經未匯出的 `run` 替換外部 Git 與 remote。`ExtractSkills(runDir, name)` 改收目錄名。scope、`name` 規則與衍生名稱、拒絕順序、Run 額度用量與結果語義的契約以 `review.Run` 的 doc comment 為準，本文不重述。`code-review` 的目錄、cleanup、scope path、`FinalDelivery.Scope`、Outputs、session 角色名、模型、prompt、Step deadline、Run policy 與其他錯誤字串不變。單元不讀 `Run.Snapshot` 做准入或預留，額度由外層 Run policy 承擔，不足時是 run-scope `LimitExceeded`。

**明列的行為差異：**

- TLS 錯誤字串：舊 `code-review blocked: Node TLS verification is disabled; launch with NODE_TLS_REJECT_UNAUTHORIZED=1`，新 `TLS preflight blocked: Node TLS verification is disabled; launch with NODE_TLS_REJECT_UNAUTHORIZED=1`。位置不變：URL 錯誤仍先於 TLS 錯誤，兩者都在任何目錄、session 或 gh／git 呼叫之前。
- Validate session 關閉：舊版從未由 workflow 關閉，留給 run finalization。新版在成功與 incomplete 路徑於 `report-accepted` Decision 之後由單元關閉，`SessionClosed` 事件因此早於 `RunFinalizing`。關閉失敗時：
  - 成功路徑由「Succeeded、交付 Final 加 cleanup error（exit 1）」變為 workflow error：只回傳關閉錯誤，outcome Failed、exit 1、不交付 Final。
  - incomplete 路徑由「交付 incomplete report 加 cleanup error」變為不交付 Final；回傳的錯誤以 reviewer 錯誤為 cause，只附上關閉錯誤的訊息，關閉錯誤不在錯誤鏈中，因此分類與 outcome 仍是 reviewer 的原分類（未分類的語義驗收錯誤仍為 WorkflowFailed）。
  - 兩者的 report Ref 仍在 Outputs；finalization 仍以 cleanup error 再回報同一個關閉結果。錯誤路徑不關閉，已開 handles 照舊由 run cleanup 處理。
- 新拒絕條件只對新呼叫者：不合格的 `name`（含結尾為 `-skills` 或 `-work`，避免一次呼叫的衍生名稱等於另一次的 checkout root）在任何副作用前以 `invalid review name "<name>"` 拒絕；`code-review` 傳 `"review"`，不觸發。不擋 engine 自用的目錄名，名稱與既有項目衝突時沿既有 Mkdir、`ExtractFresh` 或 `AddCleanup` 錯誤失敗，分類不變（skills 保留失敗仍是 StorageFailed）。

**剩餘缺口：** 預期 head（相等或祖先規則、compare 查詢、X 與 Y 不同時的釘選與限制）連同單元簽名變更在 C3 加入，R6 不預造參數。外層准入與預留、code-change 組合接線同在 C3 驗收。巢狀時 checkout 存活到外層 run 結束，單元返回後的 checkout 變動不再由 `Verify` 偵測。未被測試覆蓋：`execute` 傳入的 `r.Root()` 與 production remote 字面值（到達需要真網路，由 live 驗證），以及同一 Run 內以不同 `name` 實際呼叫兩次 `review.Run`。Go 層仍可 import review 的其他匯出 helper，「只用 `review.Run`」靠 review gate。對呼叫者的後果：同一 Run 可以不同 `name`、各自專用的 scope 多次呼叫 `review.Run`，同一 `name` 不得重複或並行呼叫；巢狀呼叫回傳的 Final 為 nil 時，單元可能留下最多 3 個呼叫者無法關閉的 live slots，呼叫者須結束其 Run，不再開 session。

以下保留目標與完成條件。

**目標與責任層：** 在 `review` package 內，把 Prepare、三位 reviewer、Validate 抽成以 PR 參數與子 scope 呼叫、Run 額度用量明文記載並由外層 Run policy 承擔的普通 Go 單元；`code-review` 自身遷移到該單元。單元是原擁有者匯出的完整流程層級 API，呼叫端不得 import `review` 的其他業務型別或 helper。不新增引擎層 sub-workflow 原語、registry 或 DSL。第二個 consumer 為 code-change 的 PR 後審閱階段。

**開工前必補的差異矩陣：** 輸入來源、目錄與資源名稱、scope 名稱（Decision、Child、Parallel、Retry 共用一組，Step 與 Attach 共用另一組）、預期 head、Run 限額會計、啟動前環境檢查的位置、report 與 branch error 並存的結果語義、模型來源、取消與 cleanup 所有權，以及 path／symlink、錯誤分類、穩定性、生命週期。

- 不降低既有保證。改變錯誤時點、會計或拒絕條件之處須明列並驗收，不稱零行為 refactor。
- 預設模型變更是獨立單元，不與抽取同批；它改變產品行為，須同步 `docs/CODE-REVIEW.md`、新增釘住模型 binding 的測試與 live 驗證。

**完成出口：** `code-review` 實際使用新單元，舊內聯副本移除；既有 workflow 契約與測試不變；新增可與外層名稱共存、巢狀 scope 與外層限額的回歸。以 code-change 組合該單元的接線在其 C3 單元驗收，不在本單元預造。

### R8：私有 skill 目錄展開中性化

服務 [CODE-CHANGE](CODE-CHANGE.md)。以其 P0 為前置。

**已實作接線：** 新 package `internal/contract/skilldir`（只 import `contract`、`contract/reportresource`，不 import engine 或 runtime）承接原本在 `triage.PrepareSkills` 內聯的來源讀取、放棄式期限與展開。`Read(ctx, source, dirs, sidecars)` 在同一次開啟、同一個 1 分鐘期限內讀 caller 宣告的子樹與 sidecar 檔；`Source.Expand(runDir, name)` 計算 digest 並以 `reportresource.ExtractFresh` 展開；`Abandon` 是原放棄式期限，triage 的上游比對也使用它。限額固定為單檔 1 MiB、子樹檔案總量 8 MiB、128 檔，不由 caller 參數化。讀取規則與 digest 公式以這些宣告的 doc comment 為準，本文不重述。Triage 的 `PrepareSkills` 簽名、兩句前置錯誤字串、子樹清單、manifest 解析與上游比對、gap ID／文字、展開目錄名、Attach key 與 `triage.skills.v1` schema 不變；`Read` 與 `Expand` 分開，使 manifest 解析與上游比對仍在展開之前，被拒的來源不展開、不產生 record。`SkillsRecord.Files` 的元素型別改為 `skilldir.File`，JSON 形狀不變。Code-change 不使用 sidecar，也不做上游比對，依據是 [CODE-CHANGE](CODE-CHANGE.md) 第 2 節對私有 skill 只記錄展開與 digest；它的子樹清單、環境變數、展開目錄名與 record schema 在 C4 落地。

**明列的行為差異：**

- 新拒絕條件只對新呼叫者，都在任何 filesystem 存取前：`Read` 拒絕非絕對路徑（含空字串）、空的 `dirs`、不是單一路徑段的 dir（空字串、`.`、`..` 或含分隔符）、重複的 dir，以及位於某個宣告 dir 之內的 sidecar（因此 sidecar 不會同時被當 sidecar 讀又被展開）；`Expand` 拒絕不是單一路徑段的 `name`，在任何寫入前。Triage 先以原字串拒絕空值與相對路徑，其固定的六個子樹、根目錄的 `manifest.json` 與 `triage-skills` 都不觸發其他新條件。
- 其他錯誤字串、期限與訊息、取消、限額與計數、dot 項目略過、名稱檢查、regular-only、每子樹 `SKILL.md`、`ReadStable` 穩定讀取、錯誤分類（仍不包成 `contract.Error`）、錯誤時點與 Attach 會計不變。
- Ownership 不變：展開仍由 `ExtractFresh` 保留新目錄，既有目錄不覆蓋；展開中途失敗時，部分展開的目錄維持保留，不刪除。
- 生命週期不變：被放棄的讀取 goroutine 在阻塞呼叫返回前不結束，可能比 run 活得久。
- 測試面：來源的讀取規則、限額、名稱與 FIFO 拒絕、逾時（含 1 分鐘預設值）與外層取消的測試在 `skilldir`；triage 只保留一個代表性的來源拒絕（缺角色目錄，端到端驗證不展開、不產生 record、run 為 Failed）、自己的兩句前置字串、manifest 與上游比對的 cases，以及上游的外層取消。

**剩餘缺口：**

- 走訪期間的取消檢查只讓被放棄的讀取 goroutine 提早結束，對呼叫者不可觀察，沒有測試覆蓋（遷移前亦然）。
- 整個目錄沒有一致性快照，檔案之間與檔案和 sidecar 之間不保證同一時點。
- 來源內的 hard link 不偵測，當作一般檔案複製。
- 展開的檔案一律為 0600，scripts 失去執行位元。
- `Expand` 不接 ctx，展開中不檢查取消；需要時由 caller 在呼叫前檢查（triage 沒有另加此檢查，與遷移前相同）。
- 阻塞的開啟（FIFO 或未下載的雲端 placeholder）在被釋放前，持有被放棄的 goroutine、其 file descriptor 與已讀入的記憶體（至多到讀取上限）。

以下保留目標與完成條件。

R8 前，`triage.PrepareSkills` 綁 triage 的角色名與上游比對。新 workflow 不得 import triage 的私有實作，所以先核對其職責：讀來源（`contract.ReadStable`）、展開到 run 內（`reportresource.ExtractFresh`）、記錄 digest、排除同步材料，以及 triage 專屬的上游比對與缺口。共用機械放 contract 責任層（`reportresource` 或相鄰的中性 package），角色名與環境變數由各 workflow 宣告。

- 差異矩陣須逐項列：取消、錯誤分類、ownership、path／symlink、限額（檔案數與大小）、穩定性、生命週期，並決定 manifest 必填與上游比對屬中性還是 triage 專屬；code-change 的私有 skill 同樣由原 command 衍生，是否需要上游比對也要決定。
- 不得默默套用更強或更弱的檢查，triage 保留原錯誤字串、期限與缺口語義。

**完成出口：** triage 實際使用中性入口，舊內聯副本移除；新 consumer 的角色集合與環境變數由其 workflow 自行宣告。

### R9：contract repair 迴圈中性化

服務 [CODE-CHANGE](CODE-CHANGE.md)。以其 P0 為前置，與 R6、R7、R8 彼此獨立。

**已實作接線：** `engine.Scope.StepWithRepair(ctx, spec, repairs, validate)` 承接原本在 `triage.TaskSession.Run` 的迴圈與 repair 訊息文字，triage 的內聯迴圈與 `repairMessage` 已移除；`TaskSession.Run` 以 `repairBudget`（仍為 2）與 `TaskStep.Validate` 呼叫它，`RunTaskStep` 與 `rounds.go` 兩個入口不變。可 repair 的條件、子 scope 命名、Feedback 組成、回傳形狀與 validate 的界線以該方法的 doc comment 為準，本文不重述；triage 專屬部分與錯誤字串不變。

**明列的行為差異：**

- Repair attempt 的 Feedback 保留首次 Feedback 的 Refs（舊版丟棄），只以診斷取代 message；原 message 不併入，也不加入被拒的 Ref。對 triage 可觀察：facts 業務重試與 investigator 第二輪起帶 Refs 的 Feedback，其 repair attempt 的 `request.json`、`AttemptStarted` 與 snapshot 的 feedback refs 由空變為原 Refs，每個 Ref 在該 attempt 派送前多經一次 committed 解析。解析失敗為 Store I/O 的 fatal、已發布檔案被改寫或移除時的 ReferenceInvalid，或取消／attempt 期限，都在派送前發生，都不 repair。
- `Observed` 由每次 attempt 後立即呼叫，改為迴圈結束後依派送順序呼叫；唯一 consumer（investigator）在 `RetryInputs` 返回後才讀，無可觀察差異。
- 新拒絕條件只對新呼叫者：負數預算在任何 Child／Step 之前以 `InvalidDefinition` 拒絕；triage 傳 2，不觸發。

**剩餘缺口：**

- 寫入型 Step 的身分與 receipt 核對放在成功返回之後，其接線與回歸在 C1。
- 寫入型 Step 的 repair 派送與 repair 意圖衝突：engine 固定的派送文字要求 Agent 先執行 `request.prompt` 的任務，repair 只帶診斷 Feedback，對寫入型 Step 可能重做副作用。由 code-change 第一個使用它的單元提出必要改動，本單元不改派送文字。
- D1 對 Agent 行為的影響（repair 時看到原 Feedback 的 Refs）未經 live 驗證。

以下保留目標與完成條件。

兩個真 consumer：triage 的 `TaskSession.Run`（經 `RunTaskStep` 與 `rounds.go` 兩個入口呼叫，含 `repairBudget = 2`、每次 repair 的 `contract-repair-*` 子 scope、同一 handle、schema 與 acceptance 兩種診斷、已發布 Ref 的保留）與 code-change 的寫入型 Step。`review` 刻意沒有內容修正迴圈（見 `docs/CODE-REVIEW.md`），不是 consumer，也不在此遷移。

- 只抽政策無關的核心：預算內的同 handle 重派、診斷作為 Feedback、子 scope 命名、執行類失敗（timeout、cancel、provider）不重試。triage 專屬的角色命名（`triage-` 前綴）、`TaskFailure` 與 recovery 型別、observation 回調留在 triage。
- 差異矩陣須逐項列：取消、錯誤分類、ownership（repair 被取消時已發布 Ref 的保留）、限額（每 Step 的 attempts 上界）、穩定性、生命週期。不得改變 triage 的錯誤字串、預算與 attempts 會計，也不得默默套用其他語義。
- 寫入型 Step 的外部副作用不由此機械處理；它不知道副作用存在。身分與 receipt 核對在迴圈接受最終 contract 之後由 consumer 執行一次，不進 `Validate`，因此核心不需要「不可 repair 的驗收錯誤」；若 consumer 把它放進 `Validate`，會被 repair，違反第四條。
- 矩陣另列：repair 時 Feedback 的 Refs 是否保留（舊版 repair 以不含 Refs 的新 Feedback 取代原 Feedback，A3 的 findings Refs 走 Feedback，必須保留，這對 triage 是行為差異，須明列並驗收）。

**完成出口：** triage 實際使用中性核心，舊內聯副本移除；既有 triage 測試與錯誤字串不變。只交付政策無關的核心；寫入型 Step 的回歸在 code-change 第一個使用它的單元驗收，不在本單元預造。

## 4. 後續 triage milestones 的依賴鎖

> **現況：**以下 M1–M7 是舊版 Planner triage 的歷史接線；該 workflow 未通過 alpha，已自 registry 移除，程式碼也已刪除（可從版本歷史取得）。v2 以新 package 改寫；v2 的共用項與相依於落地時更新本文件，下表不再是施工順序。目前 `internal/workflows/triage` 已承接 typed recovery（`Recoverable`、`ConfirmTaskRecovery`、`RetryInputs` 等）與 fresh-session task Step（`RunTaskStep`／`TaskSession.Close`），錯誤字串不變；S0（caller prompt 以 Attach 記錄、intake、facts、獨立 fact-check validator、未通過的事實以 Controller 記錄降級為缺口）已在 `runS0`，intake 驗收移植自舊版初始 intake，去掉 revision 欄位。外部 skill 目錄的展開（`triage.PrepareSkills`）經 R8 的 `contract/skilldir`，以 `contract.ReadStable`（Store 既有的 no-symlink／regular-file／NONBLOCK／大小上限／讀取期間穩定檢查，僅新增匯出）讀入記憶體，再交給既有 `reportresource.ExtractFresh`（triage 只展開六個角色子樹，略過 `.` 開頭的項目，每個角色必須有 `SKILL.md`；讀來源與比對上游各有期限，前者逾時為執行失敗，後者逾時記為未檢查的缺口）；`ExtractFresh` 本身與其 embed consumers 不變。`runtime/preflight.go` 的 discovery 讀取不遷移：它在 runtime 層（不 import contract）、讀的是他人 process 寫的 discovery 檔，只做唯讀判斷，政策是最後一段 NOFOLLOW、1 MiB、不需要讀取期間穩定檢查，與 run 內複製的信任邊界不同。

R1–R4 的匿名驗收與 consumer 遷移完成後才啟動 M1，不因某個 helper 尚未共用就在 triage 寫私有替代品。Live 驗收是 M7 的獨立 gate，不阻止已授權的匿名實作，也不因匿名通過被解除。

| Milestone | 前置完成項 | 必須重用 | 允許新增的業務內容／禁止重造 |
|---|---|---|---|
| M1 一般調查 worker | R1、R2、R3、R4 | OpenSession、Step、Inputs、共用 publication/file 消費及 test host | 新增完整 task/result/evidence contracts 與回 Planner 接線；不再造 acquisition wrapper/session driver |
| M2 Adaptive loop | M1 | Parallel、Decision、原 Run policy／會計 | 最多3個 ready workers、依賴批次、ledger、兩輪無進展 reframe/wiki 重查；不新增 scheduler/DAG |
| M3 Checkpoint／容量 handoff | M2、R2 | 每成功 Step 的 committed state、ContextUsage、嚴格 close、handoff | state 納入工作結果/feedback，每3dispatch完整 checkpoint與容量策略；不新增 Store/journal/crash resume |
| M4 Timeout recovery | M3 | typed failure、closeOnce/Wait、原 Run budget、適用時的有限 Retry | 先接已有worker/Planner的可恢復分支及remote job安全條件；verifier分支在M5角色出現時接入，不預造占位角色或新process manager |
| M5 fresh pro/con/cross | M4 | fresh sessions、Parallel、有序結果、exact Refs、M4的恢復機制 | 同版claim/evidence、runtime-supported confirmed與inference/incomplete、回饋迴圈及verifier recovery驗收；不套review Pin/roster或多數決 |
| M6 最終報告 | M5；本單元完成前必須閉合R5 | Result/FinalSelection/FinalDelivery、共用 renderer 機械部分 | 以實際triage report consumer啟動R5，完成deterministic report資料/模板/同版驗證binding；不加writer Agent、drafts/發布或final registry |
| M7 產品入口／完整驗收 | M6、R1 | 既有 registry、Definition、RoleSpec、共用 startup | 明確模型/授權scope/live驗收；不再造launcher/權限框架，不猜GLM ID或默換模型 |

M1 已接入 `plannerCaller.work` 的單項明列 worker 派工與 Planner 結果交接；`taskStepRecovery` 供既有 slice adapter 與 worker 共用（M4 前為 `taskStep`，無 caller 的轉送 wrapper 已移除），原 session/Step/strict-close 語義不另造。`worker_results` 明交 exact refs 及真正 owners，fresh handoff／support 改版不重綁歷史結果；原 query checker/schema 等義共用。這不是自動批次、產品入口或 live 驗收，具體 contracts 見舊版文件（已刪除，見版本歷史）。逐次 review/gates/commit 狀態仍在外部工程紀錄。

M2 已以 `executeInvestigation`／`adapt` 接明列 action，`workReady` 先限制最多三個 ready tasks 再使用真 Parallel，live Planner 另計。Ledger 只依 Agent 明報 changes 及 exact batch Refs 計數，不由 Go 比較領域內容。Reframe wiki 是獨立 committed Ref，沿原 WikiSearch/completeness，歷史 context binding 不重寫；模型由 caller 分別明示，不能將 Planner 默綁一般分析模型。單項 worker 及 supporting/fresh handoff 舊 callers 沿用共同執行機械。Live 模型／技能／production 驗收尚未完成，產品入口已接。

M3 已在既有 Planner snapshot 增加 exact checkpoint/counter/Controller feedback，三個 dispatch cycles 標記同份完整 snapshot，不新增抄寫 Step 或 journal。一次 workers batch／完整 support／wiki 交付各一 cycle，與 M2 hypothesis round 分開。`executeInvestigation` 非 nil 的明示容量政策沿原 adapt/step/handoff；unknown 記錄續作，達門檻 strict-close 後明交歷史及 pending feedback，不重派同 action。真 stats error 不吞，nil 保留舊 caller；沒有 live threshold 預設或週期 polling。

M4 已以明示 RecoveryPolicy 沿既有 Scope.Retry 接 Planner 有限 fresh 恢復，worker/wiki/support 以 exact typed delivery 回 Planner，由 Agent 提出具 basis 的 inspection/resume/redirect。全敗worker batch亦計一輪，進展仍Agent明報；未完成的當前reframe可先做安全inspection，不重置streak或讓舊reframe繞規則。Support重用原pipeline續接已驗收phase，原intake/wiki/context provenance不重綁，pending中間Refs不升格checkpoint。SessionIdentity提供dispatch前known owner，所有replacement仍strict close/Wait，不改普通Close語義或造重試會計。取消/fatal/cleanup/unknownProviderFailed不吞，M5 verifier恢復待角色出現才接。

M5 已接純claim producing Step與reference-linked state、fresh pro/con/cross、同版安全補角及Planner feedback；每角model/retry由caller明示。Planner/claim/verifier共用由M4抽出的retryPlannerInputs、Scope.Retry與原identity/strict-close，不加driver/engine多output/inputjournal。新verifier只claim/allowed evidence，不注入Planner敘事或舊verdict；同版Refs有序驗收、換版全fresh、pending partial不當checkpoint。完整verification delivery也算一輪，progress與支持程度由既有Planner判讀，Go不投票或按schema認列因果。M7產品入口已接，live仍未驗；M6報告與R5操作見本文件。

M6 新入口 `executeInvestigationReport` 沿共用初始化/adapt，在 yield 或純會計 admission 拒絕時呼叫現有 Planner 的 scope-aware report Step。Caller 明示 `ReportPolicy`，原 `executeInvestigation`／R5 操作保持 state-only／單項行為。Report/Planner/claim/verifier 共用 `retryPlannerInputs` 與 Scope.Retry、identity/strict-close/reopen，原 native errors 經 branch-local 收集及 join 後合併；不從診斷字串重造 cause。Step/check/Decision/RetryFinished/close 未完成的真 report Ref 只在 PendingReportError 明交，不是假 Final、不重試已 committed 產物；成功才沿 Result/FinalSelection。

Report m6 metadata 明列全部合法 recoverable 歷史項目，包括未選 claim。Agent 宣告 unresolved、handled 或具 basis 的 redirect；Go 驗 first owner/版本/完整對應，handled 必須沿 task/role/proposal/phase 的真正後續成功工作，不接受任意較晚 Planner snapshot。Planner retry 的成功接收 state 可合法處理原失敗；claim、worker、wiki、support 沿原 binding／resume lineage 驗收，support key 改變不等於失去原 lineage。這不是按 schema／support 字串認列因果，原診斷不刪；未解 error 或提早 resource-limited 都保持非零，fatal/cancel/非法產物/cleanup 不改標成功。

Admission 由唯一同步 dispatcher 一次計入普通 action 的全分支、有限 retry/fresh、support phases/reopen、下一 Planner update/retries 及容量 handoff 的保守上界，使用實際 Run.Snapshot policy/counts。不足便用 last accepted state 收尾，不新增停止用 Planner Step 或偽造 ledger yield。沒有 token 帳本、scheduler 或 engine 業務政策；Snapshot 不是對其他 dispatcher 的原子 reservation，bytes/disk/deadline/真正 hard cap 後沒有報告承諾，無 accepted state 也不保證 report。產品模型／數值已由 M7 Definition 明列，live 仍是獨立驗收 gate。

M7 由 `triage.Definition()` 接入既有 `workflows.Definitions/Resources/Schemas` 與 CLI，沿原 slice → report resources → investigation report pipeline，沒有新 launcher 或 CLI config。已批准初值集中於 Definition：fireworks GLM-5p3/high 供 Planner/三方/report，GLM-5p3-flash/high 供 analysis/vision，DeepSeek-v4p1-flash/off 供 fetch；capacity80、Planner/各 verifier/report retry1、report reserve1session/2attempts。各 Run 仍使用複製的明示政策，不在執行中改會計。

Workflow-local JSON envelope 明列 scope/request，局部 strict decoder 保留 duplicate/case/null/type/extraJSON 檢查，舊 private contract parser 不為此擴成共用 API。`callerRequest` 從 `r.WorkflowInput()` 的 Input.Prompt 明交本 Run 已持久化的原 request，保留 registered workflow gate 與每次 strict decode。此 read-only 投影在 r.mu 下回傳 immutable workflow/input 的值，不複製歷史；目前 Input 只有兩個 string，未來增加可變欄位時必須複製而不得暴露內部引用。供初始/修訂、正常/fresh Planner 與 report 使用，report 驗原字串；不是新 evidence 或 instruction Step。Partial Scope 的既定限制保持，省略 tenant_ids 表示空授權 array，不從 ticket 推導 target。模型/image catalog 宣告、CLI 匿名 engine/Store/RPC/renderer 採證與真 Pi/provider/skills/live 仍是不同層級。

M3 保存當時已有的工作結果／feedback；M5 新增 verifier 時，同步擴充可重建 state 與恢復驗收。不得把尚未存在的角色填成占位資料就宣稱完成，也不得要求 M4 先完成 M5 才能提供的 verifier，形成相依循環。

依賴表示本輪預設施工順序，不是宣稱每項都是框架缺口。要拆分或調整順序，先說明具體獨立性、維持哪些保證並取得確認，再更新此表，不讓接手者自行解讀成跳過。

`Parallel` 不限流，MaxLiveSessions 超限是失敗不是排隊；最多3 workers須先限制派送批次，仍 live 的 Planner 另計 slot。`Retry` 管有限重试會計，不決定調查方向，也不證明 remote job 已停；不能包住整個調查盲重跑。

已完成的 intake/wiki/context、局部 resolve/refresh、完整 inventory update、supporting queries、Planner supporting-work/state/handoff、同步驗收去重及測試分層，不因新 milestones 重做。必要變更以新 contracts/dependencies 明確接續，保留真實歷史 owners及既有驗收。

## 5. 每單元必填的 review／交接紀錄

記在指定工程紀錄，並更新本文件能力表及必要 workflow 文件：

| 欄位 | 必填內容 |
|---|---|
| 單元與前置項 | R/M ID；依賴是否已有 source、正式 review 與對應驗收，不能只填「應已完成」 |
| Reuse map | 實際 API/檔案/callers；新工作如何使用它；不能只說「參考上一個workflow」 |
| 差異與不變量 | 機制差異 vs 業務政策；typed errors、Refs、ownership、cancel、limits、cleanup及原API契約 |
| 變更／遷移範圍 | 新增的最小能力、遷移所有既有consumers、移除舊副本或具體保留理由 |
| 驗收 | 受影響 workflow與共用package的行為回歸；完整情境mapping；效能/資源主張的前後證據；skips/未驗live |
| 完成證據 | exact reviewed/tested/committed tree、來源manifest、正式專門review及修正閉合、採證位置 |
| 下一手 | 已完成且禁止重做項目、真正未完成項、下一個未阻擋單元、未決語義與授權限制 |

Review必須阻擋：新增同質機制卻無差異證據、只建未使用共用helper、未遷移原consumer、以較弱驗收統一、將domain判讀塞engine、把計畫API當現況，或用Skip/舊tree PASS掩蓋缺口。若確有無法共用的差異，記錄具體保證與callers；不為DRY移除合理的workflow-specific程式。

共用 refactor 至少做正式 code／scale-failure／simplicity review及必要local補審，核實修正後驗 exact待提交tree。Tests與runner採證不能代替架構review。基礎能力變更須覆盖原consumer與新consumer，不只跑新workflow。匿名、compile-only、live及history/privacy等證據層級保持分開。

工程環境、外部呼叫與發布仍受當次使用者授權約束。規劃live gate不是執行live的授權；文件完成不代表R/M實作或模型能力已完成。
