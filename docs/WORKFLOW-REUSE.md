# Workflow 重用約束與 refactor 計畫

本文件落實 [專案憲法第七條](../AGENTS.md)，是開發、review 與接手的必要入口。規範共用機制的收斂順序，不新增 WorkflowBase、DSL、工具權限框架或外部 workflow config。

**規劃中的 API 不等於已實作。** R1 已接入共用 runtime，R2 已提供共用 report 判定，R3 已共用 publication 投影與 bounded-read loop，R4 已遷移指定 host transport consumers，實際入口見下表，具體名稱以完成後的 source 宣告為準。已有 engine 原語不能因本計畫重新實作。逐次執行狀態、SHA、review/gate logs 留在 repo 外指定工程紀錄，不放進本文件。

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
| Run、session、Step 與 limits | `engine/api.go`、`session.go`、`step.go`、`policy.go` | 直接重用；禁止另一套 session driver／會計 |
| Child、Parallel、Retry、Decision | `engine/scope.go` | 直接重用；workflow 決定批次、依賴及可重做分支 |
| Stage／Confirm／Publish、committed resolver | `contract/`、`engine/resolve.go`、`step.go` | 直接重用；禁止第二個 Store／commit／journal |
| Result／FinalSelection／FinalDelivery | `engine/api.go`、`resolve.go`、`run.go` | 直接重用；禁止另一個 final artifact registry |
| Context usage 與 cleanup report | `engine/session.go`、`runtime/types.go` | M3 triage 沿 SessionContextUsage／既有 strict-close handoff 接入明示容量政策；sample 非 provider admission |
| Discovery 自動 preflight | `runtime.New` 固定有效 BridgeDir；`Pi.Start` → `runtime/preflight.go` 的 `preflightDiscovery` | R1 已接線：review 私有 guard 已移除；smoke/review/triage 共用 Start，仍須部署版本核對/live 驗收 |
| 嚴格收尾確認 | `runtime.CleanupReport.ConfirmsLocalClose(expectedSessionID)` | R2 已接線：triage slice／Planner 共用；caller 先處理 CloseSessionReport error，普通 CloseSession 不改義 |
| Envelope／file consumer | `contract.FileEntry`／`Publication[T]`／`DecodePublication[T]`；review `readReviewContract`、triage `readAccepted` 經 `engine.ReadContract` 後消費 | R3 已接線：只共用投影與 fresh decode，不新增授權；review ID 索引、triage lineage/cache 留 caller |
| Published／checkout 二次讀檔 | `contract.ReadBounded`；review `readCheckFile`／`readPublishedFile`、triage `rawFile` | R3 已接線：共用 bounded loop，root/open/identity 與錯誤政策留 adapter；Store `copyStable` 的雙 digest 不合併 |
| RPC 測試 transport | `testutil/protocol.NewHost`／`Host.Events`／`Event.Reply`／`Host.Close` 及 `WriteEnvelope` | R4 已接線：review workflow/check integration、triage RPC driver 共用 host；原 domain scenarios 與 Store-only 分層不變 |
| Skill extraction／report renderer | 目前只有 review 的 `ExtractSkills` 與 `render_report.py` | R5：第二個實際 consumer 出現時才抽機械部分 |

上述相對路徑均位於 `internal/`。角色模型、PR Pin、code-location Evidence、investigation Ref/file Evidence、Source status 及 verdict 不屬於通用登記項，保留 workflow-specific 語義。

## 3. 共用 refactor 單元與順序

預設順序為 **R1 → R2 → R3 → R4，再進 triage M1**。每項是可獨立 review／驗收的最小單元，不合成一次大重構。R4 可為前面單元的測試需求先行，但須記錄順序調整，不能因此改變依賴或跳過 R1–R3。R5 延至 M6，不預造第二個 renderer。

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

三個指定 consumers 已移除私有 host pumps／connections／join 副本。WriteEnvelope 只寫 caller 明列的 meta/data/files，不猜檔案種類、不驗授權、不更改 nil entries，允許原故障 cases 宣告不合法內容。正常 child WriteCandidate 也使用它。Engine 的同步 handoff／worker-limiter socket tests 保留逐 socket Accept/Decode barriers，runtime fixtures 使用不同 control/HTTP 協定，兩者不強制改成 eager fan-in。

**目標與責任層：** 擴充既有 `internal/testutil/protocol`，共用 listener、event pump、connection ownership、deadline、cancel/close/join 及明列 file entries 的 envelope writer。

- 至少遷移 review workflow fixture 與 triage RPC driver；review check tests 的同質部分一併接入，或列出不可共用的具體差異。
- 非 EOF decode error 不得默默消失，失敗 assertion 也要釋放自有 children／connections 並 join。
- Registry/model、scenario、role order、checkout 存活、Ref/history、故障注入與業務 assertions 仍屬各測試。不提供替代 Step／validator 的假 workflow runner。
- 保留真 engine/Store/protocol 與已建立的 Store/publication 分層；不把 Store publication 當 committed Ref，不因抽 harness 刪 case 或只驗相同總數。

**完成出口：** 指定 consumers 不再各保有相同 host pump/lifecycle；逐案 mapping、typed failure、資源 ownership／取消與完整一般/race 回歸一致。效能以整套同類情境比較，不因共用程式就預告加速幅度。

### R5：report／resource 機械部分，延後至第二 consumer

目前只有 review renderer/extractor。到 triage M6 出現實際需求時，先比較 fresh exclusive extraction、安全 JSON/file I/O、candidate identity、artifact 註冊與 cleanup，再抽窄共用部分。

**完成出口：** 原 review 仍能使用同一機械能力，triage 報告也實際接入；模板、Pin／claim、verdict及追溯資料保持各自 contracts。保留 exclusive output、大小／路徑／identity檢查與已提交 Ref 驗收。沒有第二 consumer 時保持延後，不建立 renderer plugins、installer 或新 final registry。

## 4. 後續 triage milestones 的依賴鎖

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

M1 已接入 `plannerCaller.work` 的單項明列 worker 派工與 Planner 結果交接；`taskStep` 供既有 slice adapter 與 worker 共用，原 session/Step/strict-close 語義不另造。`worker_results` 明交 exact refs 及真正 owners，fresh handoff／support 改版不重綁歷史結果；原 query checker/schema 等義共用。這不是自動批次、產品入口或 live 驗收，具體 contracts 見 [Jira triage](JIRA-TRIAGE.md)。逐次 review/gates/commit 狀態仍在外部工程紀錄。

M2 已以 `executeInvestigation`／`adapt` 接明列 action，`workReady` 先限制最多三個 ready tasks 再使用真 Parallel，live Planner 另計。Ledger 只依 Agent 明報 changes 及 exact batch Refs 計數，不由 Go 比較領域內容。Reframe wiki 是獨立 committed Ref，沿原 WikiSearch/completeness，歷史 context binding 不重寫；模型由 caller 分別明示，不能將 Planner 默綁一般分析模型。單項 worker 及 supporting/fresh handoff 舊 callers 沿用共同執行機械。M4+／產品入口／live 尚未完成。

M3 已在既有 Planner snapshot 增加 exact checkpoint/counter/Controller feedback，三個 dispatch cycles 標記同份完整 snapshot，不新增抄寫 Step 或 journal。一次 workers batch／完整 support／wiki 交付各一 cycle，與 M2 hypothesis round 分開。`executeInvestigation` 非 nil 的明示容量政策沿原 adapt/step/handoff；unknown 記錄續作，達門檻 strict-close 後明交歷史及 pending feedback，不重派同 action。真 stats error 不吞，nil 保留舊 caller；沒有 live threshold 預設或週期 polling。

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
