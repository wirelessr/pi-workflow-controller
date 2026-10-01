# Jira triage：workflow 與執行期適配

`jira-triage` 已透過既有 Definition／registry／CLI 接入 intake → wiki → context、局部補缺與 revision、Planner adaptive 調查、獨立三方驗證及固定報告收尾。保留 deadline、context usage、checkpoint／handoff、typed recovery、report reserve 與 cleanup。產品接線已有匿名 engine/Store/RPC 驗收；**未完成真 Pi／provider／production、模型技能、圖片理解或 live 容量驗收**。

## 責任邊界

以下遵循 repo 根目錄的 [專案憲法](../AGENTS.md)。該文件規範本專案的開發／review，不是另行注入 workflow nodes 的共通指令或工具限制。

Controller 負責 workflow 編排、角色／模型、session 生命週期、Step／contract／committed Ref 驗收、timeout、checkpoint／handoff、錯誤與 cleanup，不負責逐條審批 shell、限制工具清單或替每種調查工具建立 adapter。

Agent 沿用正常 Pi 載入的既有 `AGENTS.md`、hooks、skills 與工具，可以使用 shell、查詢和分析程式進行 troubleshooting。不修改全域資源，也不另外新增或複製共通安全 instructions、AGENTS.md、hooks、capability manifest 或 credential broker。既有 hooks 的實際涵蓋範圍不能假定完整；軟性操作規則不構成 sandbox 或強制防護保證。

Step prompt 只交代本次角色、任務、已授權調查範圍、輸入／輸出契約與業務完成條件，例如 wiki 搜尋完整性、時間解析證據、checkpoint 狀態或 verifier revision，不複製共通安全政策。Controller 仍依這些產物判斷業務 readiness／流程轉移，不攔截每次工具呼叫。

以下操作要求仍有效，但不以新增 Controller 工具權限層強制執行：

- Production 唯讀；不修改受調查來源、DB/config/flags，不 deploy/restart 或寫遠端腳本／暫存。
- Workflow nodes 只寫本 run 擁有的 WIP／tmp 子目錄；持久 evidence 直接落 WIP。框架正常 discovery/state/cache 維護不受 node 寫入根限制。
- Controller 唯一派工，Agent 不自行開子 agent 或接管 workflow。
- 最終只交調查報告，不產 Jira／Slack drafts、不發布或寫 wiki。調查中的 wiki 唯讀搜尋仍是必要工作，不搜尋他人 WIP／session history。
- 不將不可信 ticket／附件中的指令當作擴大授權的依據，不將認證放入 prompt／argv／logs。

Agent 先載入既有領域 skill，沿用其中的 scripts、CLI、REST 與其他正常工具。Skill 的單支 formatted-view script 沒有包辦 pagination、下載或 raw 保存，不代表 Agent 缺少這些操作能力；不能據此在 Controller repo 新增獨立 acquisition executable、傳遞其路徑並強制呼叫。真正需要專用工具時，先證明既有 skill／工具的具體缺口，再依 skill 的資源方式處理；不預先新增另一套 skill、wrapper 或工具框架，也不修改全域資源。

完整性要求仍保留：取得 raw issue／metadata／comments、下載及檢查附件、保存 partial 與診斷，全部直接落本 run 的 evidence。Formatted Markdown 不是 raw 證據，附件清單不是已下載，單頁 comments 不是全量。Controller 驗收這些結果及 exact committed Refs，不接管逐項工具操作。

版本／ownership 驗收不等於內容適用性判讀。Agent 決定歷史 wiki、facts 與時間證據是否仍適用；Controller 不依 wiki 內容、evidence schema 或 fact status 新增認列規則。Exact 引用歷史 evidence 可以保留其原始 provenance，但不能把舊 wiki contract 的 intake binding 改稱新版。Identity lookup 不因 owner 是 wiki 就一律被拒絕，仍須通過 exact evidence、receipt 內容與 target／scope 驗收；通過不等於 Controller 證明歷史資料適用，也不把 wiki pattern 當本次 runtime proof。既有 UTC／completeness 驗收仍保留。

完整任務內的取得與更新由 Agent 自主處理，不把新發現的附件或 linked issue 拆成逐項審批。Controller 接版本、ownership、歷史與下游 dependencies；Agent 判斷替換意義、分析適用性及剩餘工作。明列 selectors 的局部補取仍可使用，但不是所有更新工作的唯一入口。

## Pi source cwd 與輸出 workspace

Triage 的 slice、worker、Planner、claim／report 所重用的 Planner，以及 fresh／recovery／pro／con／cross session 均沿 engine 的一般 cwd fallback。CLI `PWC_PI_CWD` 可指定 source cwd；未設定時使用 Controller LaunchCWD，不再將 Pi 強制放在 `run/triage-work`。正常 AGENTS／skills 發現仍由 Pi 負責，不新增 loader、共通 runtime prompt 或工具權限層。

`executeSlice` 仍實際建立 `run/triage-work`。各角色任務 payload 的 `workspace` 明交該絕對目錄，用於 scratch／downloads；既有 requirements 說明 cwd 與 sibling repositories 唯讀，且不得擴張本角色的 evidence／acquisition scope。Verifier 仍只讀原 claim／allowed evidence。這些欄位是工作資料，不更改業務 output contracts，也不使 scratch 成為 committed evidence。

Step request／candidate／evidence、renderer 與 committed Refs 繼續使用原本的絕對 run-owned paths。Fresh task 同樣明交 workspace，不靠前一 session 記憶或 cwd 猜路徑。匿名 subprocess 測試只證明 cwd、workspace、持久化及原交接接線，不證明真 Agent 永不寫來源 repository；唯讀要求不是 sandbox。

## 產品輸入與原始 request

既有 CLI `run jira-triage "Prompt"` 的單一 Prompt 是 workflow-local JSON envelope，包含 `scope` 與 `request`；不新增 CLI config/input-file flags 或 launcher。Scope 欄位為 `ticket`、`stack`、`pop`、`binding`、`tenant_ids`，代表 caller 授權邊界而非已確認身份。自然語言 request 必須非空白，其內容、首尾空白、Unicode 與 JSON escape 解碼後的換行原樣保留。

Decoder 僅處理兩個固定 JSON objects，拒絕未知／錯大小寫／重複鍵（包括 escaped key）、null、錯型、extra JSON 及非法 ticket/tenant 值。沿用既有 Scope 規則，不解析票內容、不猜身份／時間或把 linked issue 當擴權。可只給 ticket 做合法前置；省略 tenant_ids 代表空授權集合，normalize 為契約所需空 array，不新增 target，explicit null 仍拒絕。Runtime 查詢仍須原完整 Scope、intake/wiki prerequisites。

`callerRequest` 只對已登記產品 workflow 從 `r.WorkflowInput()` 的 Input.Prompt 讀 engine 已持久化的原輸入；read-only 投影在鎖下以值回傳 immutable workflow/input，不複製歷史，也不快取 decode 結果。初始 intake、revision/resolution stages、正常／fresh／recovery Planner 及報告明交同一 request。它是 caller 任務指示，不是虛構的 committed evidence。Report optional `request` 必須精確符合原字串並由固定模板呈現，trim／改寫會拒絕；舊 slice/Planner/report callers 不必改成此 envelope。未新增 instruction 轉抄 Step、Store 或 registry。

每次取 request 仍需 decode 輸入，但不再為此複製完整 Snapshot；目前沒有跨 Run cache，也未量測此成本的 live N/RSS/p99，不宣稱零成本。

## 模型與預算

初版設定集中於 `internal/workflows/triage/definition.go`，可日後調整，沒有外部 config 或全域模型 fallback。Provider 全部 `fireworks`：

| 角色 | Exact model ID | Thinking |
|---|---|---|
| Planner／pro／con／cross／report | `accounts/fireworks/models/glm-5p3` | `high` |
| 一般 analysis／vision | `accounts/fireworks/models/glm-5p3-flash` | `high` |
| 機械 fetch | `accounts/fireworks/models/deepseek-v4p1-flash` | `off` |

Vision 沿既有 analysis responsibility，不新增另一個 model selector；圖片取得不等於圖片分析完成。Report 沿同一 Planner，不另開 writer。Exact bindings/thinking 已明定，但 catalog 宣告與匿名模型參數斷言不是 live 能力驗收；不換成 fast router 或默換其他模型。

產品初值：handoff threshold 80%，Planner/各 verifier/report 各最多一次 retry，最後 report 預留一個 fresh session、兩次 attempts。Definition 將政策值複製到各 Run；不是在 Execute 中改寫 Run 會計。私有入口仍可明示其他合法 policy，測試用值不變成新的產品預設。

Jira triage 從 `DefaultRunPolicy()` 起設 `DisableRunTimeout = true`，不增加 Controller 整體 run deadline；每 Step 保留 30 分鐘。所有 numeric limits 仍須正值，保留舊有零／負值 invalid-definition 行為。Parent cancellation/deadline、startup/RPC/cleanup timeout、資源 hard caps 與既有 workflows 的預設皆不變。

## Context handoff 與 timeout recovery

- `Run.SessionContextUsage(ctx, handle)` 在 Step 邊界取得 lease，按需查 `get_session_stats`，不新增 health polling。結果帶 identity、seq／epoch、取樣時間與 nullable tokens/window/percent；unknown 不等於零，estimate 不保證 provider admission。
- `Run.CloseSessionReport` 重取既有 closeOnce 結果並複製 slices；未完成不能視為 clean。Recovery 另驗 identity、WaitCompleted、ProcessExited、Unconfirmed、WaitError、KillError、DiscoveryError；舊 `CloseSession` error predicate 不變。
- 成功 Planner Step 的 committed state 須能重建決策狀態。每三個 dispatch cycles 在同份完整 snapshot 標記並驗收 checkpoint，不另叫 Agent 重抄或增加 Step；一批 workers、一次完整 supporting 工作或一次 wiki 交付各算一個 cycle，純 planning/no-ready/checkpoint/handoff 不計，與假說無進展 round 分開。確認舊 session cleanup 後，再 OpenSession，以已 committed Refs 接續。
- Timeout 沒有新合法 Ref，從上一份 committed state 加後續已提交證據接續，不要求失敗 session 補 checkpoint、不重置 session／attempt accounting。
- 本機 Wait 不證明 remote async job 已結束；未知 ProviderFailed 不分類成 overflow。取消、storage/journal failure、run hard cap 不可吞掉進 recovery。

## 匿名 intake → context 切片

切片的初始階段使用三個獨立 Step／session，沿用真 engine、Store、committed resolver 與 RPC protocol harness。匿名案例在 provider 邊界以靜態 intake fixtures 代替 Agent 的 Jira 取得，交給同一 candidate／Store 路徑；其他 Agent 分析、wiki 與 DB receipt 仍為匿名 fixtures。沒有新增通用 orchestrator、工具 adapter 層、共通 instructions 或 launcher。Private `executeSlice`／`resolveSlice`／`refreshSlice`／`updateSlice` 及下述 Planner caller 現由產品 Definition 與匿名測試重用，Pi 啟動沿用共用 runtime 的 shared-discovery preflight。GLM binding、thinking 與初版產品設定見上節；共同 guard 與匿名接線不代表已驗證真環境啟動或模型能力。

- `triage.intake.v1`：完整 issue/raw fields、field metadata、各 comment 原始頁、linked issue snapshots、附件 content／analysis manifest、來源 URL／取得時間及 gaps。Go 核對 raw key、必要欄位、分頁 offset／total／唯一 comment IDs、linked／attachment inventory 及附件 byte size；historical content 在其真正 owner 版本驗 byte metadata，不以新版 issue 的 size 否定舊 bytes。拒絕省略 inventory、截斷本次下載或偽稱 complete。部分／缺失／unsafe／too-large／未完成分析保留為明確缺口，不等於空結果。
- `triage.wiki.v1`：綁定 exact intake Ref，保存搜尋詞、wiki-only scope、搜尋證據與已讀頁面；區分完成有結果、完成無結果、partial、unavailable、not-run。未完成不得偽裝 no matches。本次開發只使用匿名 wiki fixtures，不存取實際 vault。
- `triage.context.v1`：綁定 exact intake/wiki Refs 與 caller 授權 scope，保留身份、binding、release、observations、identity/time resolution attempts、附件/wiki 完整性與上游 gaps。Evidence 使用 exact input Ref＋file ID，或 null Ref 表示本 contract 自有 evidence，不重複宣告其他 attempt 的附檔。Resolved identity 須有 target/DB resolution receipt，核對唯一 tenant/orgkey row、stack/PoP/binding/release；多 row、錯環境或版本不符不可宣稱 resolved。
- UTC 正規化驗收支援 explicit-offset RFC3339、epoch seconds/millis、同事件 local／epoch 配對與 offset 實算。保留原始時間、來源、UTC 與計算；不接受無 offset 的時間字串冒充 RFC3339，也不接受 local timestamp 自身作 absolute evidence。From/to 為已觀測 incident anchors 的 min/max，單點事件可為同一時刻；它與實際查詢窗口分開。Agent 在任務內依可靠時間依據與資料量自主選擇非零小窗，不要求每次涵蓋完整事故區間，也不逐查請批。
- Ticket-only 授權仍可執行 intake/wiki/local triage，不要求先知道 tenant/PoP 才能保存資料。只有 caller 已明確授權 target 且 intake/wiki 完整時，task 才允許唯讀 runtime resolution；此 task 規則不是 shell sandbox。
- 每個成功 Step 提交後確認舊 session cleanup，才開下一個 session。輸出為 supporting context，`ready` 只代表本切片前提驗收；`needs-resolution` 保留待補工作，不是 blocked 結案。沒有 `FinalSelection`、報告、draft、publish 或 confirmed root cause。Step failure／取消／hard cap／cleanup failure 原樣返回，不重新標為缺資料。

完整性驗收檢查 contracts／raw snapshots 與算術／版本關係，不能證明模型正確理解原始證據，也不是 live DB 或 wiki 驗證。Jira 取得與附件處理是 Agent 以既有 skills 完成的工作，Controller 沒有自帶取得器；匿名測試以靜態 intake fixtures 及 localhost provider fixtures 驗 Controller 對 Agent 產物的驗收，不證明真 Agent 已成功取得。

### Committed context 的局部補缺

`resolveSlice` 每次執行一個 Controller 指定的局部 cycle，可在同 run 從新版 committed context 再呼叫，不是完整 adaptive Planner 或自動無限 retry。

1. 重新驗收 exact committed context lineage、scope、intake/wiki Refs 與 evidence。`ready` context 不增加工作；`needs-resolution` 才記錄 dispatch Decision。
2. 必要 wiki 未完成，先用 fresh Step 執行 remedial search。新搜尋仍可 partial／unavailable，不改標 no matches；cleanup 未確認則不啟動下一階段。
3. Fresh context Step 只處理 Controller 指定的 unresolved identity/time 與 wiki 狀態。重用同一 committed intake，不重新抓 Jira；已解析 identity／UTC anchors、observations 與 resolution attempts 保留，舊自有 evidence 改以 exact previous Ref 引用，不重複複製附件。
4. 新 context 以 `previous` 綁定舊版本。移除舊 gap 必須提供 `resolved_gaps` evidence，且包含本次新 evidence 或新版 wiki evidence；上游仍存在的 gaps 不可刪除。舊版與新版 evidence 都是顯式 Step inputs。這是 provenance／readiness 驗收，不是對證據語意的獨立事實證明。
5. 所有 session／attempt 沿用同 run accounting；provider failure／timeout／取消／hard cap／cleanup failure 原樣返回，不補交未 committed candidate，也不替換最後已驗收 state。新 context 仍只是 supporting state，remaining gaps 不等於結案。

此局部 cycle 本身不修補 comments／附件等 acquisition gap，改由下節 `refreshSlice` 指定來源補取；不因 wiki 重試成功就偽稱 intake complete。每個 cycle 仍重新驗收完整 lineage 與 evidence digests；同一次同步驗收內重用已驗過的 contract envelopes，避免巢狀 checker 重複呼叫 Store（見下節）。這不是跨 Step 的 cache，也不宣稱已解決長歷史的所有成本。同 intake 的已解析前提若出現新矛盾，仍需後續 invalidation／reframe 路徑，本切片不靜默改寫。尚未接 live wiki／DB／Sumo／Prism、真 Agent 依既有 skill 取得資料或完整調查 loop。Supporting-source acquisition 與 query readiness 的局部接線及匿名驗收見下節。

### Identity/time supporting sources 與任務內 query readiness

Context、context-resolution、context-revision 共用 identity/time acquisition 的工作要求，沿既有 skills/tools 保存 target／release 原始查證、lookup query/response 與 normalized receipt。這些任務不新增 acquisition executable、工具 wrapper、query-approval Step 或獨立產品入口。

Query readiness 是本次 supporting 工作的可靠起點，不是新增全域 `ready` 狀態，也不要求全部 local timestamps 都先 resolved。仍須有 caller 授權 target、完整 intake/wiki 前提，以及查詢前已有的可信有限 UTC 搜尋依據和來源／filter 線索。例如已有 server receipt 的 UTC，但 client local timestamp 缺 TZ，可以先查同事件 supporting evidence。若沒有可信 UTC 依據，先沿其他已授權來源補找，不猜時區或盲掃。

Agent 可在同一 Step 依資料量自主縮窗、移窗、分段、擴展、加 filter 或改 aggregate；不由 Controller 固定每窗寬度、選取策略或強迫下載整段事故。既有工具限額保留。這是任務與工具操作規則，不是 Controller 對每次 HTTP／shell 的硬性攔截。

`ResolutionAttempt.queries` 是 optional 的實際操作紀錄，不是事前申請或未執行的計畫：

- 每筆保存 `source`、`filter`、UTC `from/to`、查詢前已有的 `basis` evidence、`status`（complete／partial／unavailable）、`outcome`（包含窗口選取及結果限制），以及原始結果、request/status/diagnostics 的 `evidence`。未進行 time-bounded supporting query 的 attempt 可省略；舊 contracts 不需改寫。
- Controller 驗非零正向 UTC 窗口、必要欄位與 exact evidence owners，不解析 query 語言、不檢查窗口是否覆蓋所有 anchors，也不替 Agent 認列時間／filter 證據的內容適用性。`basis` 的內容及先後真實性、原始 response 是否被正確理解，不能只憑 Ref 存在證明。
- Query 的 `complete` 指該次查詢取得狀態，不代表完整事故覆蓋或根因 confirmed。前次 partial／unavailable 在後續成功後仍保留；空小窗、partial 與 timeout 不等於事故不存在。Agent 判斷結果是否已足以支援局部問題，無須為了將所有 query 標成 complete 而重跑。
- 歷史 queries 的 basis/result 都沿真正 owner qualification 及 resolution attempt history 保留規則交接；保留舊資料不冒充本次新查詢。Observed `time.from/to` 仍只由 incident anchors 決定，不被查詢窗口覆寫。

匿名 localhost HTTP／RPC 測試在同一 context Step 讀取 raw identity response、產生 receipt、取得 partial 搜尋結果，再以其 trace 縮小窗口取得 server epoch，交給真 engine／Store 驗收 local/epoch 換算與 ownership。另涵蓋 partial／unavailable／空結果、lookup conflict／錯 target 或 release、非法窗口／Refs、fresh-session 局部續接與失敗保留。取得及後續條件使用是真 HTTP，工具選擇與領域推理仍由 provider fixture 模擬；不是已驗證真 Agent skill 載入或 Sumo／Prism live 操作。

### Committed incomplete intake 的局部版本交接

Private `refreshSlice` 從既有 committed context／incomplete intake 執行一個指定 cycle，三個 fresh Step/session 依序產生 intake revision、wiki revision、context revision。Caller 明列 source selectors 與缺少／失效原因，Controller 不從內容推斷失效，也不替 Agent 選工具。

- Intake 以 `previous` 綁定 exact prior intake，`work` 保存此次指定來源與原因。Selectors 為 `issue`、`fields`、`comment:<startAt>`、`linked:<key>`、`attachment-content:<id>`、`attachment-analysis:<id>`。可補缺少的 comment page；其他 slots 必須已在原 inventory。任意 inventory 增刪與完整 intake 更新使用下節 `updateSlice`，不由窄任務自行升格。
- 每個指定 slot 保存本次 local result，包括失敗／partial 狀態；本次 `acquisition` 必須有自有 metadata／diagnostics evidence。其餘 slots 保持原狀，有檔案者以 `Source.ref`＋`file_id` 指向真正的歷史 owner，不重複宣告舊附檔。Content 補取失敗時可以保留經 lineage 驗證的歷史 analysis，新 intake 仍 incomplete；沒有 content 卻宣稱本次新 analysis 完成仍被拒絕。歷史 raw／失敗資料仍留在原 committed contract。Issue 補取失敗時，沿 exact 歷史 issue inventory 檢查保留 slots 的結構／byte metadata；不把 inventory 當成本次成功的 issue，也不因此改標 complete。Agent 的 metadata 內容判讀不由 Controller 重寫。
- 新版 wiki 必須綁定新版 intake，重新交付本次搜尋的 local evidence 與實際完成狀態；partial／unavailable／not-run 仍是缺口。舊 wiki 是顯式歷史 input，不是新版搜尋的替身。
- Context 同時綁定新版 intake/wiki 與 previous context。Agent 保留仍適用的資訊、重評受影響身份／時間／observations；Controller 只接來源／版本關係並保留既有 receipt、UTC、scope、gap 驗收，不自行建立歷史 wiki 的適用性或 facts 認列機制。跨 intake 可更新原先 resolved 的資料，原版仍保留，resolution attempt history 不可丟棄。同 intake `resolveSlice` 的原有保留規則不变。
- 所有保留的 intake/context/wiki evidence owners 都是 exact committed Step inputs。失敗不替換 caller 的最後已驗收 context；中途已 committed 的 revision 保留在 run history，但尚未形成完整 handoff，不當作自動 checkpoint recovery。每一步仍先確認 `CloseSessionReport`，不重置計數、不吞 timeout／cancel／fatal／cleanup error。

匿名 RPC fixtures 經真 engine／Store 驗收，局部補頁／附件會讀指定 localhost HTTP 來源；Agent 判讀與 skills 操作仍是 provider fixtures。這不是 Agent 已成功載入技能或自主 acquisition 的 live 證明；局部操作本身不新增工具入口或獨立 CLI 註冊。

### 完整任務內的 intake inventory 更新

Private `updateSlice` 從 complete 或 incomplete 的 committed intake/context 執行一次更新，同樣依序使用三個 fresh Steps/sessions：intake update → wiki revision → context revision。它與 `refreshSlice` 共用版本、下游與 cleanup 接線，本身沒有 Planner、自動 retry 或產品 launcher。

- Caller 授權更新這張 ticket 的 intake，不預先列出尚未發現的附件或 linked issues。Agent 在一個 intake Step 取得新 raw issue，自主處理 inventory 增刪、內容更新及 comments 分頁；不為每個新來源停下申請派工，也不重取無需更新的來源。
- Intake 的 `update=true` 表示這次完整更新任務，`work` 是 Agent 完成工作後提交的來源 selectors／原因紀錄，不是申請書。`previous` 綁定 exact prior intake；取得或改變的 slot 交本次 local result（包含 partial／失敗），移除的 slot 須記錄並退出 active inventory。未改動的 slot 保留 exact prior Source 及真正 owner。窄任務不能用自行填入 `update=true` 擴大授權。
- Active linked／attachment inventory 仍必須符合 raw issue；取得 issue 失敗時用 exact 歷史 raw inventory 驗保留結構，新 issue 仍是缺口，不冒充成功。新增來源尚未取得或分析不完整，必須列 entry、狀態與 gaps，不得省略 inventory。新資料的 byte metadata 驗收保留。
- 不要求不同 ID 提交 old → new 業務替換關係。同 ID 的 content 更新不強迫 analysis 重跑；保留的 analysis 沿真正歷史 owner/binding 引用，不改標成本次新分析。`complete` 表示來源可用性及結構覆蓋的既有驗收，不證明歷史內容仍適用或根因已確認。
- 移除 active source 不刪 committed history，也不使仍引用它的 context evidence 失去 owner。Agent 在新版 context 判斷適用性，保留 resolution attempt history；既有 gap 可以保留，移除則仍須 `resolved_gaps` 與新 evidence 交代，不自動當作補取成功。
- 新版 wiki 仍綁定新版 intake，完整 context 驗收成功才取代 caller state。執行失敗、fatal／取消／限額與 cleanup failure 不改標為普通缺資料；中間 committed 產物不自動成為 recovery checkpoint，同 run 額度不重置。

匿名 localhost HTTP／RPC cases 覆蓋同一 Step 取得新 issue/link/attachment、移除／替換、content-only 更新與歷史 analysis、歷史 bytes 的原 owner metadata、多次更新、接續局部補缺、gap 保留／交代、非法 provenance／inventory／task 升格及失敗路徑。這些 fixtures 不證明真 Agent 已成功載入 skill 或自主決定工具操作。

### 最小 Planner caller 與狀態交接

Private `startPlanner` 從 exact committed supporting context（ready 或 needs-resolution）載入完整 context/intake lineage 及真正 evidence owners，建立明確指定 model 的 Planner session。`step` 可在同一 session 連續執行；`handoff` 只在已有合法 Planner 狀態、確認舊 session 的 CloseSessionReport／Wait／cleanup 後，重新驗收 committed state 並開 fresh session。所有 Steps 沿用同 run 的 session/attempt 限額及 30 分鐘期限，不猜 GLM binding、不繼承一般分析模型。

- `triage.planner.v1` 每 Step 交完整 snapshot：exact context／previous、假說 ID／敘述／assessment／evidence、待驗問題與具體 evidence requirements／basis、gaps、rationale。不是 delta，也不是 worker dispatch、verified claim 或 report。可選的 worker tasks/results 與交接見下節；verification feedback 仍未實作。
- 此單元的 Planner 任務只消費已交付 supporting inputs，形成計畫；不執行新 acquisition 或派工。空假說／無 evidence 的假說可以如實保存，Go 不依 evidence 類型或 assessment 文字判斷真偽。Pending requirements 不含可執行的 model／argv／session 控制欄位，不代表已授權下一工作。
- Controller 驗收 schema、exact context／previous、必要文字與唯一 hypothesis IDs、evidence owner/file、supporting gaps 保留。Planning 本身不能刪除未改版 context 的 gaps；這不是判定缺口不可補救。來源適用性、假說及下一問題由 Agent 判讀，readiness 不升格為 confirmed。
- Fresh session 明列上一份完整 Planner state、supporting context 及所有歷史 evidence owners，不靠舊對話、目錄掃描或複製 raw files。成功驗收及 Decision 持久化後才替換 caller 的 last state。無效 contract 或執行失敗使該 caller 停止，不交出失敗 candidate，也不自動 recovery；原始 fatal／取消／限額／cleanup 錯誤仍返回。
- 匿名 RPC fixtures 覆蓋同 session 多 Step、fresh handoff、revised intake 歷史 owner、supporting query inputs、ready／incomplete、非法 Ref／schema／scope、失敗與會計。這不是正常 Pi 技能操作或指定 GLM live 驗證。

`handoff` 是活動 run 內的顯式操作，不是 crash resume。既有 supporting 任務的 proposal／context 改版接線見下節；一般 worker 與自動批次／reframe 接線見下節；容量訊號、週期 checkpoint、明示政策的 recovery、三方驗證與 M6 報告收尾接線見下節；產品入口已接，live 尚未完成。

### Planner proposal → 既有 supporting 任務 → 新 Planner state

Planner snapshot 可省略 `supporting_work`，或提出一項結構化工作：`kind`、`reason`、`basis` evidence 及 `sources`。Proposal 綁定該 snapshot 的 exact context 與既有 caller scope，不另接受 model、commands、session 或任意下一節點。`pending` 自由文字仍不是派工授權。

- `resolve`：沿既有任務補必要 wiki 及 unresolved identity/time；要求 needs-resolution context，不是 ready context 上的一般假說蒐證。
- `refresh`：沿既有 incomplete intake 限制，明列來源 selectors／理由；不能升格完整更新。
- `update`：完整 intake 更新任務，complete/incomplete 均可；Agent 在任務內自主處理 inventory，`sources` 必須為空，不要求逐項批准。

Private `plannerCaller.support` 只消費最後已驗收 Planner snapshot 的 proposal，重驗 exact committed context、basis owners、任務種類及原有前提，再確認舊 Planner session identity／Wait／cleanup，記錄 Decision 並派送一次既有局部任務。每個 supporting Step 都明列 proposal Ref 與歷史 supporting owners，讓 Agent 讀取理由／basis，實際 acquisition、query 策略與適用性仍由 Agent 處理。Controller 不從 pending、hypothesis assessment 或來源內容猜測應派哪個工作。

完整 supporting context 驗收後記錄結果 Decision，再建立 fresh Planner，交付新 context、原 Planner snapshot 及真正的歷史 evidence owners。接下來成功的 Planner Step 才提交新的完整規劃狀態；Agent 重評假說、問題與 gaps，不自動升格成 verified claim。新 Planner state 的 `previous` 指向提出工作那版 snapshot，`context` 指向該工作產生的直接後繼 context；同 context 的後續規劃仍可重用 session。Fresh handoff 逐版以真正 context 驗 Planner 歷史，不把舊 state／evidence 重綁新版。後續工作須由新 snapshot 再明確提出，舊 caller 不可重複派送。

失敗使 caller 停止並保留 last accepted Planner Ref；中途已提交的 intake/wiki/context 留在 run history，不自動作為 recovery checkpoint。沒有重試或重置同 run 額度，不吞 execution、cancel、fatal、限額與 cleanup failure。`support` 返回 fresh caller 尚不表示已有新 Planner snapshot，仍須成功執行其 `step`。

匿名 localhost HTTP／既有 RPC subprocess 測試涵蓋三種派送、完整與不完整結果、多次 context 改版及 fresh handoff、proposal／selector／Refs 拒絕、歷史 owner、失敗停止與同 run 會計。它們使用真 engine/Store/validators，不是真 Agent skills、指定模型或 live 操作證明。本 supporting caller 不是完整 adaptive loop；後者接線見下節。

### 一般調查 worker 與 Planner 結果交接

Planner 可在 `worker_tasks` 明列任務 ID、source kind、evidence-only／analysis 職責、問題、完成條件、basis、依賴 task IDs 及必要起始搜尋依據；與 `supporting_work` 互斥。Private `plannerCaller.work` 一次只派送 caller 指定的 task ID，依賴必須解析為已接受結果的 exact Refs。不從 pending 或問題文字猜派工，不是自動 queue／adaptive loop；已接受結果的 task ID 不重用，重取須新明列任務。

`triage.worker.v1` 綁定 proposal/context/task/實際 inputs，保存 work、complete/incomplete 交付狀態、evidence、實際 queries、analysis、gaps 與下一問題。Incomplete 保留 gaps，不等 execution failure；complete 不等 confirmed。自有 evidence 由結果 Ref 持有，歷史 evidence 保持真 owner，不複製成新取得。Logs/metrics 起始搜尋具 source/filter、有限 UTC window 與 basis，runtime 類工作保留授權 target 與 intake/wiki 完整性前提；Agent 在任務內自主調整查詢，不逐 query 審批。

Source kind 與 responsibility 是明列的派工欄位，不是 Go 從文字推導的分類。Evidence-only 使用既定 fetch model，analysis 由 caller 明示模型；不猜 GLM ID，不宣稱 vision 或真模型能力已驗。假說真假、證據適用性、工作結果是否足以支持下一判斷均屬 Agent；Go 不根據檔案數、查詢成功、schema 或自由文字認列進展。

Worker 與既有 slice 共用 scope-aware `taskStepRecovery` 的 OpenSession／Step／30 分鐘期限／strict-close 機械。結果須通過綁定、Inputs、evidence、query 驗收及 Decision 後才納入 caller 的 accepted refs；先有 engine committed output 不代表 workflow 已接受。失敗停止 caller、保留最後 Planner state 與原 typed failure，不吞 timeout、fatal 或 cleanup failure。

下一 Planner Step 明交 `worker_results`、原 proposal/context 及所有真正 owners，snapshot 必須保留 caller 已接受的完整結果清單。Fresh handoff 亦交付已接受但尚未納入下一 snapshot 的結果，不靠 session 記憶。Supporting context 改版及其各中間 Steps 保留必要 worker inputs，舊結果仍綁原 context；這不擴張 Context 本身的 evidence eligibility 或舊 supporting proposal basis 規則。結果保留不等於 Controller 認定仍適用。

匿名測試沿原 Store/publication 與真 engine/Store/RPC drivers，驗單項／依賴交付、Planner 引用、fresh handoff、support 改版、錯綁定／ownership／UTC／schema 與 execution/cleanup failure；不替代 live 技能或模型驗收。M2 的自動批次／ledger／reframe 見下節；M3/M4 的容量／recovery、M5 驗證、M6 報告仍獨立。

### Adaptive 調查循環與獨立 wiki 重查

Private `executeInvestigation` 明示獨立 Planner model 與 worker models，接既有 committed context 後由 persistent Planner 的 `adapt` 循環接續。Agent 明列 `workers/support/wiki/reframe/plan/yield` action 與理由；Go 驗收契約後呼叫既有原語，不從問題或 assessment 文字猜下一步。`yield` 僅交 accepted investigation state，不是 verified claim、FinalSelection 或最終報告。第一次缺資料不自動結案，無可行授權路徑的原因與 gaps 由 Agent 交代。

`workReady` 在呼叫 `Parallel` 前依已接受結果解析依賴，按宣告序最多挑三項；live Planner 另占 slot。批次內尚未完成的工作不解除同批依賴；無 ready task 時明交 Planner feedback，不把 hard cap 當 queue，也不自行推論應 blocked。Branches 不並行修改 Planner caller，全部 join 後依宣告序驗收並交付結果。Failure/cancel/cleanup 與原會計保留；半批 committed outputs 不當新 Planner checkpoint。

可選 `ledger` 擴充既有完整 hypotheses/pending/worker tasks/results，adaptive caller 必須交它。`consumed_batch` 精確對照新交付結果；一輪只在一批 worker 結果交回後的 Planner 更新增加。`changes` 是 Agent 明報的 hypothesis ID、狀態變更說明、理由與 basis，沒有固定 hypothesis status taxonomy；Go 只驗 ID／Refs／echo 與計數，不比較 assessment、結果大小、查詢成功或真假。普通 plan/support/wiki Step、換 session 不增加 worker round，不當作假說進展。

連兩輪無明報進展要求 `reframe`（無可行路徑的明列 yield 仍保留 gaps），方向及新搜尋詞由 Agent 決定。`no_progress` 不因 wiki 完成或換 session 歸零；獨立 `reframe_round/reframe_streak` 只記已交付 reframe wiki 的機械邊界，供下一段兩輪檢查，不把重查本身當進展。

調查期 wiki task 綁定 proposal/context/task ID/inputs，沿原 WikiSearch/completeness，保存 terms、previous_terms、理由及真正 evidence owners。新搜尋以獨立 `wiki_results` Ref 明交 Planner／worker／support／fresh handoff，不塞進 ready context 的舊 resolve、不重抓 intake、不改寫舊 `Context.Wiki`。歷史搜尋按自己的 intake/binding 重驗；新 partial/unavailable/not-run gaps 不得用旧 context 的 complete 遮蔽，runtime worker 仍受必要 wiki 完整性前提限制。Terms echo／明列變更是任務驗收，不證明搜尋詞語義不同或 wiki pattern 能支持本次根因。

匿名同一 RPC driver 覆蓋三 worker 加 Planner、逆序完成／有序交付、依賴批次、no-ready feedback、明報進展／兩輪 reframe、wiki partial 與歷史 owners、獨立模型、support/handoff、branch 失敗／取消／cleanup／半提交與會計。沒有新 driver、scheduler、任意總輪數／查詢次數 cap 或 crash resume；真模型／技能／production 與 live 驗收仍未完成，匿名產品入口不代表這些能力。歷史 validation 的 raw evidence 重讀成本尚未完成容量量測，不能把匿名情境通過當作高負載保證。

### 完整 checkpoint 與容量 handoff

`executeInvestigation` 接明示 `*PlannerCapacityPolicy`，nil 保留原 M2 caller 模式；非 nil 沿同一 `adapt/step/handoff` 啟用容量接線，不是第二個 driver。Percent threshold 由 caller 指定有限的 `(0,100]` 值，沒有 live 預設。既有手動 handoff/support 與模型綁定保留。

Planner 的 optional `checkpoint` 與完整 domain snapshot 一起提交，M3 caller 要求精確 echo：policy、dispatch/checkpoint cycle、full-checkpoint 標記、adaptive note 與追加的 Controller feedback。驗收真正交付差異與歷史 prefix，不靠文字猜進展。每第三個 cycle 標記的是當次同一份完整 snapshot，Decision 引用其 exact Ref；沒有另一份 Store/journal，也不因這個標記建立 crash resume。

容量只在已成功接受 Planner state 的 Step 邊界按需取樣。即將 support/yield 的既有關閉路徑不額外查詢或重開。低於門檻續作；達門檻 strict close/Wait 後 fresh，仍執行原 snapshot 的同一個 action，不多跑 Planner Step、不重派工作、不重抓 intake。合法 unknown 保留診斷並維持 checkpoint 續作，不當零、不僅因此結案或反覆換 session。新 Step 不重用舊容量 sample，estimate 不保證 provider admission。

`SessionContextUsage` 的 query/identity/format/error 語義不改：真正錯誤使 handle unusable 並進既有 close，不吞為 unknown。恢復仍屬 M4，沒有在此處猜哪些 ProviderFailed 是 overflow。Capacity feedback、no-ready note 與既有 evidence/results/owners 明交 fresh/support；尚未被下一個正常 Planner Step 消費的診斷只是 pending，不冒稱已 durable。下一成功 snapshot 驗收後才清 pending，歷史 Controller feedback 不可丟棄或改寫。

匿名 fixtures 沿既有 Store/publication 與真 engine/Store/RPC driver，覆蓋週期計數、同一 snapshot checkpoint、unknown/門檻、fresh/support/owners/模型、非法 echo、stats failure/cancel/cleanup 與會計。Test protocol 的 stats control 是 opt-in 外部 provider 邊界，未開時保留舊回覆；不是產品 bypass。完整 live 能力、資料量容量與長歷史 raw/decode 成本仍未驗。

### 明示政策的 failure delivery 與安全續接

`executeInvestigation` 另接 caller 明示的 `*RecoveryPolicy`，nil 保留舊 failure 出口；capacity 與 recovery 可獨立啟用。Planner 的非負 retry budget 沒有 live 預設。只有純 inputs 的 Planner 單次工作以既有 `Scope.Retry` 有限 fresh 重試，不將整個調查放進 Retry、不重置原 run/session/attempt 會計；M5 角色此時不預造。

`Run.SessionIdentity(ctx, handle)` 讀取已 committed 的 owned identity，不 RPC、不取 execution lease、不耗 attempt，在 run 仍接受操作時可於 close 後讀取。Triage 的真正 dispatch callers 事前保存 identity，不依賴成功 Step 才有的 session ID，也不從並發 snapshot 差分猜人。這涵蓋容量 handoff 後 fresh Planner 尚未執行 Step 的失敗路徑。原 `CloseSession` producer/predicate 不改，replacement 前另驗 exact owner、strict-close、Wait、exit 與 cleanup。

可恢復來源僅可信 typed `TimedOut/AttemptDeadline` 與 `CompactionFailed/Compaction`；unknown ProviderFailed 不猜為 overflow。取消、parent/run deadline、storage/journal/run cap、cleanup/unconfirmed 或 locked outcome 不能降級。每個 branch 都檢查；FailFastSibling 只是同批從屬診斷，不單獨觸發 retry。Error wrapper/後續錯誤仍保留 `errors.Is/As` 原 chain，歷史 timeout 不得蓋過新的 fatal/cancel 分類。

一般 worker、wiki、support 失敗明交 typed delivery，不能自動原樣重發遠端操作。`recovery` 與 `recovery_choices` 保留 policy、原 proposal/context、delivery identity、failed attempt/dispatch/cleanup、已接受結果與 phase refs。每批 worker 的成功結果或全敗 feedback 被下一 Planner snapshot 消費，都算一個 round/cycle；`consumed_batch` 仍只是真成功結果，不造假 Ref。是否有假說進展由 Agent 明報，timeout 不是反證，retry/fresh 不另灌輪數。Mixed siblings 仍須真 acceptance/Decision；已 commit 但 branch close 遇 FailFastSibling 的結果保留原 Ref/owner，join 後以 parent context 驗 succeeded attempt、strict cleanup，再接受結果，不偽裝失敗 attempt。其他 fatal/cleanup failure 不降級；未 committed candidate 只是診斷。

未知遠端狀態只能 pending、明列 read-only inspection 或具 basis 的安全改向／resume，不能 caller bool 或換 task ID 冒充安全。安全內容由 Agent 查證及判讀；Go 驗 exact delivery/scope/Refs/轉移，不新增遠端 job 領域 recognizer，也不宣稱 sandbox 或遠端 exactly-once。

Support 沿原 resolve/revise pipeline 保存已驗收 intake/wiki、原 proposal/failed phase 及 resume authorization，重驗 lineage/scope/UTC/receipt/completeness後只補未完成階段。不重抓已接受的 Jira 資料、不把中間 refs 當完整 context/checkpoint、不重綁歷史。Wiki resume 沿原 task/terms/history，partial 不等執行失敗或 no matches。尚未由下一 Planner Step 提交的 delivery/feedback 仍是 pending，不聲稱 crash resume。

若當前 mandatory reframe 的 wiki 失敗，允許其明列 read-only inspection 先查證安全條件，再恢復該 reframe。只限同一未解決 reframe delivery、同一 context／reframe boundary／最後進展 round；一般 worker、舊 reframe 或已解決 delivery 不能繞過兩輪規則。Inspection 不自動歸零 streak，只有真正 wiki 交付才更新 reframe boundary。

匿名案例沿既有 engine/Store/RPC及 filesystem fault 邊界覆蓋 Planner 首步/重試耗盡、全敗/mixed batch、兩輪 reframe、support/wiki phase續作與不重抓資料、identity/模型/echo/歷史/cleanup/fatal。未窮舉所有 sibling/fault 排列；未動態偽造內部 cleanup report 或宣稱 live/job 安全判讀已驗。中性 identity API 與舊 consumers 另保留 engine 回歸。

### 獨立 claim、fresh pro/con/cross 與 Planner feedback

`executeInvestigation` 接 caller 明示的 `VerificationPolicy`，每個 pro/con/cross 的完整 ModelSpec 與非負 retry budget 分別指定，不繼承一般 analysis 模型，也不默套產品預設。啟用時亦要求 M4 RecoveryPolicy；nil verification 沿舊流程。沒有第四個裁判 Agent；此 state-only 操作本身不產最終報告或 renderer，不另註冊產品。

Planner 先提交完整 state，明列新 candidate 或要補缺角的既有 claim。新 candidate 由同一 Planner 角色/model 的正常 Step 產生 `triage.claim.v1`：只含 parent_state/context 的 exact Ref 與 candidate（ID/statement/premises/allowed_evidence），不含完整 Planner 敘事、ledger、舊 verdict 或附檔。Go 驗投影相等、producer/parent binding 與真正 evidence owners，不自行 Publish 第二 Ref。完整 state 透過明列 parent_state＋claim 引用閉包重建，必要 pending committed claim 在 handoff/timeout/下一 state 明交；未被正常 snapshot 消費不冒稱完整 checkpoint。沒有 engine 多 output、input journal 或第二 registry。

三方使用真 Parallel/CollectAll、fresh sessions，每個 attempt 僅自己的角色工作，live Planner 另占 slot。Request inputs 僅 claim 與明列 allowed evidence owners，不注入 parent Planner state 的內容、其他角色/舊 verdict 或 RetryState feedback。Parent/context Ref 是 continuation metadata，不是讀取額外敘事的工作指示；這種 input isolation 不是 filesystem sandbox。

Pro 檢查支持與前提，con 檢查反例/替代解釋，cross 獨立核對適用性、量測有效性及證據一致性，不先看其他兩角再投票。各自的 `triage.verification.v1` 綁 role/claim/exact ordered evidence，Agent 明報支持程度、理由/basis/runtime_basis、window/filter/environment/release、量測有效性、反例與處置、gaps；缺項可明列 unavailable。Go 只驗結構、版本、producer role/model、fresh attempt 與所引 evidence，不從字串、來源 schema 或模型共識認列因果。

Claim Ref 與 evidence 集合固定版本；改 candidate 或 allowed evidence 要產新 claim，三角全 fresh。相同版本只補 unavailable 角色，已 accepted 的角色 Ref 不重跑或改綁。Planner/claim/verifier 沿同一 `retryPlannerInputs` 與既有 Scope.Retry/identity/strict-close/typed failure 機械；只有原已批准 timeout/可信 compaction 可安全有限補做。精確辨認本次 RetryExhausted 及完整已確認 failure history 才產 verification-unavailable；schema/內容拒絕仍是產物 failure，不是反證、PASS 或普通缺資料。取消/fatal/limits/storage/journal/cleanup 及其他 branch error 不得被較早 timeout 蓋過。

同版結果驗收/Decision 後才保留，complete delivery 固定按 pro/con/cross 有序回 Planner。每批完整 feedback（包括明列 unavailable）被下一 Planner snapshot 消費，也算一個 round/cycle；claim production、角色內 retry/fresh 不另計，consumed_batch 仍只有真 worker Refs。是否有假說進展由 Agent 明報，原 reframe 與安全 inspection 規則不變。

既有 Planner 必須提交綁定 delivery/claim 的 `verification_review`：支持程度、runtime/量測限制、可檢驗分歧、反例處置/gaps與等於 ledger.action 的下一步。Agent 決定補證、換版再驗或 yield 調查 state；Go 不投票、不加 hypothesis taxonomy、不因三角完成自動升 confirmed。歷史版本與處置保留，後續新 verifier 不接前輪 verdict。`PendingVerificationError` 明交已 committed state/claim、已驗 partial roles、history/recovery並 Unwrap 原錯誤，不偽造成功 output。

匿名 fixtures 沿原 Store 與真 engine/Store/RPC，驗 pure claim/producer/版本隔離、三角逆序完成、同版補角/換版全驗、callback failure/未完成交付、Agent feedback→蒐證→新 claim 與 reframe。精確 claim acceptance→RetryFinished 的 filesystem 故障時點未動態注入，只有相鄰邊界與 source 證據；不宣稱所有 fault 排列、live 技能/模型或 runtime 因果判讀已驗。

### 獨立報告操作與共用 renderer 機械

R5 已提供 `ExtractReport`、`triage.report.v1`／`ReportFileID`、`InvestigationReport`／`ReportClaim` 與可呼叫的 `plannerCaller.report(ctx, renderer)`。它使用現有 Planner handle 的正常 Step，不新增 writer，不更新原 Planner state-only `last`，也不自行 yield、reserve、retry 或 close。R5 操作本身不接 adaptive 收尾；下節 M6 新入口另接歷史 failure 處置／報告額度及自動交付，產品 Definition 沿用此接線。

Report 明列 accepted state/context、選中的 exact claim/delivery/assessment owner、Planner 宣告的 completeness/closure/gaps/next_steps。沿 previous lineage 重驗真歷史，assessment 不是任取最後一個 state；新版 claim 不重用舊驗證，也不在 report 重寫 statement/premises/支持判讀。沒有 claim 只能宣告 incomplete；Go 驗來源、版本、producer、必填與既有 gaps，不按 support 字串、schema 或模型共識推定因果。

Agent 在該 attempt 執行固定 renderer，exclusive 寫 artifact 及 candidate.files，然後由真 engine Stage/Confirm/Publish/commit。Go 再經 committed resolver／原 reader 驗收，固定 host Python 用同一 embedded 模板重算完整 bytes 並比較；不相信自報 digest，也不執行 Agent 可修改的 script。成功 Decision 後回 report Ref，caller 沿既有 Result／FinalSelection 選 `triage-report` artifact，producer 由 engine 解析，沒有另一份 registry。

正文投影問題、身份環境與已解析時間線、原假說/claim、支持理由、runtime/量測條件、同版三方結果、反例/分歧、缺口/下一步，以及必要 exact Ref/file 追溯。文字使用安全 fences，Ref ordering 固定；完整 Context/PlannerState、ledger/checkpoint 與 owners registry 留工作資料，不整份塞進報告。不生成 Jira/Slack drafts 或新因果。

Extraction 與 Python file/JSON/candidate 機械使用 `contract/reportresource`，review 原 consumer 同步遷移；review 的 Pin/roster/template/appendix 驗收保持原義。Triage producer 各 Ref 讀完關閉局部 descriptors，避免整段歷史累積 FD；request/candidate 仍保持原 stack。Host 用 CommandContext、Run/WaitDelay 及非嵌入式 bounded writer，stdout 64 MiB、stderr 64 KiB；保留 cancellation 與 process errors，不變成普通缺資料。這些限額不代表 Python RSS 或 live 容量已驗。

匿名測試沿原 Store 與真 engine/Store/RPC，包含 historical assessment、兩版 claim、偽 producer/uncommitted/tamper、artifact/renderer/cleanup/cancel 與原 review consumer；另有真 producer 的局部低 FD 上限、多輸入 Ref，以及 io.Copy/host output-limit 回歸。這些單項測試不提供全 EIO/替換競態/live/模型技能證明；產品接線另有 registry／CLI 匿名 pipeline 測試。

### 明示政策的調查收尾與 execution outcome

`executeInvestigationReport` 重用初始化/adapt，要求既有 RecoveryPolicy、caller 明列的 `ReportPolicy{ReportRetries, ReserveSessions, ReserveAttempts}` 與已 extraction 的 renderer。舊 `executeInvestigation` 的 signature、nil policies、state-only 結果及 R5 單項 report caller 不改。

在 accepted yield 的 Planner close 前，執行正常 report Step、committed 驗收／全文 deterministic 比較、Decision、有限 Retry 完成與 strict close，才沿原 Result／FinalSelection 交 artifact。不新增 writer Agent、Store 或 registry。Report retry 只限原可信 timeout／compaction，重用 shared retry、SessionIdentity／strict close、fresh Refs/history。非法產物、host renderer、Decision／RetryFinished、cleanup 或 fatal/cancel 不被當成可重新成功的 report；`PendingReportError` 明交已知真 state/report Ref、Inputs、完成 boundary 與 recovery/verification/report retry/controller feedback，Unwrap 原錯誤，並不是 FinalSelection。

Report v1 的 `m6` 是相容擴充：舊 caller 不接受自行附加 metadata，M6 則要求完整。每個合法 recoverable failure 都綁最初接收的 committed owner、delivery/claim/role/index/原 failure，包括報告未選的 claims。Agent 宣告 `unresolved`、`handled` 或 `redirect`，各有理由及明列 results/basis；Go 驗完整對應、版本、原 producers 與機械續接，不從來源 schema／支持字串判因果或判斷調查必要性。

`handled` 不能只引任意較晚的 Planner state：Planner／claim 要真 retry key 與原 role/model／parent binding，worker/wiki 要原 task 與經已驗 RecoveryChoice 的 resume/proposal/context，support 要原 Proposal/Context/Work、retained phases、Authorization 及 failed phase 的真成功產物。重用原 worker/wiki/context acceptance；support resume 的 key 改變仍可保留 lineage。無法證明該續接則保留 unresolved，或由 Agent 以非空 evidence basis 交代有根據的 redirect，不把普通 state echo 當工作成功。

原 native causes 沿實際 callback 保存，verifier 分支各自收集、join 後才合併，fresh clone 保留；不從 Snapshot.FailureInfo 或文字重造 error。Unresolved 必須 incomplete，仍交原錯誤與非零 exit；後續同版成功或有據 redirect 不抹除診斷／原 assessment。合法缺資料的 incomplete report 可以 execution 成功，但不表示 root cause confirmed；Report 仍投影原 claim/assessment 支持程度，三角完成不等於因果成立。

### 最後報告額度與非保證

這個入口是 Run 調查的唯一 dispatch coordinator，並非通用 reservation API。Caller 的 attempts reserve 至少涵蓋最後 report 與全部有限 retry，sessions reserve 至少涵蓋每次 fresh retry，第一份 report 使用現有 usable Planner。私有入口不默套數字；產品 Definition 明列初版值（見上節），測試值不自行成為 live policy。

Bootstrap 與每個同步段落邊界讀原 Run.Snapshot().Policy/Sessions/Attempts，一次計入普通 action、全部分支／retries／fresh、下一 Planner feedback/retry 與可能容量 handoff 的上界。Worker 最多三個 ready tasks；wiki 一 phase；support resolve/revision 用既有二／三 phases加reopen的保守上界；新 claim 與未補 verifier 各依明示 retry policy，已 accepted 角不重派。所有 branches join、下一 Planner update 成功後才再 admission，沒有 token 帳本、通用 scheduler 或 engine 業務邏輯；算術檢查 overflow。

普通段將侵入 reserve 時，在原 Planner close 前，以最後 accepted state 直接產 resource-limited incomplete report，回普通 workflow failure／非零 exit。不追加停止用 Planner Step，不偽造 ledger yield 或重置 counters。無 accepted state 的 bootstrap 不保證報告；真的 hard cap、fatal/cancel、storage/journal/cleanup 或 byte/disk/deadline 耗盡時不強迫補報告。Snapshot 不是對同 Run 其他並行 dispatcher 的原子預留。

匿名案例沿原 engine/Store/RPC與renderer，包含 persisted final、report recovery／原 error chain、處置完整性及偽 lineage、reserve 差一/剛好、並行反序與 support phase resume。Decision 故障有真完整 completion 路徑；RetryFinished 精確注入僅 shared primitive／report consumer seam，不冒稱完整 finishReport 的所有微時序。未獨立強制所有 undispatched sibling、未捕捉每個初始 Runtime error 物件，也未窮舉交錯、live／模型技能／RSS／容量；source/fixture 採證不是這些能力的保證。

### 同一次驗收內的讀取重用

Triage 的 private `acceptance` 只在一次同步驗收中保留按完整 `contract.Ref` 索引的 verified envelope bytes。每個 Ref 第一次使用仍透過 engine committed resolver／Store 驗 contract、manifest 及所有附檔的路徑、metadata、digest；巢狀 lineage／語意 checker 再用同一 Ref 時不重做相同 Store 讀取，但仍執行自己的 schema binding／業務驗收。每次重新 decode，避免 checker 修改另一個 checker 的資料。

讀取重用不進入 engine、Store 或 caller 的持久狀態，也不保留跨 Step／session／Decision 的授權。新的驗收建立新 `acceptance`；supporting 任務前後分開，fresh Planner／handoff 亦重新驗收。Engine 在 Step inputs／Decision 的獨立 Ref 驗收不變。Cancellation 即使命中已讀 envelope 仍須返回，不缓存失敗；不是永久信任 digest、跳過 validator 或假設 mutable filesystem 已不可變。

此接線只消除同次驗收的重複讀取，不刪 `noSymlinks`、雙 digest、ownership、scope、receipt、UTC、completeness 或 cleanup。Lineage 語意走訪、必要跨邊界重驗、raw evidence 解析及 journal/state 持久化仍有成本。測試保留真 engine/Store/RPC，涵蓋 decoded 值隔離、exact Ref/schema、取消，以及任務前後與 handoff 的 evidence 篡改拒絕。

### 測試分層

同一份 `triageCases` 定義各業務情境的成功／失敗與 readiness 期望，兩個 test drivers 依明列的 `validationStage` 分流，不靠 skip 或 live opt-in 隱藏案例：

- `TestTriageValidation`：契約與資料規則直接走正式 publication validators。使用真 Store 的 BeginAttempt／Stage／Publish／Read，包含 schema、filesystem、raw evidence 與必要 localhost HTTP，不 mock parser 或 validator。每個案例有自己的 candidate/evidence，只共享唯讀 baseline Store publications；負例檢查具體拒絕原因，正例檢查資料與完成狀態。
- `TestIntakeToContext`：保留真 engine／RPC 的流程測試，包括 dispatch/task binding、runtime-resolution prerequisite、exact committed inputs、跨 Step／版本 owners、持續 session／handoff、最後 accepted state、取消／timeout／限額／cleanup，以及 HTTP acquisition 的跨階段傳遞。

Store publication 不等於 engine committed Ref。前一層只驗已載入資料與 Store 邊界，不能宣稱派工、session 或 cleanup 已被測到；後一層負責這些整合保證。Production loaders 仍先做 committed resolution，再呼叫同一份 publication validators，未以測試分層放寬原驗收。新增／搬移案例需核對原有斷言及兩層對照，不只比較總數或單一快案例。

## 驗證與下一步

現有 engine／Store／RPC subprocess fixtures 驗證 deadline opt-out、nullable stats、usage handoff、attempt timeout recovery、cleanup 拒絕、committed Ref 與計數保留。這些是框架與匿名 workflow closure，不是完整 Jira Planner，也不是 Agent 必然遵守軟性規則的證明。

```sh
go test -p 1 -count=1 ./internal/workflows/triage ./internal/runtime ./internal/engine ./internal/testutil/protocol
```

完整回歸與發布方法見 [VERIFICATION](VERIFICATION.md)。既有 shared-discovery 的 parent pid／`.recovering`／ownership gate 保留，屬於 runtime 對自有 process 的安全責任，不因 Agent 操作規則採軟性提示而取消。

局部補取、完整任務內 inventory 更新，以及 identity/time supporting-source／query 工作紀錄已有上述 private 匿名切片；Planner 已能以受驗收 proposal 派送上述既有 supporting 任務並接回新版規劃狀態。[Workflow 重用與 refactor 計畫](WORKFLOW-REUSE.md) 的 R1–R5 共用機制、M1 worker、M2 adaptive loop、M3–M6 checkpoint/recovery／三方驗證／報告及 M7 registry／CLI 產品接線已接入上述流程，不為每個 milestone 新建 session、Store、reader、scheduler 或 test host。已完成的 supporting/state/handoff/讀取去重/測試分層不重做。真 Agent 技能操作與模型 live 驗收保持獨立未驗 gate，不是目前可自行啟動的下一步。不以新增 executable 代替，也不將第一次缺欄位當作結案。真 ticket／環境／可查範圍須由使用者指定，不能以匿名 fixtures 代替真 Pi、指定模型或外部系統驗收。

不提供 OS sandbox、任意 detached 子孫清理、crash resume、exactly-once 或外部副作用 rollback；保留 [DESIGN](../DESIGN.md) 的既有非保證範圍。
