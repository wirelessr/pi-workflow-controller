# Jira triage：workflow 與執行期適配

移植已有 deadline、context usage、cleanup report、匿名 handoff／timeout recovery 適配，以及 `internal/workflows/triage` 的匿名 acquisition → intake → wiki → triage context／局部補缺與最小 Planner 狀態交接切片。**尚未註冊 `jira-triage` CLI workflow**，不是完整調查或真 Pi／provider／production 驗收。

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

## 模型與預算

機械拉取角色固定 `fireworks/accounts/fireworks/models/deepseek-v4p1-flash`；Planner／驗證／報告使用 GLM-5.3，一般分析使用 GLM-5.3 flash。完整角色 binding、thinking 與 live 能力仍須驗證，不默換模型。

Jira triage 從 `DefaultRunPolicy()` 起設 `DisableRunTimeout = true`，不增加 Controller 整體 run deadline；每 Step 保留 30 分鐘。所有 numeric limits 仍須正值，保留舊有零／負值 invalid-definition 行為。Parent cancellation/deadline、startup/RPC/cleanup timeout、資源 hard caps 與既有 workflows 的預設皆不變。

## Context handoff 與 timeout recovery

- `Run.SessionContextUsage(ctx, handle)` 在 Step 邊界取得 lease，按需查 `get_session_stats`，不新增 health polling。結果帶 identity、seq／epoch、取樣時間與 nullable tokens/window/percent；unknown 不等於零，estimate 不保證 provider admission。
- `Run.CloseSessionReport` 重取既有 closeOnce 結果並複製 slices；未完成不能視為 clean。Recovery 另驗 identity、WaitCompleted、ProcessExited、Unconfirmed、WaitError、KillError、DiscoveryError；舊 `CloseSession` error predicate 不變。
- 成功 Planner Step 的 committed state 須能重建決策狀態，每三個 dispatch cycles 另做完整 checkpoint。確認舊 session cleanup 後，再 OpenSession，以已 committed Refs 接續。
- Timeout 沒有新合法 Ref，從上一份 committed state 加後續已提交證據接續，不要求失敗 session 補 checkpoint、不重置 session／attempt accounting。
- 本機 Wait 不證明 remote async job 已結束；未知 ProviderFailed 不分類成 overflow。取消、storage/journal failure、run hard cap 不可吞掉進 recovery。

## 匿名 intake → context 切片

切片的初始階段使用三個獨立 Step／session，沿用真 engine、Store、committed resolver 與 RPC protocol harness。匿名 HTTP 案例在 provider 邊界實際呼叫 workflow-owned acquisition helper，將其原始檔案直接交給同一 candidate／Store 路徑；其他 Agent 分析、wiki 與 DB receipt 仍為匿名 fixtures。沒有通用 orchestrator、工具 adapter 層、共通 instructions 或正式 launcher。Private `executeSlice`／`resolveSlice`／`refreshSlice`／`updateSlice` 及下述 Planner caller 目前由匿名測試串接；GLM binding、thinking 與每次啟動前 shared-discovery preflight 尚未接到產品入口，不提供未驗證的預設值。

- `triage.intake.v1`：完整 issue/raw fields、field metadata、各 comment 原始頁、linked issue snapshots、附件 content／analysis manifest、來源 URL／取得時間及 gaps。Go 核對 raw key、必要欄位、分頁 offset／total／唯一 comment IDs、linked／attachment inventory 及附件 byte size；historical content 在其真正 owner 版本驗 byte metadata，不以新版 issue 的 size 否定舊 bytes。拒絕省略 inventory、截斷本次下載或偽稱 complete。部分／缺失／unsafe／too-large／未完成分析保留為明確缺口，不等於空結果。
- `triage.wiki.v1`：綁定 exact intake Ref，保存搜尋詞、wiki-only scope、搜尋證據與已讀頁面；區分完成有結果、完成無結果、partial、unavailable、not-run。未完成不得偽裝 no matches。本次開發只使用匿名 wiki fixtures，不存取實際 vault。
- `triage.context.v1`：綁定 exact intake/wiki Refs 與 caller 授權 scope，保留身份、binding、release、observations、identity/time resolution attempts、附件/wiki 完整性與上游 gaps。Evidence 使用 exact input Ref＋file ID，或 null Ref 表示本 contract 自有 evidence，不重複宣告其他 attempt 的附檔。Resolved identity 須有 target/DB resolution receipt，核對唯一 tenant/orgkey row、stack/PoP/binding/release；多 row、錯環境或版本不符不可宣稱 resolved。
- UTC 正規化驗收支援 explicit-offset RFC3339、epoch seconds/millis、同事件 local／epoch 配對與 offset 實算。保留原始時間、來源、UTC 與計算；不接受無 offset 的時間字串冒充 RFC3339，也不接受 local timestamp 自身作 absolute evidence。From/to 為已觀測 incident anchors 的 min/max，單點事件可為同一時刻；它與實際查詢窗口分開。Agent 在任務內依可靠時間依據與資料量自主選擇非零小窗，不要求每次涵蓋完整事故區間，也不逐查請批。
- Ticket-only 授權仍可執行 intake/wiki/local triage，不要求先知道 tenant/PoP 才能保存資料。只有 caller 已明確授權 target 且 intake/wiki 完整時，task 才允許唯讀 runtime resolution；此 task 規則不是 shell sandbox。
- 每個成功 Step 提交後確認舊 session cleanup，才開下一個 session。輸出為 supporting context，`ready` 只代表本切片前提驗收；`needs-resolution` 保留待補工作，不是 blocked 結案。沒有 `FinalSelection`、報告、draft、publish 或 confirmed root cause。Step failure／取消／hard cap／cleanup failure 原樣返回，不重新標為缺資料。

完整性驗收檢查 contracts／raw snapshots 與算術／版本關係，不能證明模型正確理解原始證據，也不是 live DB 或 wiki 驗證。匿名 acquisition 現在另有實際 HTTP／archive 邊界測試，不再只靠 unsafe／oversized 狀態 fixtures；既有純 contract 案例仍保留作產物拒絕測試。

### 匿名 acquisition 測試支援

既有未 export 的 `acquireIntake` 目前只有測試 callers，用於產生真 localhost HTTP／ZIP evidence，再交給 engine／Store 驗收。保留這些既有實作與 regression，不把它升格為 Agent 必須呼叫的產品工具：沒有獨立 executable、helper 路徑接線或 credential configuration 介面。

以下是該 Go helper 已測的資料處理行為，不代表真 Agent 已透過既有 skill 完成 acquisition，也不是新增 shell 入口的待辦：

- 請求 `/rest/api/3/issue/<key>?fields=*all`、field metadata、獨立 comments endpoint 的所有頁及 linked issue snapshots；從 issue 原始 inventory 取得附件。保留 unabridged JSON／ADF／raw pages；總數變動、錯 offset、重複 IDs、缺頁、HTTP failure／截斷、malformed response 都留下 partial 與 gaps，不當成空結果。
- Body 從第一筆直接串流寫入 attempt evidence，以 generated IDs exclusive create，不用來源 filename 當落盤路徑。純文字通過格式檢查後由 analysis 引用原 content，不另存相同副本。不先落別處或以摘要替換 raw。`acquisition` Source 指向受限額計算的 metadata evidence，包含 HTTP status、取得時間、logical source ID、origin、comment offset、partial diagnostics，以及 ZIP filename → extracted file ID 對照。
- Authorization 僅送至設定的 HTTPS exact origin（scheme／host／port）；跨 origin redirect 後不恢復認證，拒絕 userinfo／HTTPS downgrade，保留 redirect body。匿名 localhost HTTP 不送認證；不繼承 process proxy、不停用 TLS 驗證。診斷不記錄 Authorization／cookies／query；原始 API body 自帶的附件 URL 不改寫。
- 預設亦為硬上限：單檔 8 MiB、單 acquisition 64 MiB／96 files、32 comment pages；可調低，metadata／raw／extraction 共用限額。這是 helper 資料取得上限，不是整體 run deadline 或調查輪數限制。
- 實際 ZIP extraction 驗 traversal／absolute／backslash／drive path、symlink／special file、重名及展開 byte／count 上限；僅支援文字與非遞迴 ZIP 文字內容。拒絕或未完成時保存 archive／可取得的 partial evidence，不假稱 vision、完整 bundle 分析或 root cause。Cancellation 返回原 cause，filesystem failure 不改標成普通資料缺口。

### Committed context 的局部補缺

`resolveSlice` 每次執行一個 Controller 指定的局部 cycle，可在同 run 從新版 committed context 再呼叫，不是完整 adaptive Planner 或自動無限 retry。

1. 重新驗收 exact committed context lineage、scope、intake/wiki Refs 與 evidence。`ready` context 不增加工作；`needs-resolution` 才記錄 dispatch Decision。
2. 必要 wiki 未完成，先用 fresh Step 執行 remedial search。新搜尋仍可 partial／unavailable，不改標 no matches；cleanup 未確認則不啟動下一階段。
3. Fresh context Step 只處理 Controller 指定的 unresolved identity/time 與 wiki 狀態。重用同一 committed intake，不重新抓 Jira；已解析 identity／UTC anchors、observations 與 resolution attempts 保留，舊自有 evidence 改以 exact previous Ref 引用，不重複複製附件。
4. 新 context 以 `previous` 綁定舊版本。移除舊 gap 必須提供 `resolved_gaps` evidence，且包含本次新 evidence 或新版 wiki evidence；上游仍存在的 gaps 不可刪除。舊版與新版 evidence 都是顯式 Step inputs。這是 provenance／readiness 驗收，不是對證據語意的獨立事實證明。
5. 所有 session／attempt 沿用同 run accounting；provider failure／timeout／取消／hard cap／cleanup failure 原樣返回，不補交未 committed candidate，也不替換最後已驗收 state。新 context 仍只是 supporting state，remaining gaps 不等於結案。

此局部 cycle 本身不修補 comments／附件等 acquisition gap，改由下節 `refreshSlice` 指定來源補取；不因 wiki 重試成功就偽稱 intake complete。每個 cycle 仍重新驗收完整 lineage 與 evidence digests；同一次同步驗收內重用已驗過的 contract envelopes，避免巢狀 checker 重複呼叫 Store（見下節）。這不是跨 Step 的 cache，也不宣稱已解決長歷史的所有成本。同 intake 的已解析前提若出現新矛盾，仍需後續 invalidation／reframe 路徑，本切片不靜默改寫。尚未接 live wiki／DB／Sumo／Prism、真 Agent 依既有 skill 取得資料或完整調查 loop。Supporting-source acquisition 與 query readiness 的局部接線及匿名驗收見下節。

### Identity/time supporting sources 與任務內 query readiness

Context、context-resolution、context-revision 共用 identity/time acquisition 的工作要求，沿既有 skills/tools 保存 target／release 原始查證、lookup query/response 與 normalized receipt。沒有 acquisition executable、新工具 wrapper、query-approval Step 或產品入口。

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

匿名 RPC fixtures 經真 engine／Store 驗收，局部補頁／附件會讀指定 localhost HTTP 來源；Agent 判讀與 skills 操作仍是 provider fixtures。這不是 Agent 已成功載入技能或自主 acquisition 的 live 證明，也沒有新增工具入口或 CLI 註冊。

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

- `triage.planner.v1` 每 Step 交完整 snapshot：exact context／previous、假說 ID／敘述／assessment／evidence、待驗問題與具體 evidence requirements／basis、gaps、rationale。不是 delta，也不是 worker dispatch、verified claim 或 report。尚無 worker results／verification feedback，後續擴充時仍須明列交接。
- 此單元的 Planner 任務只消費已交付 supporting inputs，形成計畫；不執行新 acquisition 或派工。空假說／無 evidence 的假說可以如實保存，Go 不依 evidence 類型或 assessment 文字判斷真偽。Pending requirements 不含可執行的 model／argv／session 控制欄位，不代表已授權下一工作。
- Controller 驗收 schema、exact context／previous、必要文字與唯一 hypothesis IDs、evidence owner/file、supporting gaps 保留。Planning 本身不能刪除未改版 context 的 gaps；這不是判定缺口不可補救。來源適用性、假說及下一問題由 Agent 判讀，readiness 不升格為 confirmed。
- Fresh session 明列上一份完整 Planner state、supporting context 及所有歷史 evidence owners，不靠舊對話、目錄掃描或複製 raw files。成功驗收及 Decision 持久化後才替換 caller 的 last state。無效 contract 或執行失敗使該 caller 停止，不交出失敗 candidate，也不自動 recovery；原始 fatal／取消／限額／cleanup 錯誤仍返回。
- 匿名 RPC fixtures 覆蓋同 session 多 Step、fresh handoff、revised intake 歷史 owner、supporting query inputs、ready／incomplete、非法 Ref／schema／scope、失敗與會計。這不是正常 Pi 技能操作或指定 GLM live 驗證。

`handoff` 是活動 run 內的顯式操作，不是 crash resume；未註冊產品入口。既有 supporting 任務的 proposal／context 改版接線見下節；調查 workers、容量訊號觸發／timeout recovery、兩輪無進展 reframe、每三 dispatch cycles checkpoint、同版三方驗證與報告仍未實作。

### Planner proposal → 既有 supporting 任務 → 新 Planner state

Planner snapshot 可省略 `supporting_work`，或提出一項結構化工作：`kind`、`reason`、`basis` evidence 及 `sources`。Proposal 綁定該 snapshot 的 exact context 與既有 caller scope，不另接受 model、commands、session 或任意下一節點。`pending` 自由文字仍不是派工授權。

- `resolve`：沿既有任務補必要 wiki 及 unresolved identity/time；要求 needs-resolution context，不是 ready context 上的一般假說蒐證。
- `refresh`：沿既有 incomplete intake 限制，明列來源 selectors／理由；不能升格完整更新。
- `update`：完整 intake 更新任務，complete/incomplete 均可；Agent 在任務內自主處理 inventory，`sources` 必須為空，不要求逐項批准。

Private `plannerCaller.support` 只消費最後已驗收 Planner snapshot 的 proposal，重驗 exact committed context、basis owners、任務種類及原有前提，再確認舊 Planner session identity／Wait／cleanup，記錄 Decision 並派送一次既有局部任務。每個 supporting Step 都明列 proposal Ref 與歷史 supporting owners，讓 Agent 讀取理由／basis，實際 acquisition、query 策略與適用性仍由 Agent 處理。Controller 不從 pending、hypothesis assessment 或來源內容猜測應派哪個工作。

完整 supporting context 驗收後記錄結果 Decision，再建立 fresh Planner，交付新 context、原 Planner snapshot 及真正的歷史 evidence owners。接下來成功的 Planner Step 才提交新的完整規劃狀態；Agent 重評假說、問題與 gaps，不自動升格成 verified claim。新 Planner state 的 `previous` 指向提出工作那版 snapshot，`context` 指向該工作產生的直接後繼 context；同 context 的後續規劃仍可重用 session。Fresh handoff 逐版以真正 context 驗 Planner 歷史，不把舊 state／evidence 重綁新版。後續工作須由新 snapshot 再明確提出，舊 caller 不可重複派送。

失敗使 caller 停止並保留 last accepted Planner Ref；中途已提交的 intake/wiki/context 留在 run history，不自動作為 recovery checkpoint。沒有重試或重置同 run 額度，不吞 execution、cancel、fatal、限額與 cleanup failure。`support` 返回 fresh caller 尚不表示已有新 Planner snapshot，仍須成功執行其 `step`。

匿名 localhost HTTP／既有 RPC subprocess 測試涵蓋三種派送、完整與不完整結果、多次 context 改版及 fresh handoff、proposal／selector／Refs 拒絕、歷史 owner、失敗停止與同 run 會計。它們使用真 engine/Store/validators，不是真 Agent skills、指定模型或 live 操作證明。這不是完整 adaptive loop，也未新增 ready context 的一般 logs/metrics 假說蒐證 worker。

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

局部補取、完整任務內 inventory 更新，以及 identity/time supporting-source／query 工作紀錄已有上述 private 匿名切片；Planner 已能以受驗收 proposal 派送上述既有 supporting 任務並接回新版規劃狀態。後續主線依 [Workflow 重用與 refactor 計畫](WORKFLOW-REUSE.md) 的 R1–R4 前置 gate 收斂共用機制，再按 M1–M7 接一般調查 worker、adaptive loop、checkpoint/recovery、三方驗證與報告，不為每個 milestone 新建 session、Store、reader、scheduler 或 test host。已完成的 supporting/state/handoff/讀取去重/測試分層不重做。真 Agent 技能操作與模型 live 驗收保持獨立未驗 gate，不是目前可自行啟動的下一步。不以新增 executable 代替，也不將第一次缺欄位當作結案。真 ticket／環境／可查範圍須由使用者指定，不能以匿名 fixtures 代替真 Pi、指定模型或外部系統驗收。

不提供 OS sandbox、任意 detached 子孫清理、crash resume、exactly-once 或外部副作用 rollback；保留 [DESIGN](../DESIGN.md) 的既有非保證範圍。
