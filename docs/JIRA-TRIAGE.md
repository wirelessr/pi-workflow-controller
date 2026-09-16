# Jira triage：workflow 與執行期適配

移植已有 deadline、context usage、cleanup report、匿名 handoff／timeout recovery 適配，以及 `internal/workflows/triage` 的匿名 acquisition → intake → wiki → triage context／局部補缺切片。**尚未註冊 `jira-triage` CLI workflow**，不是完整調查或真 Pi／provider／production 驗收。

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

版本／ownership 驗收不等於內容適用性判讀。Agent 決定歷史 wiki、facts 與時間證據是否仍適用；Controller 不依 wiki 內容、evidence schema 或 fact status 新增認列規則。Exact 引用歷史 evidence 可以保留其原始 provenance，但不能把舊 wiki contract 的 intake binding 改稱新版。既有 receipt／UTC／completeness 驗收仍保留。

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

切片的初始階段使用三個獨立 Step／session，沿用真 engine、Store、committed resolver 與 RPC protocol harness。匿名 HTTP 案例在 provider 邊界實際呼叫 workflow-owned acquisition helper，將其原始檔案直接交給同一 candidate／Store 路徑；其他 Agent 分析、wiki 與 DB receipt 仍為匿名 fixtures。沒有通用 orchestrator、工具 adapter 層、共通 instructions 或正式 launcher。Private `executeSlice`／`resolveSlice`／`refreshSlice` 目前由匿名測試 caller 串接；GLM binding、thinking 與每次啟動前 shared-discovery preflight 尚未接到產品入口，不提供未驗證的預設值。

- `triage.intake.v1`：完整 issue/raw fields、field metadata、各 comment 原始頁、linked issue snapshots、附件 content／analysis manifest、來源 URL／取得時間及 gaps。Go 核對 raw key、必要欄位、分頁 offset／total／唯一 comment IDs、linked／attachment inventory 及附件 byte size；拒絕省略 inventory、截斷檔案或偽稱 complete。部分／缺失／unsafe／too-large／未完成分析保留為明確缺口，不等於空結果。
- `triage.wiki.v1`：綁定 exact intake Ref，保存搜尋詞、wiki-only scope、搜尋證據與已讀頁面；區分完成有結果、完成無結果、partial、unavailable、not-run。未完成不得偽裝 no matches。本次開發只使用匿名 wiki fixtures，不存取實際 vault。
- `triage.context.v1`：綁定 exact intake/wiki Refs 與 caller 授權 scope，保留身份、binding、release、observations、identity/time resolution attempts、附件/wiki 完整性與上游 gaps。Evidence 使用 exact input Ref＋file ID，或 null Ref 表示本 contract 自有 evidence，不重複宣告其他 attempt 的附檔。Resolved identity 須有 target/DB resolution receipt，核對唯一 tenant/orgkey row、stack/PoP/binding/release；多 row、錯環境或版本不符不可宣稱 resolved。
- UTC 正規化驗收支援 explicit-offset RFC3339、epoch seconds/millis、同事件 local／epoch 配對與 offset 實算。保留原始時間、來源、UTC 與計算；不接受無 offset 的時間字串冒充 RFC3339，也不接受 local timestamp 自身作 absolute evidence。From/to 為已觀測 incident anchors 的 min/max，單點事件可為同一時刻；不是已授權的 production 查詢窗口，查詢的非零窗口／擴展理由仍由後續 Planner 驗收。
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

此局部 cycle 本身不修補 comments／附件等 acquisition gap，改由下節 `refreshSlice` 指定來源補取；不因 wiki 重試成功就偽稱 intake complete。每個 cycle 會重驗完整 lineage 與 evidence digests，同一 intake 在歷史中會被重複讀取；長歷史的驗證成本尚未最佳化，不宣稱已適合完整 Planner 的長期執行。同 intake 的已解析前提若出現新矛盾，仍需後續 invalidation／reframe 路徑，本切片不靜默改寫。尚未接 live wiki／DB、原始 lookup acquisition、Sumo／Prism query readiness、真 Agent 依既有 skill 取得資料或完整調查 loop。

### Committed incomplete intake 的局部版本交接

Private `refreshSlice` 從既有 committed context／incomplete intake 執行一個指定 cycle，三個 fresh Step/session 依序產生 intake revision、wiki revision、context revision。Caller 明列 source selectors 與缺少／失效原因，Controller 不從內容推斷失效，也不替 Agent 選工具。

- Intake 以 `previous` 綁定 exact prior intake，`work` 保存此次指定來源與原因。Selectors 為 `issue`、`fields`、`comment:<startAt>`、`linked:<key>`、`attachment-content:<id>`、`attachment-analysis:<id>`。可补缺少的 comment page；其他 slots 必須已在原 inventory，任意 inventory 增刪與完整 intake 的重新 acquisition 尚不支援。
- 每個指定 slot 保存本次 local result，包括失敗／partial 狀態；本次 `acquisition` 必須有自有 metadata／diagnostics evidence。其餘 slots 保持原狀，有檔案者以 `Source.ref`＋`file_id` 指向真正的歷史 owner，不重複宣告舊附檔。歷史 raw／失敗資料仍留在原 committed contract。Issue 補取失敗時，沿 exact 歷史 issue inventory 檢查保留 slots 的結構／byte metadata；不把 inventory 當成本次成功的 issue，也不因此改標 complete。Agent 的 metadata 內容判讀不由 Controller 重寫。
- 新版 wiki 必須綁定新版 intake，重新交付本次搜尋的 local evidence 與實際完成狀態；partial／unavailable／not-run 仍是缺口。舊 wiki 是顯式歷史 input，不是新版搜尋的替身。
- Context 同時綁定新版 intake/wiki 與 previous context。Agent 保留仍適用的資訊、重評受影響身份／時間／observations；Controller 只接來源／版本關係並保留既有 receipt、UTC、scope、gap 驗收，不自行建立歷史 wiki 的適用性或 facts 認列機制。跨 intake 可更新原先 resolved 的資料，原版仍保留，resolution attempt history 不可丟棄。同 intake `resolveSlice` 的原有保留規則不变。
- 所有保留的 intake/context/wiki evidence owners 都是 exact committed Step inputs。失敗不替換 caller 的最後已驗收 context；中途已 committed 的 revision 保留在 run history，但尚未形成完整 handoff，不當作自動 checkpoint recovery。每一步仍先確認 `CloseSessionReport`，不重置計數、不吞 timeout／cancel／fatal／cleanup error。

匿名 RPC fixtures 經真 engine／Store 驗收，局部補頁／附件會讀指定 localhost HTTP 來源；Agent 判讀與 skills 操作仍是 provider fixtures。這不是 Agent 已成功載入技能或自主 acquisition 的 live 證明，也沒有新增工具入口或 CLI 註冊。

## 驗證與下一步

現有 engine／Store／RPC subprocess fixtures 驗證 deadline opt-out、nullable stats、usage handoff、attempt timeout recovery、cleanup 拒絕、committed Ref 與計數保留。這些是框架與匿名 workflow closure，不是完整 Jira Planner，也不是 Agent 必然遵守軟性規則的證明。

```sh
go test -p 1 -count=1 ./internal/workflows/triage ./internal/runtime ./internal/engine ./internal/testutil/protocol
```

完整回歸與發布方法見 [VERIFICATION](VERIFICATION.md)。既有 shared-discovery 的 parent pid／`.recovering`／ownership gate 保留，屬於 runtime 對自有 process 的安全責任，不因 Agent 操作規則採軟性提示而取消。

局部來源補取／版本交接已有上述 private 匿名切片；下一步仍需 Agent 技能操作的授權 live 驗收、較廣的 inventory 變更、identity/time 的實際 supporting-source acquisition 與 query readiness。不以新增 executable 代替，也不重做已完成 intake 或將第一次缺欄位當作結案。之後才加入 Planner／bounded workers、reframe、每 Step 可重建狀態／每三輪 checkpoint、同版三方驗證與 deterministic report。真 ticket／環境／可查範圍須由使用者指定，不能以匿名 fixtures 代替真 Pi、指定模型或外部系統驗收。

不提供 OS sandbox、任意 detached 子孫清理、crash resume、exactly-once 或外部副作用 rollback；保留 [DESIGN](../DESIGN.md) 的既有非保證範圍。
