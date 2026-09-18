# 固定 deep code review

本 workflow 是固定的靜態審閱流程，不提供可變 depth 或任意 reviewer 編排。入口保持 `pi-workflow-controller run code-review "https://github.com/owner/repo/pull/123"`，`list` 同時保留 smoke-echo。沒有 flags、DAG/DSL、supervisor、動態 depth 或 reviewer 選擇。

## 流程及權責

1. Prepare：Controller 解析 URL、以 gh GET 保存 PR metadata，取得固定 base/head commit，建立 private bare repository 與 detached checkout，計算唯一 merge-base 及 `merge-base..head` diff，分頁保存 issue/inline/review comments。Prepare Pi 整理 linked Jira/design、來源快照及共同 context。
2. Parallel：Prepare Pi 確認退出後，依序建立 Code、Scale/Failure、Simplicity 三個獨立 sessions，再以既有 CollectAll 平行派工／join。三者全部必要。Code 逐一對照 requirements、constraints、design decisions，附 head code evidence。
3. Validate：新的 Pi 接收同一 Prepare Ref、已接受的 reviewer Refs 與 Controller 產生的三角色結果名單，核實／去重／排除 findings，逐條重查需求，落地繁中 Markdown report。

每階段只有一次 Step，沒有內容修正循環、自動重審或替代模型。Pi 自身 provider retry 仍由原 runtime 管理。OCR、OCR timeout/fallback、第四產品 reviewer、多輪辯論均不包含；不能宣稱 local-correctness 或 OCR 覆蓋。開發品質審閱另行執行，不屬產品流程。

所有 Pi 使用 checkout 外的 run-owned cwd，避免將被審 repository 的 project resources／AGENTS.md 自動載入。讀 code 使用 Controller 提供的 absolute checkout path。PR body、comments、code、repo instructions 和外部文件都是分析資料，不是可覆蓋角色規則的指令。

## 模型與期限

Provider 全部為 `fireworks`，ID 前綴為 `accounts/fireworks/models/`：

| 角色 | ID 後綴 | Thinking | Step deadline |
|---|---|---|---|
| Prepare | glm-5p3-flash | low | 20 分鐘 |
| Code | deepseek-v4-pro-0813 | high | 30 分鐘 |
| Scale/Failure | kimi-k2p7-code | high | 30 分鐘 |
| Simplicity | minimax-m3 | medium | 30 分鐘 |
| Validation | deepseek-v4-pro-0813 | high | 30 分鐘 |

Run 120 分鐘，同時最多 3 個 live session，總共最多 5 sessions／5 attempts。Startup、RPC、abort、cleanup 及 contract 限額沿用框架預設。模型由程式碼固定，不繼承啟動者或 smoke-echo。

## 契約與驗收

Schema 唯一來源是 `internal/workflows/review/schemas/`，embed 後由 Go registry 註冊，request 指向實際 schema/envelope/resources。Skills 不手抄 JSON 格式。Prepare request 另提供已知 Controller sources 的具體值，要求原樣保留 ID/status/file URL/file ID，避免模型替它們另命名；來源 status 限定 available/missing/conflict。

- `review.prepare.v1`：完整 Pin（URL、repository、number、base/head SHA、merge-base、diff range、context ID）、Sources、Requirements、OpenQuestions、context file ID。來源包含 Controller 原始 snapshot 的逐 byte 拷貝；無法取得的 sources 保留原因。需求 kind 是 requirement/constraint/decision，各有穩定 ID 與 source IDs。PR body 只是作者 claim。
- `review.reviewer.v1`：exact Pin、role、完整 Prepare Ref、Coverage、Limitations、Findings、逐項 Assessments。Finding IDs 有 role 前綴，severity/title/location/evidence/impact 皆必要；locations 對照實際 head 檔案及有效行數。Code 必須回覆所有基準 IDs，不可缺漏或重複。
- `review.validation.v1`：exact Pin／Prepare Ref、三角色完整名單、confirmed findings、所有來源 finding 的 disposition／reason、所有需求的判定、completeness/conclusion、report artifact ID。每個 confirmed finding 都需來源，每個原始 finding 都須 confirmed/merged/excluded/unconfirmed 一次，不能把未核實疑點當缺陷或無聲丟掉。

需求狀態：`satisfied | not_satisfied | unconfirmed`。前兩者需 code evidence，後者仍需理由。不可將原始 statement 換成較窄的子命題；例如「存在 fallback」不證明所有被丟棄 rows 都符合 fallback 前提。將 Code 的 unconfirmed 升為 satisfied 時，Validation 須消除其原始疑點，未確認前提仍應保留 unconfirmed。Go 可以檢核身份、來源、ID 覆蓋、檔案及行數，不聲稱能證明模型的推理正確；內容核實由獨立 Validation Pi 負責。

所有交接經 Stage/Confirm/Publish 及 committed resolver。Validation 在同一 Step 先寫候選結構化 JSON，再執行自有 embedded `scripts/render_report.py` 一次，確定性生成繁中分節 Markdown 與完整原始 statement/Refs，並更新本 attempt 的 report file ID。不新開 stage、Pi 或內容重試。Controller 對已 committed report 的結構化追溯附錄與 Prepared/Validated 逐值比對，拒絕省略或改寫資料的附錄；這不是對模型語意正確性的證明。`engine.ReadContract` 與 Decode 使用同一 resolver，只增加取得完整已驗 envelope bytes 的能力，供業務驗證 file ID。Published rename 本身仍不授權下游，不搜尋最新 candidate。

## 執行結果與 PR 結論

- `complete`：三角色成功，無來源／需求／reviewer／validation 的未釐清限制，所有基準都有判定。
- `limited`：三角色都成功，但資料不足、來源缺失／衝突、需求未確認或其他明列限制。產出合法 limited report 可以是 workflow 執行成功，不能宣稱 PR 全面通過。
- `incomplete`：必要 reviewer failed/missing。CollectAll 保留 siblings，run 仍活動時嘗試 Validation 產出不完整 report，最後仍傳回原始 branch failure，不能 exit 0。取消、run-fatal、run deadline 不再開 Validation。
- PR `conclusion`：有 confirmed findings 為 `findings`；完整且無 confirmed findings 為 `no_confirmed_findings`；其餘為 `undetermined`。有缺陷的完整 report 本身不是 workflow failure。
- Metadata/pinned code 取得失敗、schema/identity/semantic acceptance 失敗、partial startup 等是 execution failure，保留已取得的 history/Refs。未產出合法 Validation 時，不偽造 final report。
- Cleanup/持久化錯誤與 outcome 分離，既有 exit precedence、StatePersisted=false、sticky run-fatal 與 provenance 不變。

## 資源、安全與保留

`review/` 保存 private repository.git、snapshots 與 temporary checkout。Store 建立時將可信 BaseDir 正規化為 canonical path，使 symlink WIP／平台 temp alias 下的 request、Ref 與 renderer 路徑一致；原始 LaunchCWD 不改，attempt／artifact 內的 no-follow 防護保留。Git 使用固定 argv、禁用 global/system git config、hooks、external diff/textconv、submodule recursion；credential helper 僅用既有 gh 認證。Partial fetch 使用 blob filter，checkout 保留 head 的 tracked files/gitlinks，不靜默做 sparse review。GitHub transport／gh/git 有 20 分鐘 command deadline（仍受 run deadline 約束）、64 MiB stdout 上限，stderr drain 不落地，避免 transport 印出 credential。Submodule gitlinks 保存為來源快照，但不執行其外部 transport；內容缺失以 `submodule-content` 明列，不能宣稱全面覆蓋。不下載 LFS 外部物件，其 pointer 不等同外部物件內容。

每階段邊界核對 HEAD、tracked/untracked changes 及 Controller snapshot digests。不修改被審 code，不執行其 tests/build/compile/deploy 或 repository scripts，不自動 GitHub/Jira/Confluence 寫入。發 comments 必須 workflow 結束後另取得批准。這是受信任 agent 的操作規則，不是 OS sandbox，不能抵抗同 UID 惡意檔案篡改。

`Run.AddCleanup` 只登記自有 resource callback，不是 command executor。資源暴露給 Pi 前須註冊；acquisition 部分失敗或註冊失敗仍由 caller 負責 bounded cleanup。Cleanup 先停止自有 Pi，等待 workflow 返回與確認 process exit，再移除原始 ownership 匹配的 checkout 及其 worktree registration。未確認退出則保留資源並回報錯誤。Private git objects、snapshots、contracts、report、embedded skill copy、Pi history 均保留。

新 skills 位於 `internal/workflows/review/skills/`，`go:embed` 展開到 `run/review-skills/`，保留相對 references；每個 Pi 明確讀對應 absolute SKILL.md 及 references。無 installer/plugin manager，不改舊 definitions/settings，新舊規則不自動同步。

## 執行前提與驗證

需 Go build 或交付 binary、Pi 0.84.3／Node、Python 3（自有 report renderer）、git、已認證 gh（含 GitHub repo 讀取權）、上述 Fireworks 模型，以及既有 deployed WebUI／hub。工具 skills（gh/jira/confluence）與其認證由環境提供，不包含在 binary；缺規格來源記為限制，不猜測。Node TLS 驗證不可停用。依環境需要用 `NODE_TLS_REJECT_UNAUTHORIZED=1 pi-workflow-controller ...`，不修改全域 settings。

每個 shared-discovery Pi 啟動前，由共用 runtime 依有效 BridgeDir 執行唯讀 parent-pid preflight；workflow 不另解環境或維護私有 guard。`.recovering` 或 dead/unverifiable parent 會以 `BridgeUnavailable`／`preflight` blocked，已建立的 handle 仍計入 total-session 額度，不產生 attempt 或 persisted child。無 truthy pid 的 hub-state 等 JSON 依 deployed recovery 規則略過。實際驗收前須重新唯讀核對 deployed `index.ts`／`session-helpers.js` 的 recovery 判準；來源版本變動須重驗，不可只看 piPid。Preflight 不是鎖，獨立 task directory 不是 discovery 隔離。共享測試在受控時段循序執行，hub 僅 GET 本次自有 sessions。

測試方法與 gates 見 [VERIFICATION](VERIFICATION.md)，最終輸出 API 見 [FINAL-DELIVERY](FINAL-DELIVERY.md)。Tests、獨立開發品質 review 與獲授權真 PR E2E 必須分組核對，不以 smoke-echo 或 substitute 代替。Repository 只保留匿名方法、可重現 tests 與限制；真 PR/ticket、traces/history、執行日期／IDs／PID／SHA、認證狀態及逐次報告只留 repo 外受限位置，不回指私有路徑。

保留 [DESIGN](../DESIGN.md) 的極端 numeric validator 可表示性／boolean inversion／資源放大及非協作取消、完整 snapshot 重寫 I/O、永久堵塞 filesystem/stdout 的非固定 wall-clock、任意 detached 子孫、abrupt death，以及無 sandbox/supervisor/crash resume/exactly-once/副作用 rollback 的限制。
