# Pi Workflow Controller

[![CI](https://github.com/wirelessr/pi-workflow-controller/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/wirelessr/pi-workflow-controller/actions/workflows/ci.yml?query=branch%3Amain)

以 Go control flow 編排多個獨立、持久化 Pi session 的本地 Controller，提供 JSON contract 交接、檢核、retry／parallel、Bubble Tea TUI 與自有資源清理。Workflow 與角色配置寫在程式碼內，不提供外部 workflow config、DSL 或動態 DAG。

## 目前 workflows

| Workflow | 功能與交付 |
|---|---|
| `smoke-echo` | 將單行 Prompt 當資料，經 Pi 產出 JSON contract；committed Decode 必須精確符合輸入。只有 contract，沒有 Markdown artifact |
| `code-review` | 固定 deep 靜態 PR review：Prepare、Code／Scale-Failure／Simplicity 平行審閱、獨立 Validation，交付結構化 contract 與繁中 Markdown report |
| `code-change` | 以已核可的 design doc 在 Controller 自有 worktree 內實作並本機 commit，由兩位審閱者做發 PR 前審閱（審閱者唯讀只靠 Step requirements，不是 Controller 的保證），再由同一實作者更正、push 一個 branch、開非 draft PR 並做 ticket 轉移；Controller 以唯讀 GET 核對 PR receipt，再以 code-review 流程對該 PR 在已核對的 head 做一次深度審閱，之後由 fresh 修正者 triage 深度審閱的 findings、修正並 push 到同一 branch，Controller 以同一套核對驗收其 push receipt，最後機械等待 PR 的 checks 與 commit statuses 進入終態並記錄一份唯讀的 PR 狀態快照。此版本到快照為止：沒有收斂與最後清理 |

`code-review` 只讀 pinned code／來源，不執行被審 repository 的 tests/build/scripts，不發 comments 或其他外部寫入。合法 `limited` report 可以是執行成功，但不代表 PR 全面通過；必要 reviewer 失敗不能 exit 0。模型、來源及失敗語義見 [CODE-REVIEW](docs/CODE-REVIEW.md)。

`code-change` 的 Prompt 為 `<owner/name> <design doc 路徑>`，程式在 `internal/workflows/codechange`，規劃與決策見 [CODE-CHANGE](docs/CODE-CHANGE.md)。Controller 以一次 gh GET 確認 repo 名稱與 push 權限，fetch default branch tip 建立 detached worktree（位於 run 目錄），由一個實作者 session 判斷 design doc 能否實作；可以則在 worktree 內依 Pi 正常載入的 git 流程 skill 本機 commit，Controller 只經 worktree registration 讀 HEAD 核對回報的 commit（等於 worktree HEAD，且不是 base 或其祖先）。核對通過後，Controller 在 run 目錄另建一個自有 bare repository，以 alternates 借用 worktree repository 的 objects，detached checkout 已核對的 commit 作為審閱用 checkout（不讀 Agent 可改的 worktree repository config，也不套用 tree 內 `.gitattributes` 的轉換），再平行派兩位 fresh 審閱者（基本 code review；design doc 對 code 的一致性），以同一個 findings schema 回報。審閱者不改檔、不 build／test 只是 Step requirements；checkout 的 objects 讀自 Agent 可寫的目錄且不驗證雜湊，所以內容也不是雜湊綁定的保證。兩位都必要，任一失敗即 run 失敗（不交付 Final，已接受的 findings 仍列在 outputs；兩位都失敗時以 basic 的失敗分類）。兩位的 session 都確認退出後，Controller 經 registration 重讀 worktree HEAD，必須仍是已核對的 commit（只偵測 HEAD 移動，不偵測其他改動），再移除審閱用 checkout。之後沿用實作者的 session 派更正 Step（Feedback 帶兩份 findings Refs，repair 派送使用不重做寫入的開頭句）：Agent 自行 triage、修正並本機 commit，push 承載本工作的一個 branch，對 default branch 開一個非 draft PR（只設 title、body、base、head），依 git 流程 skill 做 ticket 轉移；這些授權範圍只是 Step requirements，不是 sandbox。Go 只核對每一則 finding（以審閱角色加 ID 對應）恰有一筆處置（可 repair，不看內容）。Repair 迴圈接受最終 contract 之後，Controller 只核對一次、不 repair、不重試：回報的 head 等於經 registration 讀出的 worktree HEAD；`committed` 時不是實作 head 或其祖先，`no_change` 時等於實作 head；再以唯讀 GET 確認 PR 存在於目標 repo 且 URL 與 number 相符、open、未 merge、非 draft、base 是 input 記錄的 default branch、head 是目標 repo 的回報 branch，且回報的 head 等於 PR 當下 head 或為其祖先（不同時以一次 compare 判定）。`no_change` 的意思是回報的 head 等於實作 head，且仍是 PR 當下 head 或其祖先。不符或無法核對是具名的執行失敗（PR GET 的 404 也是無法核對，因為 gh 的 stderr 不保留，分不出不存在與其他錯誤）。核對通過後以 Attach 記錄 GitHub 上觀察到的 PR 身分與 head（`pull-request`），嚴格關閉實作者的 session，再經 registration 重讀 HEAD 必須仍是已核對的 head。之後 Controller 以 `review.Run` 在自己的 scope `root/deep-review`（名稱 `deep-review`）派 code-review 流程（Prepare、三位 reviewer、Validate；角色、模型與驗收見 [CODE-REVIEW](docs/CODE-REVIEW.md)），對象是 `pull-request` 記錄的 URL，預期 head 是記錄中來自 committed receipt 的 `reported_head`，不是 GitHub 當下的 `head_sha`。此時外層沒有 live session（實作者與兩位發 PR 前審閱者都已嚴格關閉），review 的 sessions 與 attempts 計入本 workflow 的額度（數字見 [VERIFICATION](docs/VERIFICATION.md) 第 6 節）。PR head 已較新（例如他人之後 push）時審閱仍釘在該 head，較新的 commits 列為缺少的來源，report 不能是 `complete`；該 head 已不是 PR head 的祖先（例如 force push）時審閱在開任何 session 之前失敗。Review 的 outputs 以 `deep-review-` 前綴併入。Report 為 `complete` 或 `limited` 時進入修正 Step（見下段）。Report 為 `incomplete`（必要 reviewer 失敗）是執行失敗：該 report 為 Final，並回傳 reviewer 的原始錯誤。深度審閱沒有產出 report 時（acquisition、Prepare、驗收、Validate 或其 session 關閉失敗），已 committed 的 `pull-request` 記錄為 Final 並回傳原始錯誤，不再 Attach、不再開 session：review 可能留下至多 3 個本 workflow 無法關閉的 live session，所以 run 立即結束，由 run 的 cleanup 關閉。取消、run deadline、fatal（例如 `StorageFailed`）與 run 範圍額度用盡時 engine 不解析 Final，這些結束沒有 Final。深度審閱的任何失敗都保留 PR、branch 與 worktree，不做外部物件記錄（深度審閱不寫外部系統）。深度審閱成功後，Controller 先經 registration 重讀 worktree HEAD，必須仍是 `pull-request` 記錄的 `reported_head`（深度審閱的 Agents 不碰 worktree 只靠 requirements），否則在開 session 之前以具名錯誤失敗，Final 為 `pull-request`。之後在 fresh session（同實作者的模型與 `PWC_PI_CWD`，不沿用已關閉的實作者 handle）派修正 Step，Inputs 為 input、implement、`pull-request` 與 `deep-review-report`：Agent 自行判斷 report 的每一則 finding 修或不修並留理由，自行處理 report 的其他項目，依原則清理本次變更新增或修改的 source code 註解（Controller 不判斷註解品質），只 push 到記錄中的那一個 branch；branch 上已有的 commit（含本工作已 push 且已審的）只往上加，不改寫或丟棄；不開或改 PR、不留 comment、不做 ticket 轉移。這些都是 Step requirements，不是 sandbox。Go 只核對 report 每個 finding ID 恰有一筆處置（可 repair）。Push receipt 以更正 Step 的同一套核對驗收一次、不 repair、不重試，差異只有：上次已核對的 head 是記錄的 `reported_head`；PR 的身分（URL、number、branch）取自記錄，contract 只回報 head；`committed` 時另要求 `reported_head` 仍是新 head 的祖先，所以改寫已審的 commit 再 force push，即使新 head 就是 PR head 也被拒。Controller 不核對他人推的 commit 是否被改寫或丟棄：只要新 head 仍以 `reported_head` 為祖先，receipt 照樣通過。逐項核對見 [VERIFICATION](docs/VERIFICATION.md) 第 6 節，修正 Step 的殘餘風險見 [CODE-CHANGE](docs/CODE-CHANGE.md) 第 10 節。通過後記 `fix-verified`，嚴格關閉修正者的 session，再經 registration 重讀 HEAD 必須仍是 push 的 head；成功（Succeeded）時 Final 是修正 Step 的 contract（`fix`，最後快照之後也是），只表示 findings 都有處置且 push receipt 通過核對，不代表 checks 通過或修正正確。修正 Step 不 Attach 另一份 PR 記錄。修正者的 session 關閉且 HEAD 核對之後，Controller 對 `pull-request` 記錄的 PR 做一次機械等待：每個輪詢間隔以唯讀 GET 讀 PR 的 head，再分頁讀該 head 的 check runs 與 combined commit status；每個 check run 的 `status` 為 `completed`、每個 commit status 的 `state` 為 `success`、`failure` 或 `error`（其他值與未知值都不是終態，`conclusion` 從不讀），且靜置窗口內每次輪詢都成立、head 不變，才算靜置，等待記錄靜置所在的 head。輪詢讀取失敗只重設靜置並記下最後錯誤；整體等待有上限，逾時只記在快照，不是錯誤。之後在讀取期限內重讀 PR、其 head 的 check runs 與 commit statuses，並以 `runtime.GitHubGraphQL` 分頁讀 review threads、只留未解者，Attach 為 `final-snapshot`（`code-change.pr-state.v1`：head、各 check run 的名稱、狀態、conclusion、app 與連結，各 status 的 context、state 與連結，未解 thread 的 ID、位置與連結，以及等待結果與觀察時間；沒有內文、log 或判斷）。間隔、靜置窗口、等待上限與讀取期限的初值見 [CODE-CHANGE](docs/CODE-CHANGE.md) 第 11 節，待 live 修正。PR 的 number 必須等於記錄的 number；各頁的 total count 必須一致且等於合併後的數量，check run 的 id、status 的 context 與 thread 的 ID 不得重複，不可為 null 的欄位不得缺少；任一讀取失敗或回應不合這些條件即以 `errStateRead` 失敗（Failed、exit 1，錯誤訊息附等待的輪詢數、失敗數與最後錯誤，Final 仍為 `fix`，不記錄部分快照，worktree 保留），快照超過 Attach 的 1 MiB 上限也是失敗，不截斷。不核對的寫入範圍：其他 ref 與 default branch 的 push、PR 的 label、reviewer、assignee 與 comment（都可能由其他人或 repo 自動化寫入，無法歸因給 Agent），以及 ticket 轉移（Controller 沒有 ticket 系統的 client，只記在 Agent 的報告）。驗證項目、findings、處置與其狀態在各 contract 內，Controller 不判斷其意義。更正 Step 或修正 Step 派送之後到其收尾的任何失敗（repair 用盡、provider 失敗、逾時、receipt 不符或無法核對，以及核對之後的 `pull-request` Attach、Decision、session 關閉未確認、關閉後 HEAD 移動）時，只要 run 仍可動作，Controller 關閉該 Step 的 session、經 registration 讀出目前 branch，以唯讀 GET 查該 branch 與其 PR，並查最後一份通過 schema 的該 Step contract（被拒者亦同，列在 Outputs 的 `publish` 或 `fix`）所回報的 branch 與 PR number（修正 Step 只回報 head，branch 與 PR 用記錄的），Attach 為 `external-objects`（`step` 標明 `publish` 或 `fix`）並選為 Final，同時回傳原始錯誤；只記錄不判斷，不關 PR、不刪 branch、不撤銷 ticket 轉移。Agent 以 refspec 推到與 worktree branch 不同名的 branch 時，只有回報值涵蓋它。取消、run deadline、fatal 與 run 範圍額度用盡時無法做這份記錄。回報 gaps 時不做審閱，worktree HEAD 必須仍等於 base：相等則交付同一 contract 為 Final，並以具名錯誤結束（Failed、exit 1）；不相等是 receipt 錯誤，不交付 Final。Worktree 只在整個流程（含修正 Step 與最後快照）最終成功返回時才標為移除，其他結束一律保留；深度審閱與修正期間 worktree 仍存在。移除前只保證：session 嚴格關閉後重讀的 HEAD 等於已核對的 head，而它在核對時是 PR head 或其祖先，即已在 PR branch 上；未 commit 與 untracked 的檔案以及該 worktree 的 reflog 會隨 checkout 與 registration 移除，本機 branch 與 objects 留在 bare repository；脫離 process group 的 Agent 子孫不在保證內。Run 的 cleanup 只在所有 session 的 process 確認退出後，依註冊的反向順序移除：先移除深度審閱的 checkout（不看 run 成敗），最後才是 worktree（只在設定移除旗標時）；任一 process 退出未確認，或等待超過 cleanup 期限時，兩者都保留。它們與 session 關閉共用同一個 2 分鐘期限，它是 deadline 而不是移除的上限（`RemoveAll` 不可中斷）：期限在移除深度審閱 checkout 時到達，該 checkout 可能已部分刪除而 worktree 保持完整；在移除 worktree 時到達，worktree 可能已部分刪除。數值待 live 修正。旗標設定之後 engine 仍可能因取消或 run deadline 判為 Cancelled／TimedOut 並移除 worktree，run 目錄的報告保留。目前沒有 live 驗收。

`jira-triage` 以固定階段調查一張 Jira 票（intake／facts、身分確認、調查 rounds、稽核、steward、對抗驗證、報告），程式在 `internal/workflows/triage`，設計與現況見 [JIRA-TRIAGE](docs/JIRA-TRIAGE.md)。執行需 `PWC_TRIAGE_SKILLS_DIR` 指向 repo 外的私有 skill 目錄，以及 `python3`（報告 renderer）。現況是 alpha：已以多張真實票 live 跑出報告，尚未 live 驗證的範圍見 JIRA-TRIAGE；成功產出報告不代表調查品質。

## CI 與 coverage

[GitHub Actions CI](.github/workflows/ci.yml) 在 `main` push、pull request 與手動觸發時執行，使用 macOS arm64 runner 與 `go.mod` 指定的 Go 版本：

- **Lint**：gofmt、go vet、固定版本 golangci-lint。
- **Tests and coverage**：一般 Go tests、Python verifier tests，以及 race＋atomic statement coverage。
- **Build and CLI smoke**：module verification、package／CGO-disabled executable build，以及 checkout 外的 CLI smoke。

Coverage 百分比放在成功測試 run 的 **Summary**；`go-coverage-macos-arm64` artifact 包含 `coverage.out`、逐函式統計與可下載開啟的 HTML report，保留 14 天。不使用 Codecov、不把產出 commit 回 repo，也不將 CI status badge 當成 coverage 百分比。目前只呈現量測值，不設定最低百分比門檻。

CI 不提供模型 credentials、不啟動 Pi／hub；bundled Pi 與 live-provider／shared-hub opt-in tests 明確排除，skip 不算通過。Coverage 僅量測 Go statements，不代表 branch coverage、所有 subprocess 的完整 coverage 或 embedded Python／TypeScript coverage。Bundled 環境尚未建立乾淨 runner 的固定版本相容性驗證，仍依 [VERIFICATION](docs/VERIFICATION.md) 分開執行。CI badge 只代表這份 workflow 的檢查結果，不代表真 provider 業務 E2E 或其他平台相容性。

## Build 與執行前提

- Go **1.25.0**，依賴以 [go.mod](go.mod)／[go.sum](go.sum) 為準；目標為 macOS arm64。其他平台的 exclusive publication 目前 fail-closed，不宣稱跨平台相容。
- 執行時需 PATH 中的 Pi **0.84.3**、其 Node.js runtime、已部署的 WebUI extension／hub 與可用 discovery。Controller 不打包 Pi、Node 或 credentials。
- 須自行備妥固定 provider/model 的認證與使用權。`smoke-echo` 使用 `fireworks/accounts/fireworks/models/gpt-oss-120b`、thinking `low`。有配置不等於認證或推論成功，模型輸出仍可能不符合 contract。
- `code-review` 額外需要 Python 3、git、已認證且有 repository 讀取權的 gh，以及其[工具 skills 前提](docs/CODE-REVIEW.md)。自有 skills/schema 已 embed，無須在 source repo cwd 執行；外部工具 skills 不包含在 binary。
- `code-change` 額外需要支援 `--no-lazy-fetch` 的 git（2.45 以上；Controller 讀取 receipt 時使用，於 repo GET 之後、建立 worktree 之前檢查，不支援時 run 以具名錯誤失敗）、已認證且對目標 repo 有 push 權限的 gh（Controller 的唯讀 GET 使用；Agent 的 push、開 PR 與 ticket 轉移使用 Agent 自己既有的認證）、Agent 自己可用的 git identity，以及 `PWC_PI_CWD` 設為既有目錄的絕對路徑（實作者、發 PR 前審閱者與修正者的 CWD；深度審閱沿用 review 自己的工作目錄；未設定、相對路徑或不是目錄時 run 在任何副作用前失敗）。
- 不停用 Node TLS 驗證，不自動提升 project trust，不更動既有全域 skills/agents/settings。

```sh
mkdir -p bin
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 \
  go build -trimpath -buildvcs=false -o bin/pi-workflow-controller ./cmd/pi-workflow-controller
```

交付 build 使用 `-trimpath -buildvcs=false`，避免嵌入開發機 source 路徑與本機 VCS stamps；這不代替 binary 的獨立安全掃描。這是 Controller executable 的 CGO-disabled build，不是 Linux ELF 全靜態或無 OS runtime 的承諾。分組測試方法另見 [VERIFICATION](docs/VERIFICATION.md)，此處不提供歷史測試通過宣稱。

## CLI 與安全啟動

```text
pi-workflow-controller list
pi-workflow-controller run <workflow> "一行 Prompt"
pi-workflow-controller run smoke-echo "範例文字"
pi-workflow-controller run code-review "https://github.com/owner/repo/pull/123"
PWC_PI_CWD=/absolute/path pi-workflow-controller run code-change "owner/repo /absolute/path/design.md"
```

不從 stdin 讀 Prompt，不接受 task naming、input-file 或 config flags。未知 workflow、空／多行／非法 UTF-8／超限 Prompt 在建立 task 或啟動 Pi 前拒絕。每個 Controller 執行一個 run，資料路徑由 Controller 自動產生並顯示：`~/WIP/<task-id>/runs/<run-id>/`。

`jira-triage` 另需 `PWC_TRIAGE_SKILLS_DIR` 指向私有 skill 目錄（CLI 啟動時讀取並交給 workflow 定義；未設定時 run 在任何 Step 之前失敗）。可用單次環境設定 `PWC_PI_CWD=/absolute/path/to/service pi-workflow-controller run ...` 指定 Pi 預設工作目錄。CLI 在 `run` 讀此值並傳入 `engine.Options.PiDefaultCWD`，另於啟動時把原值交給 `workflows.Definitions`，`code-change` 據以檢查並明設為實作者、發 PR 前審閱者與修正者的 `Role.CWD`；不新增 flags 或全域設定。優先序為 workflow 明設的 `Role.CWD` → 預設目錄 → 啟動 Controller 時的 `Input.LaunchCWD`。相對值在建立 Run 時依 LaunchCWD 固定；unset／空字串停用，不 trim 空白、不展開 `~`。僅選用的 default 含 NUL 時提前拒絕，explicit Role.CWD／fallback LaunchCWD 保留原 runtime 驗證與會計；不存在或非目錄在實際 spawn 時失敗，未使用的壞 default 不阻擋 explicit role，`list` 不使用此設定。

Review 保留 explicit `review-work` 與 pinned `task.Worktree`，smoke 使用一般 fallback。這是 cwd／任務接線，不是阻止 Agent 寫入來源的 sandbox；也不改變 Input、discovery anchor 或 session ownership。

**共享 discovery 安全前提：**啟動 persisted Pi 可能觸發 deployed WebUI 的 recovery。每次啟動或 resume 前，唯讀核對 deployed recovery 判準、discovery 的父 `pid`（不只 `piPid`）及 `.recovering`。可能 delete/resume 他人 stale session、父 process 不可確認或有異常 claim 時停止，不代清理。無 truthy pid 的 hub-state 類 JSON 僅按實際 recovery 規則略過。Controller 的共用 runtime 在每次 version probe 成功後、persisted child spawn 前執行程式化 preflight，涵蓋所有 workflow 的 Pi 啟動；它不能代替部署版本核對，也不涵蓋 Controller 外的手動 resume。**獨立 task 目錄不是 discovery 隔離，preflight 不是鎖。**請在受控時段執行。

Runtime 建構時選定 `BridgeDir`（明示 option、`PI_BRIDGE_DIR`、home 預設依序），相對值依 Controller 當時 cwd 固定為絕對路徑；preflight、child 與 cleanup 共用該位置，不要求 Pi 與 Controller cwd 相同，也不自動建立 discovery 目錄。預檢拒絕為 `BridgeUnavailable`／`preflight`，發生在 Run 建立後；保留已消耗的 session 額度，但不建立 persistent session 或 attempt。

Controller 是執行期間唯一派工來源，使用者只觀看；不修改或禁用 hub 既有功能，也不承諾人工 prompt／steer／Stop／model/session 切換的相容性。

stdin／stdout 同為 TTY 時使用 TUI，否則自動純文字輸出。TUI 可捲動，`q`／Ctrl+C 取消後等待 cleanup／Report 再還原終端，不 detach。

| Exit | 意義 |
|---|---|
| `0` | Workflow 成功，必要持久化、清理及輸出均成功 |
| `1` | Workflow failure、timeout，或非取消結果的 cleanup／finalization／輸出失敗 |
| `2` | Run 建立前的用法或 preflight 錯誤 |
| `130` | 明確取消／SIGINT |
| `143` | SIGTERM |

取消保留其 exit precedence，仍必須閱讀所有 warnings。

## 收取結果

不必保留 Pi process 或 resume 才能收結果。最終 Report 顯示：

- Result index 路徑 `result.json`、outcome、所有輸出 Ref 與 cleanup/finalization warnings。
- Workflow 明確選定、在 cleanup 前驗證的 final contract，以及可選 readable artifact。
- Producer attempt 對應的 scope/step、handle、role、session ID/file 與 process exit 確認狀態。

`Result.Final` 使用 `FinalSelection{Output: <output-key>, FileID: <artifact-id>}`；FileID 可省略。Engine 經 committed resolver 建立 `FinalDelivery`，保存於 `result.json.final` 與 `Report.Final`，不是從 map 順序或最後關閉的 session 猜測。`result.json.outputs` 保留輸出 Ref；`final` 含選定 Ref、`artifact_path`（若有）、`handle_id`、`scope`、`step`，session identity 可由 handle 查 `run.json.sessions`。

沒有合法 final 時明示 unavailable，不拿 reviewer 或未提交 candidate 代替。普通失敗仍可交付合法 incomplete report，但保留失敗 outcome／非零 exit；root cancellation/deadline/fatal 不強迫產出。這些定位資訊與檔案存在都不是新 publication 授權或持久化成功證明。看到 `EMERGENCY: state_persisted=false` 或任何收尾錯誤，不能以 result 路徑宣稱成功。完整語義見 [FINAL-DELIVERY](docs/FINAL-DELIVERY.md)。

## 結束後繼續討論（resume）

Controller 清理自有 Pi、discovery 與 review checkout，保留 contracts、報告、證據、private Git objects 及 Pi session JSONL。延續的是 Pi 對話，不是 Controller workflow。

1. 確認 Controller 已結束，Report 中該 producer 的 `ProcessExited` 與 `WaitCompleted` 均已確認；unconfirmed process 或 cleanup warning 必須先處理，不同時開同一 session。
2. 重新執行上節共享 discovery 唯讀安全預檢。Resume 同樣會啟動 persisted Pi，並非豁免入口。
3. 使用 Report 顯示的 session file，或由 `result.json.final.handle_id` 對應 `run.json.sessions`，不要搜尋「最新 session」。確認檔案存在後，在可信 cwd 執行：

```sh
SESSION="/path/to/owned/session.jsonl"
cd /path/to/trusted/workdir
NODE_TLS_REJECT_UNAUTHORIZED=1 pi --session "$SESSION"
```

`--session` 會繼續寫原 JSONL；需保留原始 history 不變時，改用 `pi --fork "$SESSION"`。Session file 路徑來自 runtime metadata，不是 Controller 重新檢查 history 存在或 durability 的保證。

- 恢復同一 history、新 Pi process，不保證工具狀態、角色 system prompt 或資源設定完整還原；不重新派工、commit Ref 或改寫原 outcome。
- Review checkout 已清理。需重新查 code 時，另行準備相同 pinned revision 的自有 checkout 並告知新路徑，不使用 branch 最新值或改寫 published artifacts。
- Pi `/reload` 只重新載入資源；Hub Reload 需要在線 bridge，不能恢復已關閉節點。不要在 Controller 活動時 reload/resume 節點。

## 文件導航

| 文件 | 用途 |
|---|---|
| [PRD](PRD.md) | 通用產品需求與範圍 |
| [DESIGN](DESIGN.md) | Runtime／contract／engine／terminal 不變量、API、limits 與已知限制 |
| [IMPLEMENTATION](IMPLEMENTATION.md) | 開發維護路線、責任分工與 source/tests 入口 |
| [ADDING-A-WORKFLOW](docs/ADDING-A-WORKFLOW.md) | Workflow authoring、registry／skills／交付與清理 |
| [CODE-REVIEW](docs/CODE-REVIEW.md) | 固定靜態 review 的業務設計 |
| [JIRA-TRIAGE](docs/JIRA-TRIAGE.md) | `jira-triage` 的階段、驗收、alpha 現況與限制 |
| [FINAL-DELIVERY](docs/FINAL-DELIVERY.md) | FinalSelection／FinalDelivery／result.json.final |
| [VERIFICATION](docs/VERIFICATION.md) | 分組測試 gates、共享環境安全與發布 scan 方法 |

## Privacy 與發布 checklist

- [ ] Repository 只留 source、可重現匿名 tests/fixtures、依賴鎖檔及通用文件。Binary、coverage、profiles、local env、執行結果與報告留 repo 外。
- [ ] 不提交真實測試 PR/ticket、受審組織的內部名稱／domains、私有路徑、私人／公司 email、task/session/run IDs、機器 PID、實測日期、舊執行紀錄的 commit SHA、RPC traces、history 或 source manifests。匿名 fixtures、公開 dependency/model 配置及經確認的公開 Git 作者身分不屬此類。
- [ ] Prompt、contracts、evidence、session history 可能包含敏感資料；檔案權限不是匿名化。分享前檢查內容、metadata、附件與 Git history，不把原始資料複製進 repo。
- [ ] Credentials 不放 Prompt／argv／journal／文件；不因忽略規則存在就假定 index 或既有 history 安全。
- [ ] 按 [VERIFICATION 的發布策略](docs/VERIFICATION.md) 檢查工作樹、待發布檔案及可達歷史；修正本地 Markdown links。真實採證及版本核對只記在受限、repo 外的位置，不留私有備份連結。

## 已知限制

極端 numeric validator 的可表示性／資源／非協作取消限制，以及完整 snapshot 重寫造成的 I/O 放大，見 [DESIGN](DESIGN.md)。Journal quota 不限制所有 snapshot 流量；沒有高吞吐量認證。永久堵塞 stdout 或 filesystem syscall 不保證固定 wall-clock 返回／terminal restore，但取消登記與 engine cleanup 不得依賴 renderer 恢復。

不提供 supervisor、crash resume、sandbox、任意 detached 子孫清理、exactly-once 或外部副作用 rollback。Controller abrupt death 可能殘留 Pi；session 隔離不等於工具、檔案或 credentials 隔離。
