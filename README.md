# Pi Workflow Controller

[![CI](https://github.com/wirelessr/pi-workflow-controller/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/wirelessr/pi-workflow-controller/actions/workflows/ci.yml?query=branch%3Amain)

以 Go control flow 編排多個獨立、持久化 Pi session 的本地 Controller，提供 JSON contract 交接、檢核、retry／parallel、Bubble Tea TUI 與自有資源清理。Workflow 與角色配置寫在程式碼內，不提供外部 workflow config、DSL 或動態 DAG。

## 目前 workflows

| Workflow | 功能與交付 |
|---|---|
| `smoke-echo` | 將單行 Prompt 當資料，經 Pi 產出 JSON contract；committed Decode 必須精確符合輸入。只有 contract，沒有 Markdown artifact |
| `code-review` | 固定 deep 靜態 PR review：Prepare、Code／Scale-Failure／Simplicity 平行審閱、獨立 Validation，交付結構化 contract 與繁中 Markdown report |

`code-review` 只讀 pinned code／來源，不執行被審 repository 的 tests/build/scripts，不發 comments 或其他外部寫入。合法 `limited` report 可以是執行成功，但不代表 PR 全面通過；必要 reviewer 失敗不能 exit 0。模型、來源及失敗語義見 [CODE-REVIEW](docs/CODE-REVIEW.md)。

`jira-triage` 以固定階段調查一張 Jira 票（intake／facts、身分確認、調查 rounds、稽核、steward、對抗驗證、報告），程式在 `internal/workflows/triage`，設計與現況見 [JIRA-TRIAGE](docs/JIRA-TRIAGE.md)。執行需 `PWC_TRIAGE_SKILLS_DIR` 指向 repo 外的私有 skill 目錄，以及 `python3`（報告 renderer）。現況是 alpha：一張真實票的 live run 已成功，尚未 live 驗證的範圍見 JIRA-TRIAGE，不代表調查品質。

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
```

不從 stdin 讀 Prompt，不接受 task naming、input-file 或 config flags。未知 workflow、空／多行／非法 UTF-8／超限 Prompt 在建立 task 或啟動 Pi 前拒絕。每個 Controller 執行一個 run，資料路徑由 Controller 自動產生並顯示：`~/WIP/<task-id>/runs/<run-id>/`。

`jira-triage` 另需 `PWC_TRIAGE_SKILLS_DIR` 指向私有 skill 目錄（CLI 啟動時讀取並交給 workflow 定義；未設定時 run 在任何 Step 之前失敗）。可用單次環境設定 `PWC_PI_CWD=/absolute/path/to/service pi-workflow-controller run ...` 指定 Pi 預設工作目錄。CLI 只在 `run` 讀此值並傳入 `engine.Options.PiDefaultCWD`；不新增 flags 或全域設定。優先序為 workflow 明設的 `Role.CWD` → 預設目錄 → 啟動 Controller 時的 `Input.LaunchCWD`。相對值在建立 Run 時依 LaunchCWD 固定；unset／空字串停用，不 trim 空白、不展開 `~`。僅選用的 default 含 NUL 時提前拒絕，explicit Role.CWD／fallback LaunchCWD 保留原 runtime 驗證與會計；不存在或非目錄在實際 spawn 時失敗，未使用的壞 default 不阻擋 explicit role，`list` 不使用此設定。

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
