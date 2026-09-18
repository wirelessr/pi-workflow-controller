# 新增 workflow：接手 agent 指南

先讀 repo 根目錄的 [專案憲法](../AGENTS.md)，以及 [Workflow 重用與 refactor 計畫](WORKFLOW-REUSE.md)。後者的能力登記、R/M 相依與每單元 reuse map 是必要開工 gate，不是可略過的參考；以下流程與個別 workflow 設計不得默默改變其責任邊界。

本文件是新增 predefined workflow 的工作入口，不是新 DSL、全域 skill 或安裝器。Workflow 使用 Go control flow；新 workflow 的業務選擇由使用者確認，不從當前 agent、smoke-echo 或 code-review 默默繼承。

## 1. 接手基線與閱讀順序

先在 repo 核對 `git status --short`、`git rev-parse HEAD`，確認實際基線；不要覆蓋不明來源的未提交變更。

1. [README](../README.md)：目前功能、CLI、執行前提、收取結果與 resume。
2. [PRD](../PRD.md)：已確認的產品範圍，特別是 Controller 唯一派工、使用者執行中只觀看。
3. [DESIGN](../DESIGN.md) 與 [IMPLEMENTATION](../IMPLEMENTATION.md)：框架語義與維護責任；[VERIFICATION](VERIFICATION.md) 定義分組 gates，不記實際執行結果。API 以 source 宣告及立即 callers 為準。
4. [FINAL-DELIVERY](FINAL-DELIVERY.md)：`FinalSelection`、產出／producer session 定位與失敗收尾。
5. [CODE-REVIEW](CODE-REVIEW.md)：多階段、embedded skills、來源／證據驗收的既有業務實例。
6. 下表的實際 API、立即 callers、tests/fixtures。若目前程式與需求衝突，指出具體差異再確認；不要自行選較寬鬆版本。

| 程式入口 | 要看什麼 |
|---|---|
| `internal/workflows/registry.go` | `Definitions()`、`Resources()`、`Schemas()`，以及最小產品 `smokeEcho` |
| `internal/engine/api.go`、`policy.go` | Definition、RoleSpec、StepSpec、Result、FinalSelection、timeouts／限額 |
| `internal/engine/scope.go`、`session.go` | Child／Parallel／Retry／Decision、session lease、Close／slot |
| `internal/engine/resolve.go`、`step.go` | committed Ref、Decode／ReadContract、Stage/Confirm/Publish 接線 |
| `internal/engine/run.go`、`resource.go` | finalization、AddCleanup、取消與自有資源生命週期 |
| `internal/contract/contract.go`、`schema.go` | request/envelope/meta/data/files、離線 schema registry |
| `internal/workflows/review/{workflow,resources,check}.go` | 編排、embed／展開、業務語意驗收範例 |
| `internal/tui/format.go`、`cmd/pi-workflow-controller/main.go` | 最終結果的 consumers，通常不必修改 |

回歸原因以現行不變量、匿名測試及限制說明核對，不依賴私有歷史報告或追蹤代碼。Code review 的三個 reviewer、唯讀原則、模型、120 分鐘期限、PR URL、pinned checkout 都是它的業務選擇，**不是 framework 對所有 workflow 的限制**。

## 2. 實作前先定義

在 `docs/<WORKFLOW>.md` 記錄下面的決策。先完成重用計畫要求的 API/callers 對照、機制與政策差異、必要 consumer 遷移及前置單元核對；找不到共用入口不代表可以另複製一套。一般工程細節自行處理；規格歧義、權限／安全風險或必須擴大範圍時先確認。

- **輸入／產出：** CLI 仍是 `run <workflow> "單行 Prompt"`。業務如何解析這個字串、最後交付什麼，不能靠模型自己猜。框架的單行／UTF-8／大小 preflight 不等於業務 URL/ticket 等格式驗證。
- **角色／session：** 每個角色職責、哪些沿用同一 handle、哪些需要新 Pi、必要分支、join 模式。不要讓 Pi 自行 spawn、選下一節點或控制 Controller。
- **模型：** 查證完整 provider/model ID、thinking 支援、認證與 minimal inference；configured auth 不等於 live 成功，更不等於 tool/schema 品質已驗證。模型固定於 Go RoleSpec，不繼承當前 agent 或照搬範例。
- **Contract：** 先從下游與使用者需求設計 schema、穩定 IDs、來源／版本關係、證據引用與語意驗收；每個 Pi Step 都有輸出 Spec，最終節點也有可驗收 contract。
- **成功／失敗：** 分開定義執行成功、業務結果、資料不足、必要角色失敗。業務 reject／發現缺陷不必然是 execution failure，缺失結果也不能偽裝成功。
- **期限／重試／資源：** 正值 RunPolicy、Step timeout、live／total sessions／attempts，重試誰、是否可沿用 context、哪些資源需要清理／保留。只有明確設 `DisableRunTimeout=true` 才停用 Controller 自有 run deadline，RunTimeout 等 duration 仍須正值；parent cancellation／deadline、單次期限與資源限額不變。
- **工具／副作用：** 明確界定可讀／可寫的外部系統及授權範圍、敏感資料、不可回滾或非冪等動作；不要以重試默認安全。必要權限在啟動前確認，不設計依賴中途人工 dialog 的流程。
- **真實驗收：** 實際輸入／環境／provider 與付費範圍。需使用者指定目標時先取得，不自行挑真 PR/ticket 或寫入目標。

## 3. 檔案與 registry 接線

複雜流程優先放 `internal/workflows/<name>/`，只新增實際需要的檔案，例如 `workflow.go`、業務型別／驗收、`schemas/`、`skills/`、`resources.go`。極小流程可直接參考 smoke-echo；不要為統一目錄形狀加入空 scaffolding。

接線順序：

1. 建立業務 data 型別、schema resources 及業務驗收函式。
2. 建立 `engine.Definition`：唯一 Name、Version、Description、由 `DefaultRunPolicy()` 起調整的 Policy、Execute callback。不要使用 zero-value policy。
3. 在 `internal/workflows/registry.go` 分別追加 definition、schema resources、schema definitions；保留已有 workflows，不只改其中一份 registry。
4. 子 workflow package 可依賴 engine/contract/runtime 的型別；不要反向 import 父 `workflows` package 造成 cycle，也不把業務 schema 放進 engine。
5. 擴充 `internal/workflows/registry_test.go` 的目前清單／schema 與相關消費端驗證。

通常不需修改 CLI/TUI、runtime 或 contract engine。若真的缺共用 API，先查 exports／callers／既有 helpers，提出必要的最小擴充；不要建立泛用 command executor、supervisor、外部 config/flags 或 DAG/DSL。

## 4. Contract 交接與內容驗收

- Workflow schema 驗 **data**；框架 envelope 的 meta/files 由既有 schema 驗證。Schema ID／URI 必須唯一、使用 Draft 2020-12；`$ref` 只能指已註冊的離線 resources，不依賴網路載入。
- `request.output` 提供實際 output schema、envelope、resource paths/digests。Skills 要求 Pi 讀這些檔案，不手抄另一套輸出格式。
- File references 使用 `files[].id`，路徑限本 attempt 的 evidence/artifacts；下游相對路徑以該 published contract 所在目錄解析。不要將任意 host path 填成可發布產物。
- `scope.Step()` 成功才取得可消費 Ref。以 `engine.Decode[T]` 讀 data，需 files 映射時用 `engine.ReadContract`；`Step.Inputs`／Feedback refs／Decision／Retry Result／final Result 都使用 committed resolver。
- **Publish rename 不等於 commit。** Journal append+Sync 等必要提交成功後才授權 Ref；禁止掃最新 candidate/published、僅憑 hash 或檔案存在建立下游輸入。
- Schema 正確不代表內容正確。驗收來源／版本／角色／輸入 Ref 是否對應本次工作，以及必要項目是否完整；不能以 PASS 文字、空 findings 或正常 assistant stop 替代。
- 下游收到的新 handle 不會繼承上游對話。所需資料必須透過 request、已發布 Ref 與附檔明確傳遞，不能依賴另一個 Pi 記憶體。

## 5. Skills 與資源交付

需要專用 skills 時，把原始檔放新 workflow package 的資源目錄，以 `go:embed` 打包；執行時展開至本 run 擁有的目錄，提供對應 Pi **絕對 SKILL.md 路徑**並要求全文讀完再執行。出現第二個實際 consumer 時，依重用計畫 R5 從 `review/resources.go` 收斂共用 extraction 機械部分並遷移原 consumer；不直接呼叫 review 專用 extractor 當通用安裝器，也不複製後改名另養一套。這不授權另造 acquisition tools 或共通安全 instructions。

- 動手前讀目標 Pi 已安裝版本的 skills／settings 文件與必要 resource-loader source。普通 `repo/skills/` 不保證被 Pi 自動 discovery。
- 保留 references/scripts/assets 的相對結構；引用以 SKILL.md 所在目錄解析，不以 code checkout cwd 解析。展開應 exclusive、不覆蓋其他 run 或全域內容。
- 不修改、覆蓋、搬移舊 skills/agents/settings，也不為重用邏輯回頭重構它們。新專用規則與舊規則不自動同步。
- 專用 skill 只執行本角色，不內嵌舊 workflow 的派工／下一階段指令。可按需唯讀載入既有工具 skills，但副作用權限仍以這次業務 scope 為準。
- 對外部文件、PR body、comments、code 等標記資料邊界，不讓其覆蓋角色規則。決定安全 cwd／project trust，不自動提升信任；新 session 隔離對話，不等於 sandbox。
- Binary 不打包 Pi/Node/credentials 或所有外部工具 skills。README 必須列實際前提，如 Python renderer、gh/Jira 認證等。

## 6. 編排、錯誤及資源

| API／情境 | 正確用法 |
|---|---|
| `OpenSession` | 每次新 process／session；provider/model/thinking 必須明確，CWD 使用可信的絕對路徑。需新 context 就建新 handle |
| `Step` | 同 handle 不可同時執行兩個 Step；同 scope execution 不重複 key。Timeout=0 沿用 policy，等待／provider retry／驗證都計時 |
| `Parallel(FailFast)` | 失敗取消 siblings 但仍 join，保留根因及 collateral cancellation；不把 sibling Cancelled 當原始問題 |
| `Parallel(CollectAll)` | group err==nil 不代表全部成功，逐一處理每個 BranchResult.Err；必要分支不可略過 |
| `Retry` | 首次之外的額度，Again=true 需有效 Feedback；callback 明確把 Feedback 放入下次 Step。Provider auto retry 不是這個 budget |
| `Decision` | 業務驗收／轉移留下理由與已 committed refs，不讓 TUI 或模型自行決定下一階段 |
| `CloseSession`／`CloseSessionReport` | 可在活動 workflow 中提早關閉，共用同一次 close 結果與獨立 cleanup budget；report 回傳 defensive copy，未完成會保留 Unconfirmed 與錯誤。不要用 canceled context 的 defer 呼叫取代 engine cleanup |
| `SessionContextUsage` | 僅在 idle owned handle 按需取得當前 context estimate、Identity、SampledAt／Seq／ActivityEpoch；nil tokens／percent／window 表示 unknown，不是零。與 Step／Close 互斥，不增加 health polling，也不是 provider admission 保證 |
| `AddCleanup` | 在把自有資源暴露給 Pi 前註冊；若 acquisition 部分失敗或註冊失敗，caller 仍負責 bounded cleanup。成功註冊後等待 workflow join／確認 Pi exit 才清理 |

Timeout／context handoff 由 workflow 顯式編排：保留上一份 committed Ref，驗收 `CloseSessionReport` 的 identity、WaitCompleted／ProcessExited、Unconfirmed 與 cleanup errors 後，才 OpenSession fresh handle 並以 Step.Inputs 交接。新 session 不重置 run session／attempt 計數；engine 不自動 retry，不把未知 ProviderFailed 當作 overflow。本機 Wait 不證明遠端 async job 已停止，需另做 workflow-specific 安全判定。Step deadline 是取消觸發，不保證固定時刻返回；最後 durable commit 窗口沿用既有仲裁，不在已提交成功後追加相反終態。

不要吞掉 StorageFailed、JournalFailed、run-level LimitExceeded 後繼續成功，也不要把不明 transport completion 自動重送。Workflow callback／工具 subprocess 必須遵守 context 並保有 ownership／Wait／cleanup，不建立 detached Go 工作或掃殺全機 process。

只清理本次擁有的資源，保留 contracts、報告、證據、Pi history。未確認 exit 不釋放 live slot或刪掉仍在使用的 workspace。Outcome 鎖定後 cleanup 不等 journal I/O；既有 root-fatal、dispatch acceptance、StatePersisted=false、所有收尾 errors、exit precedence、terminal writer/input/join/restore 不得回退。

保留既有非保證範圍：[DESIGN](../DESIGN.md) 的極端 numeric validator 可表示性／boolean inversion／資源放大及非協作取消、完整 snapshot 重寫 I/O、永久堵塞 stdout/filesystem syscall、任意 detached 子孫、abrupt death、無 supervisor/crash resume/sandbox/exactly-once/外部副作用 rollback。

## 7. 最終交付：不要忘記 FinalSelection

最後一個成功 Step 的 contract 仍需業務驗收，再明確放進 `Result.Outputs`，用 `Result.Final` 選擇對應 key。若有可讀附檔，`FinalSelection.FileID` 填驗收過的 artifact **ID**，不是檔案路徑；JSON-only workflow 可省略 FileID。

`FinalDelivery` 由 engine 從 committed Ref／producer attempt 產生，session 身分由 runtime metadata 提供，不讓 Pi 填 session ID。收尾會顯示產出與 producer session，`result.json.final` 留定位資訊；沒有合法 final 就明示 unavailable，不猜最後關閉的節點。

資料不足或業務失敗是否仍交付報告，需在業務設計定義；合法 incomplete report 可以搭配非零 execution outcome。取消／fatal 路徑不為了顯示漂亮而強迫產出。`result.json`、FinalDelivery 或檔案存在都不是成功／publication 的額外授權。Resume 是後續人工 Pi 操作，不是重啟 workflow。

## 8. 可編譯的最小範例

完整單節點範例：[`internal/engine/example_test.go` 的 `ExampleWorkflow`](../internal/engine/example_test.go)。程式碼只放此處作單一來源，不在文件另抄一份容易漂移的 skeleton。它展示：

```text
schema Resource/Definition + RunPolicy
  → OpenSession
  → Step（輸出 contract）
  → Decode + 業務比對
  → Decision
  → Result.Outputs + FinalSelection
```

驗證命令：

```sh
go test -count=1 ./internal/engine -run '^ExampleWorkflow$'
```

這個 example **會驗證 schema／definition registry 並核對輸出，但不呼叫 Execute、不啟動 Pi**。Closure 內的 Step/Decode/Decision/FinalSelection 由 Go compiler 檢查目前 API；`fixture/example-model` 只是占位，沒有認證或模型能力驗證。它沒有註冊進正式 CLI，也沒有實作 skills/resources 或 shared-discovery preflight，不能直接當成可交付 workflow。

另參考：

- 真正最小產品：[`smokeEcho`](../internal/workflows/registry.go) 與 [`registry_test.go`](../internal/workflows/registry_test.go) 的 bundled case。僅作形狀參考，不繼承其模型。
- 多階段／session 重用／平行／retry：同檔 `ExampleDecode` **只編譯、不執行**；其中 fixture IDs 同樣不是產品模型/schema。
- 真業務完整流程：[`review/workflow.go`](../internal/workflows/review/workflow.go)。借用框架接線，不複製 PR-specific 規則／角色數／工具權限。

## 9. 驗收與正式 review

先讀並擴充既有 `internal/testutil/protocol`、`internal/testutil/bundled`、engine `bundled_test.go`／`live_test.go`、workflows `registry_test.go` 及相近業務 tests。只有確實不同的層級才新增測試檔，不能每個 workflow 重造 RPC harness。

必要驗收依新增行為具體化，至少考慮：

- [ ] 已核對 reuse map、實際 exports/callers、前置 R/M 單元；沒有新增未說明差異的同質機制或把尚未實作的計畫 API 當現況。
- [ ] 共用 refactor 已遷移原 consumers，舊副本移除或有核實保留理由；錯誤/拒絕條件/會計變化已明列，沒有藉 DRY 削弱保證。
- [ ] 真 engine/Store/filesystem，只替代外部 API/provider/RPC；不 stub Step、不 mock 內部轉換或編排。
- [ ] 必要角色／parallel join／retry budget／fresh 或 reused context 與規格一致，以 channel/RPC/filesystem barrier 控制，不靠 sleep 猜 race。
- [ ] schema／identity／來源 Ref／版本拒絕、資料不足、空結果／缺失角色、業務 reject、最後產出及正確 producer 定位。
- [ ] cancel、attempt/run deadline、partial initialization、storage/journal/finalization failure，以及自有資源 cleanup／history 保留；每個自有 subprocess 有 cleanup 保險。
- [ ] Binary 在 source repo 外、不同 cwd，仍可讀 embedded skills 及全部 references/scripts/assets；既有全域定義/settings 不變。
- [ ] 該 workflow 自己的真 Pi＋真 provider＋真實輸入＋既有 hub 唯讀 E2E，不以 smoke-echo 或別的 workflow 通過代替。
- [ ] 正式獨立 code／scale-failure／simplicity review，必要 local-correctness 補審；runner 採證與 tests PASS 不能替代。只修核實有規格／使用影響的問題，補回歸並複審，不無限擴大極端案例。

**Shared-discovery gate：**每個 persisted Pi 啟動前，重新唯讀核對 deployed WebUI recovery source 與 discovery，依實際判準檢查父 `pid`（不是只看 `piPid`）及 `.recovering`。會 delete/resume 他人 stale session、異常 claim 或來源判準無法確定時 blocked，不代清理。當前 recovery 略過無 truthy pid 的 hub-state 類 JSON；不能把這個例外擴成忽略未知真 session。Preflight 不是鎖，獨立 task 不是 discovery 隔離，shared tests 需在受控時段循序執行；hub 只看自有 session，不 kill/resume/switch 他人。

基本命令（live/E2E 另依本次已批准目標與安全 gate 執行）：

```sh
go test -count=1 ./...
PWC_BUNDLED_PI=1 go test -race -count=1 ./...
go build ./...
go vet ./...
go mod verify
git diff --check
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 \
  go build -trimpath -buildvcs=false -o bin/pi-workflow-controller ./cmd/pi-workflow-controller
```

Opt-in suite 的 skip 不算 PASS；各組實際執行／blocked／未執行及原因、binary 執行、版本、source manifest、原始 logs 只記 repo 外受限目錄，不把該私有位置連回公開文件。Repository 可保留匿名方法、可重現 regression tests 與限制，不放真 PR/ticket、traces/history、日期、session/run IDs、PID、commit SHA、email 或逐次 review 結果。不要在 argv/logs/journal/docs 洩漏 credentials，不修改 Pi/hub 或偷偷解除 TLS 驗證。

## 10. 交接完成條件

- [ ] README 列 registry 新項目、必要工具／認證、使用方式、產出／resume 入口。
- [ ] `docs/<WORKFLOW>.md` 記已批准的公開模型配置、session／contract／錯誤／權限／清理決策，以及匿名測試方法／限制；實際 review/verification 證據只存 repo 外，不把 cached auth 或某次 inference 成功當產品保證。
- [ ] 必要 gates 全部通過才宣稱完成；blocked 明列，不拿先前結果或其他 workflow 代替。
- [ ] 按 [VERIFICATION 的發布 scan 策略](VERIFICATION.md) 檢查內容、本地 links、index 與待發布 history。獲授權後只提交 source、專用 embedded resources、schemas、匿名 tests 與通用 docs；不得把本機報告或真實來源納入 commit，也不將交接 SHA 寫入 repo 文件。未獲要求不 commit/push／開 PR。
- [ ] 更新重用能力登記及工程狀態：實際共用 API/callers、已完成且禁止重做單元、剩餘依賴與下一個未阻擋工作。接手者不得只看歷史待辦而重做已提交成果。
- [ ] 不修改既有全域定義，不將 code-review 內容自動寫入個人 wiki。

給下一個 agent 的起始訊息可以是：「在指定 HEAD 接手，先讀 README 與 `docs/ADDING-A-WORKFLOW.md`，核對 working tree。此次新增 `<name>`，已批准的輸入、角色／模型、權限、contracts、期限、真實驗收目標如下……未決項目如下……。依指南完成實作、獨立 review、分組驗收與發布安全檢查；實際採證只存 repo 外，commit 另依授權，不擴大框架範圍。」
