# Pi Workflow Controller 框架設計

需求：[PRD](PRD.md)。維護入口：[IMPLEMENTATION](IMPLEMENTATION.md)。測試 gates：[VERIFICATION](docs/VERIFICATION.md)。業務 workflow 見 [固定 deep code-review](docs/CODE-REVIEW.md)，交付 API 見 [FINAL-DELIVERY](docs/FINAL-DELIVERY.md)。

本文件描述現行框架語義，不記錄 milestone 或實測結果。Source 宣告見 [engine API](internal/engine/api.go)、[runtime types](internal/runtime/types.go)、[contract types](internal/contract/contract.go)。Publish/commit、fatal ingress、provenance、slot、outcome lock、獨立 cleanup 與 terminal ownership 是維護時不可回退的不變量。

## 1. 範圍與設計原則

已確認的產品決策：

- Go 單一 binary，Bubble Tea TUI；workflow、角色模型與防禦參數由 Go 程式碼定義。
- CLI：`pi-workflow-controller list`、`pi-workflow-controller run <workflow> "Prompt"`。
- 每個 Controller 一個 run，可同時開多個 Controller；不做 crash resume、supervisor 或動態 workflow config。
- 所有執行資料落在 `~/WIP/`，task ID 自動產生；Pi session 保持持久化，沿用既有 WebUI／hub。
- 正常成功、失敗、取消及 timeout 均清理自有 Pi；Controller 無法收尾時允許殘留。
- 支援任意角色數量、逐角色模型綁定、session 重用與跨階段隔離。

工程選擇：

- 普通 Go 函式加少量 structured concurrency helper，不建立 DAG interpreter、排程服務或 plugin system。
- Interface 只放在 Pi process／RPC 外部邊界；檔案 store、狀態 reducer 與流程 helper 使用 concrete type。
- 不將模型文字回答當作 process 間的業務介面；下游只讀已發布 JSON contract 及其檔案引用。
- 無法證明本次 dispatch 的完成歸屬時明確失敗，不推測成功、不自動重送。
- 文件的數值是目前工程預設，可依測試調整，不是要求使用者再次決定的產品選項。

## 2. Package 與依賴方向

```text
cmd/pi-workflow-controller
  ├── workflows ──> engine ──> runtime
  │                   └─────> contract
  └── tui ─────────> engine 的唯讀狀態與取消入口

runtime、contract 不依賴 engine；所有核心 package 不依賴 tui。
```

| Package | 責任 | 明確排除 |
|---|---|---|
| `internal/runtime` | Pi 啟動、RPC codec、request map、session 觀測、dispatch 完成歸屬、process ownership 與清理 | 業務 schema、workflow 轉移 |
| `internal/contract` | Run 目錄、attempt 檔案、schema registry、驗證與發布、digest 引用 | Pi RPC、業務真假判斷 |
| `internal/engine` | Workflow 執行、scope、step/attempt、retry、parallel/join、journal、snapshot | 業務審查規則、終端繪製 |
| `internal/workflows` | Registry、角色 spec、schema、Go 流程、結果驗收 | Process 與 RPC 細節 |
| `internal/tui` | 狀態畫面、文字與色彩、spinner、耗時、feedback、取消 | 自動推進流程、重新派送工作 |
| `cmd/pi-workflow-controller` | CLI 驗證、依賴組裝、signal、exit code | 業務邏輯 |

目前不抽成對外 SDK，不維護公開 Go API 相容性。不要先新增 repository、service、event bus 等對應抽象層。

## 3. 身分、資料與生命週期

### 3.1 身分關係

```text
TaskID（自動產生；本版一次 CLI 呼叫一個 task）
└── RunID（一次 workflow 執行）
    ├── SessionHandleID ── Pi PID / PGID / Pi SessionID / SessionFile
    └── StepInvocationID（scope path + step key 的一次邏輯呼叫）
        └── AttemptID（一次已接受的 Step 執行，包含派送前等待與驗證）
            └── DispatchToken（隨機、不可重用的關聯值）
```

- TaskID：`pw-<UTC YYYYMMDDTHHMMSSZ>-<12 hex random>`；以 exclusive mkdir 防碰撞，不以 Prompt 生成名稱。
- RunID、handle、invocation、attempt、dispatch token 使用至少 128-bit 隨機識別。
- Step key、scope name、branch name 是 workflow 程式碼中的穩定標籤，不接受 Prompt 當目錄名稱。
- Attempt number 從 1 開始，在 Step 通過結構 preflight、取得 handle lease 並進入 Preparing 時配置；同時消耗 run 的總 attempt 額度，之後即使未派送、遭拒絕或準備失敗也不退還。Retry count 從 0 開始，與 attempt counter 分開。
- 結構 preflight 包含 scope/key 唯一性、schema 已註冊、handle 屬於本 run 且可用、timeout 合法及總額度未耗盡。此階段拒絕不建立 attempt；開始 Preparing 後的 Ref 讀取、request 落地、WaitingSession 等失敗都有自己的 attempt ID／終態。
- ID 不作為認證憑證；nonce 防止意外拿錯產物，不抵抗有本機檔案權限的惡意 agent。

### 3.2 Session 與角色

- `RoleSpec` 是 provider/model/thinking/system instruction 的值，不是 process。
- `OpenSession` 每次建立新 handle、新 Pi process、新持久化 session。SessionDir 在本次 run 下；不得使用 `--continue`、`--resume` 或 `--no-session`。
- 一個 handle 終身綁定一個 Pi SessionID，不在原 process 內呼叫 `new_session`／`switch_session`。
- 同一 handle 的多次 step 保留 context。需要新 context 時由 workflow 再呼叫 `OpenSession`。
- Run 登記所有已啟動 child；即使 Start readiness 失敗，也不能遺漏已存在的 process。
- Workflow 可以提早 `CloseSession`；否則 run 結束統一清理。Close 必須可重入且冪等。
- 同一 handle 一次只能被一個 Controller step 使用，鎖涵蓋派送、等待、驗證與發布；衝突回報 `SessionBusy`，不偷偷排隊。
- 執行前提是 Controller 唯一派工、使用者只觀看。Hub 技術上仍可繞過 Controller 鎖，但人工介入不屬支援的驗收情境；不為此新增全域鎖或修改 hub。
- Fresh session 隔離對話歷史，不等於隔離檔案、認證、工具或全域 AGENTS.md。

## 4. 核心 API

以下固定責任與呼叫形狀；省略 JSON tags、enum 常數與機械欄位，不是獨立可編譯程式。

### 4.1 Runtime 邊界

```go
// internal/runtime

type ModelSpec struct {
    Provider string
    ID       string
    Thinking string
}

type SessionSpec struct {
    HandleID      string
    Name          string
    Model         ModelSpec
    CWD           string
    SessionDir    string
    AppendPrompt  string
}

type Dispatch struct {
    Token   string
    Message string
}

type Execution struct {
    SessionID       string
    Token           string
    StartSeq        uint64
    SettledSeq      uint64
    ActivityEpoch   uint64
    PromptEntryID   string
    LastEntryID     string
    LastAssistantID string
    StopReason      string
    ExtraUserInputs int
}

type Runtime interface {
    Start(context.Context, SessionSpec) (Session, error)
}

type Session interface {
    Identity() Identity
    Snapshot(context.Context) (SessionState, error)
    ContextUsage(context.Context) (ContextUsage, error)
    Execute(context.Context, Dispatch) (Execution, error)
    Confirm(context.Context, Execution) (Confirmation, error)
    Close(context.Context) (CleanupReport, error)
}
```

- `Execute` 不讀 contract，回傳 Pi 執行證據；`Confirm` 是候選產物快照之後的觀測檢查，不是 distributed lock。
- Runtime 在建構時接受具體觀測 callback，輸出 bounded `Observation`，供 engine 記錄健康與執行進度。不要讓多個 consumer 競爭讀同一 RPC channel。
- `Identity` 包含 handle、Pi SessionID、SessionFile、PID、PGID、spawn 時間及 executable。PID 不由 hub 列表反推。
- `SessionState` 包含 process health、session identity、selected model、streaming/compacting、pending count、last response time。它是觀測 snapshot，不是全域 idle 保證。
- `Confirmation` 包含最後確認的 RPC seq 與 activity epoch。Engine 只在未觀測到 epoch 變更時提交產物；此時間邊界見第 6 節。
- `CleanupReport` 分開回報 abort、SIGKILL、Wait、discovery 清理與未確認項目。

### 4.2 Workflow 與 engine API

```go
// internal/engine

type Input struct {
    Prompt    string
    LaunchCWD string
}

type Result struct {
    Outputs map[string]contract.Ref
    Final   *FinalSelection
}

type FinalSelection struct {
    Output string
    FileID string
}

type FinalDelivery struct {
    Output       string
    Ref          contract.Ref
    ArtifactPath string
    HandleID     string
    Scope        string
    Step         string
}

type Workflow func(context.Context, *Run, Input) (Result, error)

type Definition struct {
    Name        string
    Description string
    Version     string
    Policy      RunPolicy
    Execute     Workflow
}

type RoleSpec struct {
    Name         string
    Model        runtime.ModelSpec
    CWD          string
    AppendPrompt string
}

type Feedback struct {
    Message         string
    SourceAttemptID string
    SourceCode      string
    Refs            []contract.Ref
}

type RetryState struct {
    ActivationID string
    RetryCount   int
    MaxRetries   int
    Feedback     *Feedback
}

type StepSpec struct {
    Key      string
    Session  *SessionHandle
    Prompt   string
    Inputs   []contract.Ref
    Feedback *Feedback
    Output   contract.Spec
    Timeout  time.Duration
}

type StepResult struct {
    Output    contract.Ref
    AttemptID string
    Execution runtime.Execution
}

type Branch struct {
    Name string
    Do   func(context.Context, *Scope) (Result, error)
}

type BranchResult struct {
    Name   string
    Result Result
    Err    error
}

type RetryAction struct {
    Again    bool
    Feedback *Feedback
    Result   Result
}
```

主要方法：

```go
func (r *Run) OpenSession(ctx context.Context, role RoleSpec) (*SessionHandle, error)
func (r *Run) CloseSession(ctx context.Context, h *SessionHandle) error
func (r *Run) CloseSessionReport(ctx context.Context, h *SessionHandle) (runtime.CleanupReport, error)
func (r *Run) SessionContextUsage(ctx context.Context, h *SessionHandle) (runtime.ContextUsage, error)
func (r *Run) Root() *Scope
func (r *Run) AddCleanup(ctx context.Context, name string,
    close func(context.Context) error) error
func ReadContract(ctx context.Context, r *Run, ref contract.Ref) (json.RawMessage, error)
func Decode[T any](ctx context.Context, r *Run, ref contract.Ref) (T, error)

func (s *Scope) Child(name string) (*Scope, error)
func (s *Scope) Step(ctx context.Context, spec StepSpec) (StepResult, error)
func (s *Scope) Parallel(ctx context.Context, name string, mode JoinMode,
    branches []Branch) ([]BranchResult, error)
func (s *Scope) Retry(ctx context.Context, name string, maxRetries int,
    fn func(context.Context, *Scope, RetryState) (RetryAction, error)) (Result, error)
func (s *Scope) Decision(ctx context.Context, name string,
    reason string, refs []contract.Ref) error
```

- 建構入口：`engine.New(ctx, Definition, Input, Options)`，先完成 definition/schema/prompt preflight，再建立 run；`Run.Execute()` 單次執行，回傳 outcome、exit code、result、cleanup、finalization errors 與 snapshot。`engine.NewRegistry` 提供穩定排序與查詢，沒有外部 workflow config。
- `Run`、`Scope`、`SessionHandle` 是 concrete type。Workflow 不持有底層 Session 或 `exec.Cmd`。
- `Step` 自動建 attempt，落 request，派送，檢查 execution，驗證並發布 output。任何階段失敗都留下結構化原因。
- `Result.Outputs` 只持有已 committed Ref，不用 memory-only JSON 傳給下一個 Pi；`Result.Final` 明確選 output key 與可選 artifact file ID，不是任意檔案路徑。
- Workflow 透過 `engine.Decode[T](ctx, run, ref)` 讀取業務資料，供 B verdict 等 Go 分支判斷；內部走本 run 的統一 Ref resolver（第 8.3 節）。這是 package function，不是 generic method，也不把 Store 的寫入權限暴露給 workflow。
- Workflow 回傳成功後、outcome 鎖定前，engine 重新檢核 Result 裡所有 Ref，從 producer attempt 建立 `FinalDelivery`，並寫入 `result.json` 的 `run_id`、`outputs` 與可選 `final`。`final` 含 output、ref、artifact_path（若有）、handle_id、scope、step；Session ID/file/role 由 handle 查 `run.json.sessions`。結果檔不是成功證明，權威 outcome 在 RunFinalizing／RunFinished 事件及 run snapshot，持久化失敗另須看 EmergencyStatus。
- `Report.Final` 與 `result.json.final` 使用同一份 cleanup 前驗證 metadata。普通 workflow error 仍可解析明確選定的合法 incomplete report，不覆蓋原始 error；root cancellation/deadline/fatal 或 callback fatal 不額外解析 Final。無合法選擇就顯示 unavailable。Formatter 只讀 Report、不重開 Store、不依 map 順序或最後關閉 session 猜 producer。它不是新的 publication capability，完整收尾規則見第 7.4 節。
- `ReadContract` 取得同一 resolver 驗過的完整 envelope bytes；`Decode[T]` 解碼其中 data，不另讀 mutable candidate。`AddCleanup` 的 acquisition ownership 與失敗責任見第 9.4 節。
- 本地業務驗收由 workflow Go 程式碼執行，重要轉移呼叫 `Decision` 留下依據；框架不替 workflow 判斷 root cause。

### 4.3 Contract API

```go
// internal/contract

type Identity struct {
    RunID         string
    InvocationID  string
    AttemptID     string
    DispatchToken string
}

type Spec struct {
    SchemaID string
}

type Ref struct {
    RunID     string
    AttemptID string
    Path      string
    SchemaID  string
    SHA256         string
    ManifestSHA256 string
}

func (s *Store) BeginAttempt(id Identity, request Request) (*Attempt, error)
func (a *Attempt) Stage(ctx context.Context, spec Spec) (*Staged, error)
func (a *Attempt) Publish(ctx context.Context, staged *Staged) (Ref, error)
func (s *Store) Read(ctx context.Context, ref Ref) (json.RawMessage, error)
```

- Store 本身是 concrete type，測試使用真實 temp directory；只有 engine 持有 Store。`Read` 做 rooted filesystem、identity/schema/digest 檢查，engine resolver 在呼叫它之前另核對 committed publication membership。
- `Stage` 讀取候選檔案、schema/identity 檢核並複製 referenced files 到私有 staging；`Publish` 原子發布這一份已驗證 bytes，不重新讀 candidate。
- `Staged` 不可從 workflow 自行建構。讀／發布失敗是外部 filesystem error，不給下游半成品 Ref。
- Concrete API：`NewRegistry(resources, definitions)` compile immutable registry；`NewStore(registry, Options)` 建新 run，不提供 reopen／resume。`NewID()` 供 engine 配置 invocation／attempt／token；`Attempt.Number()` 與 request／candidate 路徑 accessor 供 engine 接線。Confirm 失敗時呼叫 `Staged.Discard()`，Store 不自行呼叫 runtime 或假造 committed membership；`Close()` 只關 descriptor，不刪 run/history。

## 5. 流程、scope 與 retry

### 5.1 Scope 命名與 attempt

- `Child("round-003")` 建立新的邏輯 scope；同一 parent execution 內名稱不可重複。輪次用新 child 表達，不重用 round key。
- Logical scope ID 由穩定 path 識別；execution epoch 表示該 scope 的一次執行。Invocation key = logical scope path + StepSpec.Key，平行分支增加 branch name。
- 同一 scope execution 內同一 step key 不可呼叫兩次；重跑必須來自 `Retry`，不由同名呼叫隱含重試。
- 每次進入 Retry 呼叫建立新的 activation ID 與 budget；同一 activation 的各次 callback 使用同一邏輯 scope path、不同 execution epoch。被重跑的 step invocation 增加 attempt number。
- 外層重跑而再次進入同名內層 Retry，建立新內層 activation，budget 從 0 起算；其 logical path 不變。Child/Retry 名稱重複檢查限定同一 parent execution，不跨 epoch 拒絕合法重跑。
- Step attempt number 按進入 Preparing 的次數累加，不以實際 prompt 數量或某個 retry count 代替；是否送出及是否接受由獨立 dispatch 狀態記錄。RetryState 帶 activation ID、RetryCount、MaxRetries、前次 Feedback；總 attempt 上限防止巢狀 budget 乘積失控。
- 沒被執行的分支不產生虛構 attempt；不預建整個 workflow graph。

### 5.2 循序與條件

普通 Go 呼叫、`if`、`switch`。條件只能從成功發布的 contract 或明確的執行錯誤導出。Level 1 schema 失敗時，Step 不產生可消費 Ref，自然不呼叫下游 reviewer。

### 5.3 Parallel 與 join

- 分支輸入在啟動前固定為 Ref；各分支使用自己的 Scope 與不同 session handle。
- `FailFast`：第一個分支 error 取消 siblings；仍 join 所有分支後返回。回報原始錯誤與所有分支結果，不能以 sibling 的 Cancelled 蓋掉起因。
- `CollectAll`：branch error 不取消 siblings；全部完成後回傳有序 BranchResult。只有 parent 取消、執行器錯誤才回傳 group-level error。
- 結果按宣告順序排列，不依完成時間。重複名稱在啟動任何分支前拒絕。
- Business contract 的 `reject` 不是 branch error；Go workflow 決定要不要 retry。
- API 不提供 detached workflow goroutine。所有 helper 建立的子工作必須在 scope 返回前 join。
- 不支援任意 Go callback 的強制終止；workflow 是可信的內建程式碼，必須遵守 context。Controller 內部 deadline handler 仍可停止所屬 Pi，但不能安全殺掉不合作的 Go goroutine；這不是另一個 watchdog process。

### 5.4 Retry 與 feedback

- `maxRetries >= 0`，首次 callback 的 RetryCount 為 0。僅 `RetryCount < maxRetries` 才可增加計數再呼叫。
- `Again=false`：完成 scope，返回 Result。`Again=true`：必須帶非空 feedback；耗盡則 `RetryExhausted`，保留最後原因。
- Callback 回 error：立即失敗，不隱含重試。若 workflow 明確要修正 schema，可將該錯誤轉成 RetryAction。
- Feedback 包含文字、來源 attempt/error code、可選已發布 Ref。寫入新 attempt 的 request，送回哪個 handle 由 workflow 決定。
- Reviewer reject 與 schema 修正可以共用同一 Retry scope；是否拆 budget 由 workflow 程式碼決定。
- Provider auto retry 在同一 attempt 內，由 Pi 管理；記錄其獨立 counter，不消耗 workflow budget。
- Controller 不在 pipe 斷線、ack timeout 或結果不明時自動重送 prompt。Retry 也不代表外部 API／檔案修改的 exactly-once。

### 5.5 A → B → C 的 feedback 鏈路（通用範例）

本節展開 produce/review/deliver 控制模式，不把 A/B/C、pass/reject 或 reviewer schema 變成框架限制。

```text
建立 A、B、C session handle
│
└─ Retry scope：produce-and-review，maxRetries = 3
   │
   ├─ A：接收原始任務、明確輸入 Ref 與本次 feedback
   │   ├─ schema 不合格 → Controller 產生格式 feedback → 請求 scope retry
   │   └─ schema 合格 → 發布本次 A contract
   │
   ├─ B：只審查這次發布的 A contract
   │   ├─ B contract 不合格 → 整個 scope 失敗，不把它當有效 reject
   │   └─ B contract 合格 → workflow Go 程式碼讀取 verdict
   │       ├─ reject → 從 B contract 取 feedback → 請求 scope retry
   │       └─ pass   → 回傳這一組 A Ref + B Ref，結束 scope
   │
   └─ retry：有額度則回 A，否則 RetryExhausted，不執行 C

scope 成功 → C：接收已通過的那一組 A Ref + B Ref → 最終驗收
```

#### 誰決定回頭、誰傳遞 feedback

- B 不直接呼叫 A，也不自行往 A 的 RPC pipe 送訊息。B 的輸出是落地的審查 contract。
- Controller 先完成 B execution 與 schema 檢核，再由該 workflow 的 Go 程式碼解讀 verdict；框架本身不知道 pass/reject 的業務意義。
- Workflow 將 reject 轉成 `RetryAction{Again: true, Feedback: ...}`。Feedback 帶退回文字、B attempt ID、B contract Ref 及被審查的 A contract Ref；不是只在 TUI 顯示一段字。
- Retry helper 檢查 budget，允許時將 feedback 帶到下一次 callback 的 `RetryState.Feedback`。Callback 必須明確把它放入 A 的 `StepSpec.Feedback`；helper 不猜 feedback recipient，也不自動廣播給所有 step。
- A 的新 `request.json` 保存 feedback 全文、來源與 refs。派給 A 的 prompt 明確要求讀取 request、按 feedback 修正，並寫到本次新 candidate 路徑。這是上一輪已結束後的一次新 dispatch，不是對舊 attempt 追加 steer。

#### Session 與產物如何重用

- A handle 在 Retry scope 外建立，所有修正都使用這個 handle，因此保留 A 的既有 context；每次修正仍建立全新 attempt ID、dispatch token 與輸出目錄。
- 本範例可讓 B、C 沿用初始化建立的 handle；一般 workflow 也可在每次審查建立新 B handle。B 的 context 策略不改變 A 的 feedback 路由。
- 不只依賴 A 記得上一輪對話。新 request 仍帶原始任務、必要輸入 refs、被退回的 A Ref 與 B 的審查 Ref，以明確資料支撐修正。
- B 每次使用 callback 內剛取得的 A Ref。此範例的 reviewer contract 必須識別被審查的 A Ref／digest，由 workflow 檢查匹配；不接受針對舊版 A 的 pass，也不以 A/B attempt number 相等推斷版本配對。
- RetryAction 的成功 Result 必須同時返回當次 A、B Ref；C 位於 Retry scope 外，只接收這一組，不搜尋最新檔案、不讀共享變數裡殘留的前次 pass。

#### Budget、格式錯誤與終止

- 此範例 A 的 schema 修正與 B 的 reject 共用 `produce-and-review` budget。首次加三次重試，A 最多派送四次；B 只在 A schema 合格時執行，C 只在本次審查 pass 後執行。
- A schema 失敗的 feedback 來源是 Controller 的 validation report，帶 A attempt ID、錯誤碼與欄位路徑；不存在可給 B 消費的合法 A Ref，也不偽造 B review。
- RetryExhausted 保留最後 feedback；取消、timeout 或不可重用的 A handle 不被當成內容 reject 自動重跑。若不能再沿用原 A session，本範例失敗，不靜默換新 session 假稱延續原 context。
- 例如 `A1 → B1(reject) → A2(schema invalid) → A3 → B2(pass) → C1`：兩次退回消耗兩次 retry，C 收到的是 A3/B2，A1/B1 只保留為歷史。
- C 自己失敗不隱含回 A；若某 workflow 需要 C 回饋或另一層驗收迴圈，須另外用 Go 明確定義，不擴大這個範例的 retry 範圍。

## 6. Runtime 派送與完成判斷

### 6.1 啟動與資源載入

1. 檢查 `pi --version` 是目前列入支援表的 `0.84.3`；不靜默降級為 `agent_end`。新版本須先通過 protocol tests 再擴支援表。Version probe 成功後、建立 session 目錄及 persisted child 前，由共用 runtime 對有效 BridgeDir 執行唯讀 parent-pid／`.recovering` preflight；拒絕為 `BridgeUnavailable`、phase `preflight`、`DispatchAccepted=No`。OpenSession 已登記的 handle 仍消耗 total-session 額度，關閉該未啟動 handle 不虛構 Wait／process cleanup，也不產生 attempt。
2. 使用 `exec.Command` argv，不經 shell 拼接 Prompt。macOS child 以 `Setpgid: true` 建立自己的 process group。
3. 使用 `--mode rpc --provider ... --model <exact-id> --thinking ... --session-dir ... --name ...`，不在 CLI 帶 provider secret。
4. 保留全域 Pi 設定、skills、context files 與 extension 載入；不使用 `--no-extensions`，不自動提升 project trust。Role cwd 依 `Role.CWD` → `engine.Options.PiDefaultCWD` → `Input.LaunchCWD` 選擇。Default 在 `engine.New` 依 immutable LaunchCWD 固定相對路徑；engine 不讀 env，CLI run 才將 `PWC_PI_CWD` 傳入。空字串停用，不 trim／展開 home，不在 constructor stat 目錄；僅選用的 PiDefaultCWD 含 NUL 時在 OpenSession 提前拒絕；explicit Role.CWD／fallback LaunchCWD 及其餘不可用目錄保留原 runtime 驗證與會計路徑。Effective Role 仍記入 SessionStarting／snapshot，Input 與 runtime identity 不改。
5. 使用同一個已存在的 `PI_BRIDGE_DIR` 或 deployed WebUI 預設 discovery 目錄；不建立隔離的 discovery 目錄令 hub 看不到 session。Runtime.New 依明示 Options.BridgeDir、parent 環境、home 預設選定目錄；相對值依建構時 Controller cwd 固定為絕對路徑。Child env、preflight、owned discovery 及 cleanup 都使用此值，不受 role cwd 或 Options.Env 同名項目影響。避免繼承父 session 的固定 `PI_HTTP_PORT`，由 extension 尋找 port。
6. 保持 stdin pipe 開啟。stdout 一個 reader，stderr 持續 drain 到有界檔案。
7. 用 `get_state` 確認 RPC 可用、provider/model/thinking 與 session identity 正確。Model 模糊匹配或不支援的 thinking 不可靜默接受。
8. 查到對應 sessionId/piPid 的 discovery 後標示 HubVisible；startup 期限內未出現則 `BridgeUnavailable`，保留診斷後清理。這是檔案觀測，不建置 hub API 整合。

Online 表示 process 與 RPC 存活，不代表 provider 認證／推論成功；認證問題可能直到第一個 prompt 才暴露。

### 6.2 RPC transport

- LF-delimited JSONL；允許 LF 前的單個 CR，不把 Unicode U+2028/U+2029 當分隔。
- 有界 line buffer，不使用 Go Scanner 預設 64 KiB 限制，也不無限增長。
- 每個 command 唯一 request ID。單 writer 序列化 stdin；reader 分流 response 與 event。
- 每個輸入 frame 取得單調遞增 seq。Command response 依 ID 關聯；agent event 不宣稱有 dispatch ID。
- 第一次 `get_state` readiness response 使用剩餘 startup deadline；RPC ready 後才使用一般 response timeout。Start 的 parent typed cancellation／deadline cause 保留，runtime 自己的 startup timeout 在 preflight／discovery 階段記 BridgeUnavailable，其餘階段記 StartFailed。所有階段共用原 StartupTimeout，不另開 preflight budget；同步 filesystem syscall 仍有既定不可中斷限制。
- Append cursor 保留在 session 層，跨 Step 只讀增量 entries。Event／entry hashes 增量配對後釋放，1024 限制的是未匹配 backlog，不是 attempt lifetime 訊息總數；已驗證的線性 append lineage 用 checkpoint 摘要保存，fork／rewind 保守拒絕，不保留無界 node map。
- Entries 查詢由相關完成／entry 事件喚醒，streaming delta 不驅動查詢；沒有進度時由 50ms backoff 至 5 秒，保留 event 先於 entry append 的 fallback。
- Ack 前到達的 event 也必須保留；不能由等待 ack 的 goroutine 丟棄。
- Unknown event type 可記錄後略過；invalid JSON、必要欄位錯誤、超大 frame 或 EOF 中的半個 frame 是 protocol failure。
- Process exit／EOF 會解除所有 pending waiter；錯誤保留 stderr tail。Reader 不等待 TUI 繪製或慢速 journal I/O。
- RPC state query timeout 不是 provider timeout；provider 沒輸出但 RPC 仍有回應，不判為 Offline。

### 6.3 Dispatch envelope 與事件關聯

Prompt 由 engine 組成固定前綴，包含 dispatch token、request.json 絕對路徑、candidate 路徑及角色指令；使用者 Prompt 存在 request 裡。Dispatch 明確要求先執行 request.prompt，再讀 schema／envelope path 及產出 task result，避免把寫空 envelope 當成已完成任務；規格檔仍是唯一 schema 來源。固定前綴不以 `/` 開頭，避免意外執行 extension command。需要 skills 時由 workflow 指令要求載入，不依賴把整段 dispatch 當 slash command。

Dispatch 流程：

1. 取得 handle 的 step lease；記錄 pre-dispatch identity、model、append cursor 及活動 epoch。
2. 派送前 readiness 由 state、pending queue 及已觀察到的未結束活動共同判定。既有 agent run/retry 要等其 settled；獨立 manual compaction 則等 `compaction_end` 後重查 idle state，不要求不存在的額外 agent_settled。所有活動結束事件都只觸發重新 preflight，不能直接視為 ready。等待計入 attempt deadline；等待期間的事件不算本次 dispatch 的結果。獨立 manual compaction 的既有防禦可以保留，但人工觸發相容性不再是必要 gate。
3. 以不帶 streamingBehavior 的 RPC `prompt` 派送。Busy race 可能導致明確拒絕，該次 attempt 失敗，不自動 queue／重送。
4. 必須同時有成功 ack，以及本次 token 對應的實際 user message。以 `get_entries` 的 append cursor 觀察 SessionManager entry／lineage，處理 message event 與 entry append 時序差。此 API 讀記憶體 entries，不是磁碟持久化證明；第一個 assistant 前 session file 尚不存在也不令 prompt observation 失敗。
5. Input extension 可能吞掉或轉換 Prompt。Ack 成功但無法觀察到本次 token 時，回 `PromptNotObserved`／`AmbiguousExecution`，不等待任意其他 turn 來湊成功。
6. 自 prompt 起觀察 assistant terminal messages、tool 狀態、provider retry、compaction 及 queue。若觀測到 queued continuation，仍等待 settled；既有額外 input 計數可以保留，但不承諾人工 Hub steering／follow-up 的相容性。
7. 在本次 prompt 之後觀察到 `agent_settled` 才進入完成候選。`agent_end`、檔案存在或 silence timeout 都不是替代條件。
8. 檢查最終 assistant 的 stopReason 是 `stop`、沒有未結束工具／retry／queue，且 session／provider／model／thinking binding 不變。`length`、`error`、`aborted` 不發布。暫時 provider error 只有對應 retry 成功結束才可解除；`auto_retry_end.success=false` 是 sticky failure，若 `finalError="Retry cancelled"` 則是 sticky cancellation，即使沒有 aborted assistant message，也不能由後續 queued 人工回答的 stop 覆蓋。Compaction aborted/failed 同樣不能由未證明屬於恢復的後續 turn 消除。
9. 取得 Execution receipt 後 Stage candidate bytes 與引用檔案。
10. `Confirm` 再查 state 與自 baseline 後的 entries，核對 active branch 包含本次 prompt、沒有額外未歸屬 run 或 model/thinking/session drift。與 Stage 期間觀察到的新活動比較 epoch；不一致即丟棄 staged output，回 `AmbiguousExecution`，不重新等待另一個 turn 自動補成功。
11. Engine 在本地控制序列上確認 receipt 未失效，發布 contract，再提交 AttemptSucceeded 與 invocation 的 provisional outcome。所有可重跑的祖先 Retry 結束後才定案 invocation，見第 7.1 節。

不是所有 tool error 都直接令 attempt 失敗：agent 可以修正工具失敗。未解決的 terminal error、protocol error 或 contract validation 才阻擋發布。活動期間的 extension error 保守視為 `ExtensionFailed`，不自動忽略可能影響 dispatch 的失敗。

### 6.4 單一派工來源與觀測邊界

- 使用者啟動 workflow 後不介入，Controller 是唯一派工來源，hub 僅觀看。人工 prompt／steer／Stop／model／thinking／session 變更不列必要相容性或驗收，也不因它們的 RPC 可觀測性缺口要求修改 Pi／hub。
- 不關閉或修改 hub 的既有操作功能。已實作的 drift／lineage／extra input 防禦可保留，但通過部分防禦案例不等於支援任意人工介入。
- Controller 自身取消、timeout、SIGINT／SIGTERM 及已觀測到的 aborted／retry cancellation 仍按既定規則處理。無法證明本次完整結束時仍按 ambiguity／deadline 失敗，不能用舊 contract 成功。
- Hub browser 切 sidebar 不改變原 Pi session，Controller 不受影響。若其他 extension 真正 switch/new/fork session，identity／branch 檢查失敗，handle 不可繼續使用。
- Provider/model/thinking 都是 attempt 全程 binding，不是 startup hint。Preflight 檢查 state，派送到 Confirm 期間核對 assistant model、model_change entry、`thinking_level_changed` event 與 `thinking_level_change` entry。任何可觀測的不符都是 sticky drift，即使改回原值也回報 `ModelChanged`／`ThinkingChanged`，不由 Controller 默默改回。Startup 的初始設定事件在 dispatch baseline 前，不當成 drift；不推論 provider 內部不可觀測的 reasoning 行為。
- 同 token 出現在多個新的 user entry，或 observed event 與 entry lineage 矛盾，視為 ambiguity。
- 第 6.3 節的確認只建立「截至本地最後確認點」的產物快照，不提供與 hub／filesystem 原子協調的 task fence。確認點之後新發生的人工活動不追溯改寫已發布 Ref；下次 Step 必須重新 preflight。
- 不宣稱 malicious input 隔離、全域 exactly-once 或外部副作用 rollback。在上述單一派工前提下仍不能區分的事件順序，必須在測試中保守失敗。
- 若 protocol gate 在上述支援範圍內證明僅用原生 RPC 仍會錯認完成，該 gate 不通過；先提出最小修正及其範圍影響，不偷偷改 hub、加入 extension 或改用 `agent_end`。

### 6.5 錯誤後的 handle 使用

- 本節列常見規則，完整錯誤分類與 precedence 以第 7.2 節的 disposition 表為準。
- ContractMissing/ContractInvalid/IdentityMismatch 只要 execution 已明確 settled、session/model/thinking 未變，handle 可留給 workflow 修正重試；不自動建立新 session。
- 明確 dispatch rejection 且 identity 未變，可以在下一個顯式 step 重新 preflight；這不是原 command 的 transport resend。
- Timeout、取消、ack ambiguity、protocol/process failure、identity/model/thinking drift、無法確認的 prompt observation、InteractionRequired 或 extension failure，將 handle 設為不可派送並執行 bounded Close。後續 workflow 若要繼續只能明確 OpenSession。
- ProviderFailed 若已收到本次 settled 且 state 可確認 idle，允許保留 handle；是否重跑仍由 workflow 決定，sticky failure 只限制本次 attempt，不能跨 attempt 偷變成功。
- Stage/Confirm 之間新活動造成 ambiguity，同樣關閉不再可用的 handle；這會停止該自有 Pi 的新活動，TUI 應顯示原因，不宣稱把人工 turn 排隊留到下次。

### 6.6 Extension UI

- `notify`／`setStatus` 等 fire-and-forget request 轉成有界診斷，不作為流程轉移。
- Hub 已有的對話／工具互動照原機制運作，不重建到 Controller。
- 原生 RPC 的 select/confirm/input/editor dialog 不保證被 hub 代答。目前不另建 dialog frontend；遇到此 request，回 `cancelled: true` 並令本次 step 回 `InteractionRequired`，不得自動批准，也不得永久掛住。
- 若已安裝 WebUI 必要流程依賴這類 RPC dialog，須將其列為相容性問題，不能宣稱 hub smoke 通過。

## 7. 狀態、錯誤與 deadline

### 7.1 狀態

| 層級 | 非終態 | 終態 |
|---|---|---|
| Run | Created、Running、Finalizing | Succeeded、Failed、Cancelled、TimedOut |
| Step invocation | Pending、Running、Retrying、AwaitingScope | Succeeded、Failed、Cancelled、TimedOut |
| Attempt | Preparing、WaitingSession、Dispatching、Running、Validating | Succeeded、Failed、Cancelled、TimedOut |
| Session | Starting、Online/Idle、Online/Busy、Unresponsive、Closing | Closed、Exited |

- 每次已進入 Preparing 的 attempt 最多提交一次 AttemptSucceeded/Failed/Cancelled/TimedOut，包含未派送就結束的情況；舊 attempt 終態與已發布 Ref 不回滾。
- 尚有可重跑該 invocation 的祖先 Retry 時，invocation 處於 AwaitingScope，保留 latest attempt 的 provisional outcome。祖先再次重跑時回到 Running，不曾提交 invocation 終態。
- 所有可重跑的祖先 Retry 均結束後，依最後實際 attempt 定案 invocation；只有內層 Retry 結束不足以定案。未被最後一輪執行的 step 保留最後實際 attempt 與 epoch 標記，其 Ref 不會被自動選為本輪輸出。沒有 retry ancestor 的 step 可直接定案。
- Attempt／invocation 終態與 workflow outcome 由 engine 序列化提交一次。進入 Finalizing 時鎖定 workflow outcome，該點前 deadline 已到期則不能選 success；晚到的取消不覆蓋已鎖定的結果。
- Cleanup 使用獨立期限，跨過原 run deadline 不推翻已鎖定的 workflow outcome。RunFinished 分開帶 workflow outcome、cleanup outcome 與 finalization errors；成功但清理或收尾持久化失敗，顯示對應警告且 CLI exit 非零（第 7.4 節）。
- Cancellation／timeout 不等同 provider failure；`context.Cause` 保留原始因果，FailFast sibling 取消也記錄發起的 branch。

### 7.2 錯誤

`Failure` 包含 code、message、phase、run/step/attempt/handle ID、cause、typed Origin、可選 LimitScope，以及 `DispatchAccepted` 三態（yes/no/unknown）。Origin 區分 ControllerUser、SignalINT、SignalTERM、ExternalAgentAbort、RunDeadline、AttemptDeadline、FailFastSibling、Protocol、Provider、Compaction、Contract、Storage、Definition；無法區分 hub/extension 的 abort 使用 ExternalAgentAbort，不偽造使用者歸屬。透過 `errors.As`／`context.Cause` 保留分類，不靠錯誤字串或包裝層數推導。

| 事件／Failure code | Attempt 終態（若已建立） | Handle 處置 | 未被 workflow 處理而傳至 run 的 outcome / exit |
|---|---|---|---|
| Controller q、SIGINT | Cancelled | Close | Cancelled / 130 |
| SIGTERM | Cancelled | Close | Cancelled / 143 |
| aborted assistant、Retry cancelled、compaction_end.aborted | Cancelled，Origin=ExternalAgentAbort | Close | Cancelled / 130 |
| RunDeadline、AttemptDeadline | TimedOut，保留實際 Origin | Close | TimedOut / 1 |
| FailFastSibling | Cancelled，cause 指向根 branch error | Close | group 傳根 branch error，不能用 sibling 取消蓋掉根因 |
| terminal error／耗盡 retry：ProviderFailed | Failed | 本次已 settled 且 binding 正確、idle 時可保留，否則 Close | Failed / 1 |
| terminal length：OutputTruncated | Failed | 同上一列 | Failed / 1 |
| compaction_end.errorMessage：CompactionFailed | Failed | Close | Failed / 1 |
| InteractionRequired、ExtensionFailed | Failed | 先取消 dialog（若有），再 Close | Failed / 1 |
| ModelChanged、ThinkingChanged、SessionChanged | Failed | Close，不回復舊設定 | Failed / 1 |
| PromptNotObserved、AmbiguousExecution、ProtocolFailed、RPCUnresponsive、ProcessExited | Failed | Close | Failed / 1 |
| DispatchRejected | Failed，accepted=no | binding 未變則可重新 preflight | Failed / 1 |
| ContractMissing、ContractInvalid、IdentityMismatch、輸出檔案/大小 LimitExceeded | Failed | 合格 settled receipt 存在且 binding 未變時可保留 | Failed / 1 |
| ReferenceInvalid | Failed（Preparing 的輸入亦適用） | 尚未派送則保留；已派送須確認 settled，否則 Close | Failed / 1 |
| InvalidDefinition、SessionBusy | preflight 拒絕，不建 attempt | 不干擾已持有 lease 的 step | run 尚未建立為 exit 2；run 內未處理錯誤為 Failed / 1 |
| StartFailed、BridgeUnavailable、UnsupportedPiVersion | 不建 step attempt | 已 spawn 者 Close | run 尚未建立為 exit 2；run 內為 Failed / 1 |
| RetryExhausted | 不新增 attempt，保留最後 attempt 狀態 | 不自動重派；run 終結時 Close | Failed / 1 |
| StorageFailed、JournalFailed、run 級總量 LimitExceeded | Failed（仍在活動的其他 attempt 取消） | 立即停止新工作並 Close 全 run | sticky run-fatal：Failed / 1，不允許 workflow 吞掉後回 success |
| CleanupFailed、FinalizationFailed | 不追溯改寫既有 attempt | 不再派送，記錄未確認項目 | 不改已鎖定 outcome；按第 7.4 節回非零 |
| 未分類的非 nil error | Failed，原始 cause 保留 | 已有 dispatch 而無完整 receipt 則 Close，否則保留 | Failed / 1，不猜為取消或可重試 |

Pre-dispatch 的獨立 manual compaction 結果只觸發 readiness 重新查詢，不套用本次 dispatch 的 sticky failure；本次 prompt 寫入後觀察到的 compaction failure/abort 才按上表處理。每個 error 都不會自動 dispatch retry；workflow 必須顯式處理，已關閉 handle 若要繼續須 OpenSession。

錯誤正規化取最外層已分類 boundary，不能把 StorageFailed 因內層 timeout cause 降級。Root/collateral 歸屬依明確 attempt identity／handle provenance 判定，不依 error chain 形狀。真正觸發 fault 的 attempt 保留 failure；被同一 root-fatal 停止的 siblings 記 Cancelled，根因留 Cause。Consumer Ref read failure 以當前 operation 的 attempt/handle 記來源；runtime fatal ingress 保留來源 handle。Collateral cancellation 的 DispatchAccepted 必須保留本次 dispatch evidence，不被根因的 accepted=no 覆蓋。

Run 終態仲裁：第一個已登記的 root stop cause（取消、deadline、run-fatal error）鎖住原因，後續 sibling error 不覆蓋。沒有 root stop 時，workflow 返回的 typed error 按表映射；返回 nil 時先做 final Ref 驗收與結果落地。成功鎖定前再檢查 context cause／monotonic run deadline。非 run-fatal 的 step error 可由 CollectAll 或顯式流程處理，不強迫 run 失敗。普通 callback 的 bare context.Canceled 若無已知 cause，記 Origin=Definition、Failed / 1，不偽稱 hub 或使用者取消。

不在 error 裡附一個泛用 `Retryable=true` 讓 engine 自動重試。Pi 0.84.3 的 `finalError="Retry cancelled"` 只是版本相容 parser 的輸入；runtime 先轉成 typed Cancelled，其餘 engine 不匹配該字串。

### 7.3 工程預設

全部寫在程式碼的 Policy，workflow 可在編譯時指定其他正值，不增加 CLI flags。`RunPolicy.DisableRunTimeout=true` 明確停用 Controller 自有 run deadline；所有 duration（包括此時未使用的 RunTimeout）仍須正值，預設不變。Parent context 的 deadline／取消、attempt／startup／RPC／cleanup 期限及資源 hard caps 仍生效，不代表無限執行。StepSpec.Timeout=0 表示使用 Policy.AttemptTimeout，負值為 InvalidDefinition；effective deadline 取 step 與仍啟用的 run／parent deadline 較早者。Role 的 provider/model/thinking 必須明確指定，不接受空值繼承。

| 項目 | 預設值 | 起算／耗盡語義 |
|---|---:|---|
| Startup | 120 秒 | Spawn 到 RPC readiness 與 discovery |
| 一般 RPC response | 10 秒 | 完整寫入 command 後；超時使 handle 不可安全重用 |
| Prompt ack | 120 秒 | 包含 Pi preflight/auth/compaction；仍受 attempt deadline 限制 |
| Ack 後 prompt observation | 30 秒 | Ack 後仍未觀察到 token，失敗且不重送 |
| Attempt | 30 分鐘 | Preparing 開始，包含 session 等待、provider retry、驗證 |
| Run | 4 小時 | RunCreated 起，包括 session 初始化；cleanup 另計 |
| Abort grace | 5 秒 | 等 abort／abort_bash 完成，不能無限等待 |
| Cleanup | 全 run 15 秒 | 獨立 context，各 handle 平行清理 |
| Health probe | 每 5 秒 | 避免重疊 probe；已有正常 response 可更新健康 |
| 同時存活 session | 8 | 超限明確失敗，不等待可能永不釋放的 slot |
| 每 run 總 session / attempt | 64 / 256 | 防止程式化 loop 失控 |
| 每 RPC frame | 32 MiB | 超限 protocol failure，不截斷後繼續 parse |
| 使用者 Prompt | 64 KiB | 非空單行 UTF-8；拒絕 CR/LF/NUL |
| Candidate contract | 1 MiB | 原始 bytes 上限；只接受一個 JSON value |
| Candidate JSON depth | 64 | 超限不交給 schema validator 遞迴處理 |
| 每份引用檔案 / 每 attempt 引用總量 | 64 / 256 MiB | 超限不發布 |
| 每 attempt 引用數量 | 128 | 防止無界 manifest |
| Core journal / runtime observation queue | 100 MiB / 1024 筆 | 耗盡停止 run，不靜默丟核心事件 |
| Stderr | 每 session 8 MiB，另留 64 KiB tail | 達上限仍 drain，記錄 truncation，不阻塞 child |

不將 agent tool 寫入的所有檔案計入可保證的 quota；只有 Controller 經手的檔案有上述限制。進階 disk sandbox／OS quota 不在本版。

### 7.4 Finalizing 與持久化失敗

順序固定如下，不把業務結果、持久化與 process 清理混成同一個成功布林值：

1. Workflow 返回並 join 後停止接受新的 workflow API 呼叫。若尚無 root stop cause 且返回 nil，透過統一 resolver 驗收 Outputs／Final，以 producer attempt 建立 FinalDelivery，將 `run_id`、`outputs`、可選 `final` 寫入 `result.json`（temp/Sync/rename），仍計入 run deadline。結果 I/O 失敗登記 StorageFailed，不能鎖定 Succeeded。普通 workflow error 可僅解析選定 Final 以交付 incomplete report；解析錯誤保留原 error 並合併診斷，root cancellation/deadline/fatal 或 callback fatal 則不額外解析。
2. Engine 序列上依第 7.2 節仲裁一次並鎖定 workflow outcome，進入 Finalizing，嘗試 append+Sync RunFinalizing。取消與 cleanup 的啟動不以該次 I/O 返回為前提：鎖定後立即使用獨立期限啟動資源清理，再嘗試此持久化，避免停滯的 journal 阻止 Close。結果事件與 cleanup report 仍按以下順序收束。若結果已寫入但取消先於鎖定生效，該檔案只保留為產物清單，不代表成功，無須改寫成另一份假結果。
3. Cleanup 以獨立 15 秒期限執行，無論先前持久化是否失敗都必須嘗試。收尾嘗試寫 cleanup.json、append+Sync RunFinished（含 cleanup outcome）、再原子更新最終 run snapshot。失敗／取消路徑的結果清單只 best-effort 寫入，不阻擋清理。
4. outcome 鎖定後任一必要 journal／snapshot／cleanup report 寫入失敗，記錄獨立 FinalizationFailed（包含實際 phase/cause）。不推翻 workflow outcome、不重跑 workflow，也不能 exit 0。RunFinished 自身若無法寫入，就沒有已持久化的 RunFinished；不得假報該事件成功。
5. Journal 不可用時，engine 仍維護 best-effort 記憶體的 EmergencyStatus，TUI／stderr 顯示 `state_persisted=false`、workflow outcome 與 finalization errors。它不是 committed snapshot、不得供流程繼續消費。完整結果可能無法保存，保留可用的診斷，不無限重試磁碟寫入。
6. Exit code 以已鎖定原因為準：Cancelled/SignalTERM=143，其他明確取消=130；其餘有 workflow failure、timeout、cleanup failure 或 finalization failure 均為 1；只有成功且上述兩類收尾均成功才為 0。所有非零收尾錯誤都要顯示，即使取消保留 130/143。

所有 workflow API 的 journal/snapshot I/O failure 在 Finalizing 前都是 run-fatal。已鎖定後的 failure 則只走 finalization error channel；不能再依一般「journal error 取消 run」規則改變已決定的 outcome。

## 8. Contract、資料佈局與完整性

Store 建構時將可信 `BaseDir` 建立後以 `EvalSymlinks` 正規化為 canonical root，避免合法 WIP symlink／平台 temp alias 造成 request、Ref 與 renderer 路徑不一致。`LaunchCWD` 保留原值；此入口正規化不放寬 attempt／artifact 的 no-follow、regular-file 或 rooted containment 檢核。

```text
~/WIP/<task-id>/
└── runs/<run-id>/
    ├── run.json
    ├── input.json
    ├── events.jsonl
    ├── result.json
    ├── cleanup.json
    ├── schemas/                      # registry resources 與 envelope 規格
    ├── sessions/<handle-id>/
    │   ├── owner.json
    │   ├── pi/                       # --session-dir
    │   └── stderr.log
    └── steps/<invocation-id>/
        ├── step.json                # scope path、key
        └── attempts/0001-<attempt-id>/
            ├── request.json
            ├── candidate.json
            ├── evidence/
            ├── artifacts/
            ├── validation.json
            ├── attempt.json
            └── published/
                ├── contract.json
                ├── manifest.json
                ├── evidence/
                └── artifacts/
```

不複製 Pi 對話成另一套 history；既有 session JSONL 與 hub 負責對話回看。本版選擇持久化 session 模式、保留 Pi 自己的寫檔責任，不新增逐 entry 磁碟落地校驗作為 dispatch／Step 成功條件；get_entries 不作 durability 證據。真實 Pi gate 必須檢查正常結束後 history 保留，仍不保證 Pi 或 OS 異常時沒有歷史遺失。Core journal 不保存 token streaming／thinking delta。`input.json` 保留原始 Prompt，`run.json` 記錄 workflow/controller/Pi 版本、policy、launch cwd 與時間，不保存 auth 或完整環境變數。

### 8.1 JSON envelope

```json
{
  "meta": {
    "version": 1,
    "run_id": "...",
    "invocation_id": "...",
    "attempt_id": "...",
    "dispatch_token": "...",
    "schema_id": "example.output.v1"
  },
  "data": {},
  "files": [
    {"id": "source-1", "kind": "evidence", "path": "evidence/source.txt"}
  ]
}
```

- Framework 驗證 meta/files；workflow schema 只驗證 `data`。不要把某個 workflow 的 status/pass/reject 寫入框架 envelope。
- `data` 需要引用檔案時使用 manifest file ID；下游從已發布 contract 所在位置解析相對路徑。
- 引用過往 step 的 contract 透過 request.inputs 的 Ref；本次 files 只列本次 attempt 內的 evidence/artifacts，避免把任意 host 路徑當可發布產物。
- `files.kind` 使用 `evidence`／`artifact`，路徑分別限定 `evidence/`／`artifacts/`。
- Agent 不需要計算 SHA-256。Controller 發布時生成 sidecar manifest（隨 published 目錄原子提交），記錄各檔案 digest/size；Ref 的 SHA256 指向原始 contract bytes，ManifestSHA256 錨定 manifest bytes。不能只檢查可能一起被修改的 artifact 與 manifest。
- 身分欄位、schema ID 必須與 request 精確相等。不存在「檔案名稱正確就接受」。

### 8.2 檢核與發布

1. Exclusive 建立 attempt 目錄及 request；確定 candidate 尚不存在。目錄 0700、Controller 檔案 0600。
2. Candidate 必須是一般檔案、合法 UTF-8、唯一 JSON value；拒絕 duplicate keys、trailing JSON、超出大小／深度限制。
3. 驗證 envelope、identity，再以 JSON Schema Draft 2020-12 驗證 `data`。使用 json.Number 解碼避免 float64 精度丟失。
4. Schema 由 `go:embed` 或 Go 常數註冊，啟動時 compile；schema ID 不重複，全部 `$ref` 只能指向已註冊 resource。禁用 network/filesystem fallback loader，明確啟用 format assertion。Compile 前拒絕目前 validator 無法忠實表示的 schema numeric bounds、counts 及 const／enum 數值，避免規則被靜默忽略或反轉；validator panic 留在邊界轉成錯誤，不改用 float64。這不解決 candidate 的全部極端數值語義／資源問題，限制見第 12 節。Engine 將同一 registry 的 envelope 規格、output schema 及所有必要 resource 寫入 run.schemas；request.output 帶 schema ID、路徑、resource URI→path 對照與 digest。Pi 能直接讀到實際驗證規格，workflow 不在 Prompt 手抄另一份 schema。Controller 驗證仍使用 binary 內 registry，不信任落地 schema 的修改。
5. Manifest ID 唯一；路徑必須相對於 attempt，限定 evidence/artifacts，拒絕 `..`、absolute path、symlink、非一般檔案與重複目標。以 rooted filesystem operation 防止檢查後改路徑逃逸；不只做字串 prefix 比對。
6. 在同 filesystem 建私有 staging directory，複製當前 bytes，驗證拷貝與來源讀取的穩定性，為 staged files 計算 digest。發現讀取期間改變即失敗；不宣稱可對抗同 UID 惡意改寫。
7. 寫 validation report；經 runtime Confirm 後，將 staging 目錄原子 rename 成 published。不得覆蓋既有 published；新 attempt 永遠用新位置。在 macOS 使用 rooted parent descriptors 與 `renameatx_np(RENAME_EXCL)`，連空目錄也不覆蓋。Manifest 讀取上限使用 Controller 保存的實際生成長度，避免 JSON escaping 膨脹與 policy 算式溢位。
8. Journal append+Sync AttemptSucceeded（含完整 Ref）與 provisional invocation 狀態，完成 run snapshot 及 attempt terminal snapshot 等必要持久化後，才在 engine 序列內登記 committed publication registry 並回傳 Ref；invocation 定案遵循第 7.1 節。Publish rename 不等於 commit。若 publish 後 journal／必要 snapshot 失敗，不登記 membership，產物只留診斷，run 失敗且不得向下游派送。
9. 所有消費路徑使用第 8.3 節的統一 resolver；只有 digest 正確仍不夠，必須是本 run 已提交的發布紀錄。

此流程保證 Controller 消費的是具體版本，不保證 Pi 已完成外部業務副作用、候選內容語意正確或 host filesystem 對 agent 唯讀。mtime 不是新舊產物判定依據。

### 8.3 Ref resolver 與發布來源

Engine 為每個 run 維護 committed publication registry，key 為 AttemptID；value 保存完整 Ref、producer InvocationID、dispatch identity 與 journal seq。只由第 8.2 節提交成功的路徑登記，不掃描目錄推導 membership；不提供從檔案重建 registry 的 crash resume。

統一 resolver 依序：

1. Ref.RunID 必須為當前 RunID；AttemptID 必須已在 registry，且 Ref 全欄位與記錄相同。跨 run import 不在本版，不能自行把 agent 回傳的 JSON Ref 當授權發布。
2. Path 必須是 registry 對應 attempt 的 canonical published/contract.json，位於當前 store root；不能指向 candidate、staging、未提交 published 或任意 host 路徑。
3. Store.Read 核對 envelope run/invocation/attempt/token、schema ID 與 registry 的已知身分，再驗證 schema、contract/manifest digest、引用檔案 digest。
4. 回傳該次讀取並驗證的固定 bytes；`engine.Decode[T]` 解碼其中的 data，不在驗證後再重讀可能改變的檔案。

Step.Inputs、Feedback.Refs、Decision refs、engine.Decode／ReadContract、RetryAction 的成功 Result、Parallel branch Result、workflow 最終 Result 與 FinalSelection 都使用此入口。Feedback.SourceAttemptID 若非空，必須是本 run 已知 attempt（可失敗而沒有合法 Ref）；Message 必須非空，Controller-origin feedback 可只有 SourceCode／attempt 而無 Ref。Registry 只允許先前已提交的輸出，不能建立循環或 forward reference。實作者不可為上述不同入口各寫一套較寬鬆的 digest-only check。

## 9. Process 清理與 discovery

### 9.1 正常可處理的退出

1. 按第 7.4 節完成結果驗收／必要落地及 outcome 仲裁，設 Finalizing 並禁止新 dispatch。取消所有 scope；開始獨立 cleanup deadline，不先無限等待 join。
2. 用獨立 cleanup context 停止正在進行的 Pi 操作，best-effort 發 `abort`、`abort_bash`。讓等待中的 Step 隨 context 解除，並與下面的 process 清理並行收束所有已啟動工作。不把 abort ack 當所有 process 已退出。
3. 對仍由本次 runtime 持有的 Pi process group 發 SIGKILL；不用 `pkill pi`，不對 hub 回傳的父 pid 發 signal。
4. 每個 `exec.Cmd` 只有一個 owner 執行 Wait；bounded 等待 Pi 退出。確認退出後先做第 5 步的 discovery 清理，再收束 pipe、reader/probe 與 observer，避免非必要 drain 阻擋主要清理。Expected SIGKILL 的 exit status 不是新 workflow failure。
5. Wait 確認 Pi 已停止後，優先清理本次 session 的 discovery，不等 observer drain 耗盡 cleanup deadline。核對 `sessionId + piPid + sessionFile + parent pid` ownership，不刪其他 session 或掃空 discovery 目錄。Observer drain 使用剩餘 cleanup budget 的四分之一，之後取消並 bounded join；未交付／未停止需明列未確認。
6. 依第 7.4 節寫 cleanup.json、RunFinished 與 snapshot；寫入失敗也必須給 best-effort FinalizationFailed 狀態與非零 exit。Contract、evidence、session history 不刪除。

讀取／刪除 discovery 失敗、ownership 不符或已有異常 `.recovering` claim，記錄 CleanupFailed／未確認原因，不殺可能已由其他 owner 建立的替代 process。Startup 未取得 SessionID 時，只能以本次 child PID、parent PID 及 run 專用 session dir 的組合識別自有 discovery；無法確認則留下警告，不能猜。

### 9.2 為什麼先 Wait 再刪 discovery

目前 WebUI discovery 的 `pid` 是 Controller 父 PID、`piPid` 是 Pi PID。Hub 以 piPid 判活，但 extension startup recovery 以父 pid 判活。Controller 在清理時仍存活，正常情況不會因 Pi 先死就觸發 recovery；Wait 後刪檔也避免活 Pi 再次 writeDiscovery 蓋回檔案。

這只適用核對過的版本與直接 spawn 映射。沒有持久 tombstone，也沒有跨 process 原子撤銷／kill 保證。Controller 自身被 SIGKILL 的殘留已由產品範圍接受。

另外，啟動任意持久化 Pi 本身會觸發 extension 對共享 discovery 目錄的 recovery，可能影響非本次 run 的 stale session。這是既有 extension 行為，不是 Controller 清理。每次啟動／resume 前唯讀核對 deployed recovery 判準及共享 discovery：檢查父 `pid` 而非只看 `piPid`；存在 `.recovering` 或可能 delete/resume 他人 stale session 時停止，不代清理。無 truthy pid 的 hub-state 等 JSON 僅依實際 recovery 規則略過。Preflight 不是鎖，不因使用專用 task 目錄就宣稱 discovery 隔離。受控執行方法見 [VERIFICATION](docs/VERIFICATION.md)。

### 9.3 子 process 的保證界線

Pi 0.84.3 的內建 bash 在 Unix 使用 `detached: true`，bash process group 不一定與 Pi 相同。因此不能宣稱 kill Pi PG 就殺光所有工具子孫。

- 活躍內建 bash 的 abort handler 會 kill 自己的 process tree，所以先 bounded abort，再 SIGKILL Pi。
- 直接 RPC bash 另用 abort_bash；一般 Controller 工作不主動使用此 command。
- Pi 0.84.3 的 RPC abort 只呼叫 agent/retry abort，不呼叫獨立的 abortCompaction。Summary 正在執行時，不能把 abort ack 視為 compaction 已停止；5 秒 grace 後仍以自有 process group 的 SIGKILL／Wait 收尾，報告保留 abort 未確認狀態。此限制不新增正式 extension 或 cancellation API；測試中的 compaction aborted event 由既有 session_before_compact cancellation hook 觸發，見 [VERIFICATION](docs/VERIFICATION.md)。
- Extensions 自行 daemonize、已脫離祖先的背景工作、或 Pi 無回應使 abort 失效，不提供完整子孫清理保證。安全優先，不用全機 process name 掃殺。
- 必須驗證活躍內建 bash 的取消；無法確認工具終止時 cleanup report 不能聲稱 complete。
- Hub kill-session 可能針對 discovery 的父 group 送 signal；本 Controller 不呼叫該 API。使用者從 hub kill 的影響依既有 hub 行為，不能在不修改 hub 的前提下宣稱其一定只殺單一 child。

### 9.4 自有 workspace／resource cleanup

`Run.AddCleanup(ctx, name, close)` 只註冊 run-owned resource callback，不是通用 command executor。名字唯一、callback 非空，須在資源暴露給 Pi 前註冊；acquisition 部分失敗或註冊失敗時 ownership 仍在 caller，caller 負責 bounded cleanup，不能假定 engine 已接管。成功註冊後由 engine 以反向註冊順序收尾，保留各 callback error。

Root stop／outcome lock 後的 Pi cleanup 不等待 workflow callback 或 journal 返回；resource callback 則等待 workflow join 及自有 Pi 確認 exit。等待超出 cleanup budget 或 process exit unconfirmed 時保留資源並回報，不刪仍在使用的 workspace。未確認退出的 handle 仍占 live slot，不能靠 Close 已呼叫或 deadline 過期釋放容量。

Checkout cleanup 只能刪原始 ownership 匹配的自有 checkout／worktree registration；private Git objects、snapshots、contracts、artifacts、embedded resources 與 history 保留。確認 `ProcessExited && WaitCompleted` 後，formatter 才顯示可在安全預檢後 resume；history 路徑仍只是 runtime metadata，不新增存在或 durability 保證。

## 10. 事件、持久化與 TUI

- Engine 單一序列提交 run/step/attempt 狀態；runtime observation 帶各 session 的 seq，engine journal 另有全 run seq。不以跨 process timestamp 決定先後。
- Core event 欄位：version、seq、UTC timestamp、run ID、可選 scope/invocation/attempt/handle ID、kind、structured details。
- 事件類型覆蓋 RunStarted/Finalizing/Finished、SessionReady/HealthChanged/Closed、AttemptStarted/Dispatched/Settled/ValidationFailed、AttemptSucceeded/Failed/Cancelled/TimedOut、InvocationUpdated/Finished、RetryScheduled、Decision、GroupStarted/Joined、Feedback、CleanupFailed。
- Core event append 成功後才更新對外 committed state。Run/attempt snapshot 以 temp file + rename 寫入；snapshot 包含 last seq。Journal 故障後唯讀顯示用的 EmergencyStatus 是第 7.4 節明定的例外，必須標示未持久化，不可用它繼續推進 workflow。
- Journal 保留可診斷的順序，不是 crash replay engine。必要交接點 Sync；不承諾斷電後每筆 token/event 都存在。
- Snapshot 與 journal 可以用來看過去狀態，但 Controller 重啟不能 resume workflow。
- Runtime callback 只投遞有界 queue，飽和時明確 fail run；token delta 不進 core queue。Fatal ingress 與 outcome lock 使用不受 journal I/O 阻塞的共同仲裁 latch；不等 consumer 排程才登記 root cause。Runtime 的 Dispatching／DispatchAccepted observation 帶本次 token，只有 handle＋token 匹配的活動 attempt 才更新派送狀態，不以任意 agent_start 推定 acceptance。Finalizing 前 journal 無法寫入時停止派工並以 JournalFailed 終結；outcome 已鎖定則記 FinalizationFailed，不回頭取消已完成 workflow。兩者均保留 stderr/EmergencyStatus 的 best-effort 診斷。
- TUI 訂閱 coalesced snapshot notification，慢 consumer 可丟 notification 並重讀最新 snapshot，不丟核心狀態。
- TUI 每 100 ms 更新 spinner／耗時；耗時使用 monotonic clock，UTC 只作記錄。
- 每個平行 step 獨立顯示 scope/key、attempt、retry budget、session/model、狀態、spinner 及耗時；動畫共用 100 ms 時脈，依各 invocation 狀態啟停。Invocation snapshot 的 `RetryActivationIDs` 複製當次 scope ancestry，不依路徑推測 repeated activation；attempt 的 `Feedback` 保存 Preparing 時的值與 Refs 副本。兩者沿既有 commit 路徑提供顯示 metadata，不授權 Ref 或繞過 resolver。Feedback 顯示最多 240 Unicode rune 的安全摘要（含截短提示），全文留 request/event；此上限不是 input quota 或全文處理成本上限。
- Health 同時顯示 Online/Offline/Unresponsive 文字與色彩，不能只靠顏色。Rendering 移除外部文字的 terminal control sequence，避免 Prompt/feedback 注入終端操作。
- `q`／Ctrl+C 取消整個 run 並等待清理，不是 detach。完成後還原終端，輸出 outcome、產物及 run 路徑。
- 無 TTY 時使用相同 engine 的純文字狀態輸出，不要求額外 flag，也不為測試建立第二套 engine。只有 stdin／stdout 都是 TTY 才啟用 TUI，不讀 stdin Prompt。
- Terminal adapter 擁有 raw mode／restore，使用鎖定的 UV 單一 reader；Bubble Tea 使用 `WithInput(nil)`。獨立 input consumer 先直接呼叫 `Run.Cancel`，再經容量 64 的 presentation queue 送 UI；queue 滿只丟顯示輸入，不丟取消。FD-preserving writer 攔截輸出錯誤，不依賴 Bubble Tea v2.0.7 的 renderer error 傳遞。SIGPIPE 不可跳過 cleanup。
- Input 使用同一 UV parser 區分 key、paste 與 terminal replies，不掃原始 bytes 猜 q。Scanner、持續 drain 的 consumer、可能等待 `Program.Send` 的 sender 分工保留；UV event send 不一定選 context，consumer 必須 drain 到 scanner 結束才能 join。最終 Report 才表示收尾資料齊全，terminal Snapshot 不是 Store 關閉／finalization 完成證明。
- Terminal I/O error 會由 Controller 明確取消活動 run；若 outcome 已鎖定則不改寫，但輸出／restore failure 仍不能 exit 0。最終 stdout report 失敗時 best-effort fallback stderr。正常收尾完整 join input scanner／consumer／sender、嘗試 restore，再輸出 Report。永久堵塞的 stdout 可能延後 CLI 返回與 restore，但不能阻擋取消登記或 engine cleanup；不承諾固定 wall-clock 退出上限。

## 11. CLI、相依版本與交付

- `run` 要求 workflow name 與一個引號包住的單行 Prompt；不隱含選第一個 workflow，不讀任務 JSON／stdin prompt，不加入 task naming option。
- CLI grammar 與 registry/schema preflight 在建立 task dir、啟動 Pi 前執行。未知 workflow、空 Prompt 或 invalid definition exit 2；不產生假 run。
- 認證沿用 Pi，Controller 不管理 token。Provider secrets 不寫 journal／argv。
- Exit：0=workflow、必要持久化與清理全部成功；1=失敗、timeout 或非取消結果的收尾失敗；2=run 建立前的用法／preflight 錯誤；130=Controller 或 propagated external agent 的明確取消／SIGINT；143=SIGTERM。優先順序與 cause mapping 依第 7.2／7.4 節，即使取消仍顯示所有收尾錯誤。
- Module：`pi-workflow-controller`；目標 macOS arm64，Go 1.25.0。
- 主要依賴：`charm.land/bubbletea/v2 v2.0.7`、`charm.land/bubbles/v2 v2.1.0`（spinner）、`charm.land/lipgloss/v2 v2.0.4`、`github.com/santhosh-tekuri/jsonschema/v6 v6.0.2`。CLI／concurrency 優先標準庫，不為旗標解析加框架。
- 完整版本以 [go.mod](go.mod)／[go.sum](go.sum) 為準；exclusive rename 使用 x/sys，terminal 使用 UV、x/ansi、x/term、cancelreader。修改依賴須重驗相關行為，不把此清單當作已執行相容性 gate 的證據。
- 交付單一 Controller executable，不打包 Node.js、Pi 或 provider credential。macOS 的 CGO-disabled 不等於 Linux ELF 的全靜態承諾，不宣稱 binary 能在無 OS runtime 的環境執行。
- Registry 提供 `smoke-echo` 與固定 `code-review`。Smoke 依 request 寫 echo contract，以 committed Decode 精確比對、Decision 後選 Final；review 的多階段與結果語義見 [CODE-REVIEW](docs/CODE-REVIEW.md)。
- Smoke 明確綁定 `fireworks/accounts/fireworks/models/gpt-oss-120b`／thinking `low`，不繼承啟動者模型。Startup 不接受 model/thinking clamp；配置、provider 認證與 live 推論品質分別核對，不保留某次 catalog/auth 成功作為永久保證。

## 12. 已知限制與相容性邊界

### Numeric validator

目前 `jsonschema/v6` 邊界保留 `json.Number`、schema numeric bounds/counts 與巢狀 const/enum representability guards，並捕捉 validator panic。這不等於任意合法 JSON number 都能忠實判定：

- 極端 candidate 數值超出依賴的 `big.Rat` 可表示性時，部分 type/equality 路徑可能把「不能判定」當 false；經 `not` 或條件規則反轉後可能誤接受。Panic guard 不會捕捉這種非 panic 語意錯誤，重驗同一 snapshot 也不能修正。
- 可表示但極大的 exponent／多項 numeric constraints 可建立龐大 rational values 與完整 error tree；candidate byte limit 不等於 validator CPU/heap quota。Validator 公開呼叫沒有 context/fail-fast 中斷，事後截斷報告不能避免前期配置，recover 不能處理 fatal OOM。
- 這些極端範圍是保留的非阻擋性限制，不宣稱已修復、不新增 dependency fork 或 numeric quota。需要此數值範圍的 workflow 必須重新評估，不能以既有 suite 推論安全。既有 JSON/schema/identity/file quota、snapshot、typed cancellation 與 cleanup 要求不因此取消；非協作 validator 本身仍無強制 wall-clock 保證。

### I/O、資源與持久化

- 核心 observation append+Sync journal 並重寫完整 run snapshot，成本隨累積狀態增加。Journal byte limit 不限制所有 snapshot 重寫流量；沒有 checkpoint scheduler 或高吞吐量容量承諾。
- Feedback 的畫面摘要不是全文 sanitization、snapshot 記憶體或輸入 quota。永久堵塞 stdout／stderr 或 filesystem syscall 可延後 CLI 返回及 terminal restore；取消登記、RPC 與 engine cleanup 不依賴 renderer 恢復，但不能強制中斷所有 OS I/O。
- Contract 固定 bytes／dual digests／rooted filesystem 不構成同 UID 惡意寫入隔離；沒有 OS sandbox、全磁碟 quota、任意 detached 子孫清理、supervisor、exactly-once 或外部副作用 rollback。
- Controller abrupt death 可能殘留 Pi；沒有 crash resume。Journal 可診斷但不是 replay engine，Sync/rename 不承諾斷電後每個 directory entry 或 Pi history 的完整 durability。

### 相容性與證據

Pi version gate 與 runtime source 不等於 bundled executable 或 provider/hub 相容性證明。調整 Pi／WebUI 版本前，核對目標安裝版的 RPC、session persistence、skills/settings/resource loading、discovery recovery 與 process cleanup 契約，再依 [VERIFICATION](docs/VERIFICATION.md) 分組驗證。Shared hub 的磁碟版本不等於已啟動 backend 的記憶體版本，不為採證擅自重啟他人服務。未執行的 browser／平台／fuzz mutation／高吞吐測試不得宣稱通過。

真實版本、source manifests、raw logs、history、PID、日期及執行結果只存 repo 外受限位置；repository 保留通用方法、匿名可重現 tests 與上述限制，不留真實執行紀錄。
