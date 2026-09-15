# 開發與維護指南

本文件說明目前責任分工、開發路線及驗收入口，不記錄個別 session 交接或 milestone 通過狀態。需求以 [PRD](PRD.md) 為準，不變量以 [DESIGN](DESIGN.md) 為準；新增業務流程從 [ADDING-A-WORKFLOW](docs/ADDING-A-WORKFLOW.md) 開始，分組方法見 [VERIFICATION](docs/VERIFICATION.md)。

## 1. 責任分工與依賴方向

```text
cmd → workflows → engine → runtime
                       └→ contract
cmd → tui → engine 的唯讀 Snapshot／Report 及取消入口
```

Runtime／contract 不依賴 engine／TUI，所有核心 package 不依賴終端繪製。只在外部 Pi/process/provider/API 邊界使用替身；Store、schema validator、流程 helper、reducer 與業務驗收使用真實實作。

| 層級 | Source 入口 | 維護責任 |
|---|---|---|
| CLI | [main.go](cmd/pi-workflow-controller/main.go)、[terminal.go](cmd/pi-workflow-controller/terminal.go) | Grammar/preflight、registry 組裝、typed signals、TTY/plain、SIGPIPE、單一 input owner、writer interception、join/restore、Report fallback |
| Runtime | [types.go](internal/runtime/types.go)、[process.go](internal/runtime/process.go)、[rpc.go](internal/runtime/rpc.go)、[execution.go](internal/runtime/execution.go)、[cleanup.go](internal/runtime/cleanup.go) | Exact model/session binding、Execute/Confirm、增量 entries、bounded transport、process ownership、唯一 Wait、discovery/history |
| Contract | [contract.go](internal/contract/contract.go)、[store.go](internal/contract/store.go)、[schema.go](internal/contract/schema.go)、[attempt.go](internal/contract/attempt.go)、[read.go](internal/contract/read.go)、[fs.go](internal/contract/fs.go) | Canonical root、strict JSON／離線 schema、attempt identity、Stage/Publish、雙 digest、no-follow、不可覆蓋的歷史 |
| Engine | [api.go](internal/engine/api.go)、[policy.go](internal/engine/policy.go)、[scope.go](internal/engine/scope.go)、[step.go](internal/engine/step.go)、[resolve.go](internal/engine/resolve.go) | Go workflows、session lease、retry/parallel、attempt budgets、所有 Ref 入口共用 committed resolver |
| Engine 收尾 | [run.go](internal/engine/run.go)、[failure.go](internal/engine/failure.go)、[persistence.go](internal/engine/persistence.go)、[session.go](internal/engine/session.go)、[resource.go](internal/engine/resource.go)、[state.go](internal/engine/state.go) | First-root cause、fatal ingress、root/collateral provenance、outcome lock、journal/snapshot、獨立 cleanup、FinalDelivery |
| Workflow | [registry.go](internal/workflows/registry.go)、[review/workflow.go](internal/workflows/review/workflow.go) | Definitions/Resources/Schemas、smoke-echo、固定 code-review 編排與成功條件 |
| Review 業務 | [acquire.go](internal/workflows/review/acquire.go)、[check.go](internal/workflows/review/check.go)、[safety.go](internal/workflows/review/safety.go)、[resources.go](internal/workflows/review/resources.go) | Pinned acquisition、來源／需求／evidence／report 驗收、共享預檢、embedded resources、自有 checkout 清理 |
| TUI | [model.go](internal/tui/model.go)、[format.go](internal/tui/format.go)、[safe.go](internal/tui/safe.go) | Coalesced snapshot、健康／平行／retry／feedback／耗時、terminal-safe 顯示、最終 Report；不重讀 candidate |
| 測試邊界 | [protocol.go](internal/testutil/protocol/protocol.go)、[bundled.go](internal/testutil/bundled/bundled.go) | 共用外部 RPC subprocess 與真 Pi/localhost provider fixtures，只由 tests 使用 |

## 2. 開發路線

1. **讀需求與立即 callers。** 先確認更動是 workflow 業務規則、框架不變量或外部相容性；查看 exports、callers、現有 tests/fixtures 與共用 utilities，不憑概念上「無關」跳過依賴。
2. **確認契約。** 輸入、schema、來源/版本關係、成功/不足/失敗、權限、model/thinking、retry、timeout、資源清理與 FinalSelection 應明確定義。有規格或安全歧義先確認，不用模型自行選擇補空白。
3. **先保護最接近行為的 gate。** 相同行為優先擴充既有 table-driven tests；並行或 I/O 故障使用具名 scenario 及 channel/RPC/filesystem barrier，不用 sleep 猜 race。
4. **實作最小必要變更。** 優先普通 Go control flow 與 concrete types，不加入通用 command executor、plugin manager、DSL、supervisor 或新 CLI 配置。不順便修改 Pi/hub 或全域定義。
5. **同步消費端與文件。** Result／FinalSelection／FinalDelivery 變更須核對 producer、resolver、持久化、Report、formatter 與 workflow callers；不只更新一份歷史 skeleton。Canonical root 變更不可改寫原始 LaunchCWD 或放寬 attempt/artifact no-follow。
6. **分組驗證與獨立審閱。** 依下一節選 local、bundled、live、業務 E2E 及 release gates。必要的 code／scale-failure／simplicity 與 correctness 審閱不得由 tests PASS 或採證 runner 代替；未完成的審閱不列通過。
7. **交付前安全檢查。** Repository 只提交匿名方法、可重現 tests、產品配置與限制。真目標、raw output、source manifests、history、PID、日期、commit SHA 及逐次 review 結果保存在 repo 外受限位置，不能複製進開發指南。

## 3. 必須一起維護的不變量

- **Publication：** Stage 的固定 bytes → Confirm → Publish rename → journal append+Sync／必要 snapshot → committed registry。僅 Publish、digest 或檔案存在不能授權 Ref。Inputs、Feedback、Decision、Decode/ReadContract、Retry/Parallel/final 不能各建較寬鬆 resolver。
- **錯誤與資源：** Storage/Journal/run-limit sticky fatal；最外層已分類 boundary、root/collateral provenance、本次 dispatch acceptance 與 fatal ingress latch 必須保留。關閉未確認的 process 不釋 slot；partial startup 仍屬本 run ownership。
- **收尾：** Outcome 鎖定後立即啟動獨立 cleanup，不等 journal I/O。Resource callback 等 workflow join/Pi exit，acquisition/註冊失敗仍由 caller 清理。所有 cleanup/finalization/close errors 可見，StatePersisted=false 不假冒 committed state，取消 exit precedence 不被覆蓋。
- **Terminal：** Writer error interception 與獨立 input cancellation 不依賴 Tea renderer/event loop。單一 UV reader、bounded presentation queue、scanner/consumer/sender join、restore 及最終 Report 不可被簡化掉。
- **交付／保留：** FinalSelection 明確選 output key/artifact ID，FinalDelivery 由 producer attempt/runtime metadata 產生；無合法 final 就 unavailable。History、contracts、evidence/report/private Git objects 保留，不把 cleanup 當資料刪除。
- **共享環境：** 啟動 persisted Pi 前核對 parent pid／recovering；preflight 非鎖，task 非 discovery 隔離。不得清理他人 session。

## 4. 驗收組別與常駐 tests

完整情境、命令、安全前提與 gate 判定在 [VERIFICATION](docs/VERIFICATION.md)。以下是 source 入口，不是執行結果。

| 組別 | Tests 入口 | 核對重點 |
|---|---|---|
| Runtime／transport | [runtime_test.go](internal/runtime/runtime_test.go)、[transport_test.go](internal/runtime/transport_test.go)、[codec_test.go](internal/runtime/codec_test.go) | Ack/token/settled、sticky retry/compaction、typed causes、增量 cursor/backlog、framing、readiness、ownership/Wait |
| Store／schema | [contract_test.go](internal/contract/contract_test.go)、[schema_test.go](internal/contract/schema_test.go)、[json_test.go](internal/contract/json_test.go) | 真 temp filesystem、strict JSON、numeric guards、offline refs、snapshot/tamper/identity/limits、exclusive publication |
| Engine／persistence | [engine_test.go](internal/engine/engine_test.go)、[persistence_test.go](internal/engine/persistence_test.go)、[deadline_test.go](internal/engine/deadline_test.go)、[protocol_test.go](internal/engine/protocol_test.go)、[resource_test.go](internal/engine/resource_test.go) | 順序/parallel/retry、全部 Ref 入口、provenance/fatal、blocked journal、outcome lock、slot、FinalDelivery、AddCleanup |
| CLI／TUI | [main_test.go](cmd/pi-workflow-controller/main_test.go)、[protocol_test.go](cmd/pi-workflow-controller/protocol_test.go)、[protocol_darwin_test.go](cmd/pi-workflow-controller/protocol_darwin_test.go)、[model_test.go](internal/tui/model_test.go)、[format_test.go](internal/tui/format_test.go)、[safe_test.go](internal/tui/safe_test.go) | 真 RPC subprocess/macOS PTY、堵塞與壞輸出、取消/termios、全部 warnings、producer 定位、安全文字 |
| Registry／review | [registry_test.go](internal/workflows/registry_test.go)、[workflow_test.go](internal/workflows/review/workflow_test.go)、[acquire_test.go](internal/workflows/review/acquire_test.go)、[check_test.go](internal/workflows/review/check_test.go)、[report_test.go](internal/workflows/review/report_test.go)、[resources_test.go](internal/workflows/review/resources_test.go)、[safety_test.go](internal/workflows/review/safety_test.go) | 固定必要角色、pinned Git／來源／requirements、incomplete/limited、renderer、embed 異 cwd、共享預檢與清理 |
| Bundled Pi | [runtime/bundled_test.go](internal/runtime/bundled_test.go)、[engine/bundled_test.go](internal/engine/bundled_test.go)、[registry_test.go](internal/workflows/registry_test.go) | 真 Pi executable/extension + localhost provider，不能用 RPC substitute 代稱 |
| Live／hub | [live_test.go](internal/engine/live_test.go)；另按 workflow 做獲授權 E2E | 真 provider、exact binding、native bash／cancel／真正 Step timeout、history／recovery、只看自有 hub sessions |

[example_test.go](internal/engine/example_test.go) 的 `ExampleWorkflow` 驗 registry 並由 compiler 檢查 closure API，不 Execute 或啟動 Pi；`ExampleDecode` 只編譯。這些不是 live workflow gate。

## 5. 測試與交付紀律

- 每個 test 自己啟動的 subprocess 都須獨立 cleanup 保險；assertion 提早失敗也不能殘留 fixture process。Test-only lifeline 不得變成產品 supervisor 宣稱。
- Filesystem failure 使用 temp 目錄與 OS 邊界，保留 sentinel 不被誤刪；權限型案例須注意 root 可能使條件無法成立。不要 mock 內部 write helper。
- 任一 necessary gate 無法觸發、認證不可用或環境不安全，記 blocked／skip／未執行與原因；不以其他 workflow、parent PASS、compile-only example、helper guard 或 fuzz seed 代替。
- Live tests 有費用及共享 recovery 風險，不預設啟用，不與其他 shared-discovery 工作平行；不為取得版本或證據重啟他人 hub。
- Numeric validator 極端範圍、snapshot I/O 放大、永久 blocked I/O、任意 detached 子孫及非 sandbox/crash-resume/exactly-once/rollback 等限制維持 [DESIGN](DESIGN.md) 的原界線，不把未量測的容量寫成保證。
- Commit/push/發布仍需明確授權。允許提交可重現匿名 regression，不提交本機執行／review 報告或真實來源；對外分享前按發布 scan 方法檢查內容與歷史。
