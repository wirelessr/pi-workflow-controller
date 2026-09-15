# 收尾產出與最終 session 定位

本文件定義交付 metadata 的設計，不保存實測或 review 報告。Source： [engine/api.go](../internal/engine/api.go)、[engine/state.go](../internal/engine/state.go)、[engine/resolve.go](../internal/engine/resolve.go)、[engine/run.go](../internal/engine/run.go)、[tui/format.go](../internal/tui/format.go)。

## 1. Workflow 選擇與 engine 解析

```go
type Result struct {
    Outputs map[string]contract.Ref
    Final   *FinalSelection
}

type FinalSelection struct {
    Output string `json:"output"`
    FileID string `json:"file_id,omitempty"`
}

type FinalDelivery struct {
    Output       string       `json:"output"`
    Ref          contract.Ref `json:"ref"`
    ArtifactPath string       `json:"artifact_path,omitempty"`
    HandleID     string       `json:"handle_id"`
    Scope        string       `json:"scope"`
    Step         string       `json:"step"`
}
```

- Workflow 用 `Result.Final` 選擇 `Outputs` 中的一個 key，及可選 artifact file ID。`code-review` 選已通過 Validation/Decision 的 report；`smoke-echo` 選 echo contract，不憑空附上 Markdown path。
- `FileID` 是已驗 envelope 的 `files[].id`，不是路徑。只接受 `kind=artifact`；不存在的 output key、未知 file ID 或 evidence file 皆拒絕。
- Final Result、Retry Result、Parallel Result 都透過同一 committed resolver 檢核。Publish rename 不是 commit，不能從 mutable candidate、latest publication、digest-only 檢查或跨 run Ref 取得授權。
- `FinalDelivery` 的 handle/scope/step 來自 producer attempt，Session ID/file/role 由 Controller 的 session snapshot 取得，不要求模型填 runtime 身分，也不按 map 順序、session 建立／關閉時間推測最後節點。
- 需要 envelope files 映射的 workflow 使用 `engine.ReadContract`；它與 `engine.Decode` 共用 resolver，取得本次驗證的固定 bytes，不另讀 mutable JSON。

## 2. 持久化與 Report

Engine 在 Store 關閉前建立定位資訊，寫入 `result.json.final`，並交給 `Report.Final`。`result.json` 同時有 `run_id` 與 `outputs`，`final` 可省略；沒有合法 Final 就明示 unavailable，不拿某個 reviewer 代替。

`final.handle_id` 對應 `run.json.sessions`，其中保存 runtime identity。Formatter 只讀 Report，不新增收尾 filesystem I/O、不重開 Store；history file 路徑是 runtime metadata，不保證該檔仍存在或已 durable。

這些資訊是 **cleanup 前驗證的定位 metadata**，不是新的 publication capability。`result.json` 路徑／內容或 `FinalDelivery` 存在都不是 outcome 成功或持久化成功證明。

## 3. 失敗、取消與清理

- 成功返回時，engine 驗收所有 Outputs／Final，必要 result I/O 成功後才參與 outcome 鎖定。StorageFailed／JournalFailed／run-limit 仍是 sticky fatal。
- 普通 workflow failure 若明確選擇合法 incomplete report，可以提供交付入口，但保留原始 error、outcome 與非零 exit。若 final 解析失敗，保留原 error 並合併解析錯誤，不為了顯示而假造 report。
- Root cancellation/deadline/fatal 或 callback fatal 不額外解析 final metadata；若取消或持久化錯誤發生在解析之後，已有 metadata 仍只表示先前驗證點，不覆蓋失敗 outcome。
- Outcome 鎖定後立即啟動獨立 cleanup，不等 journal I/O；failed/cancelled 結果清單只 best-effort 寫入。所有 cleanup/finalization/close errors、`StatePersisted=false` 與取消 exit precedence 保留，無法落地的事件不得假稱已持久化。
- Formatter 只有在相同 handle 的 cleanup `ProcessExited && WaitCompleted` 都確認時顯示 Closed；否則顯示 unconfirmed 並提醒不要 resume，所有原有 warnings 仍列出。
- `Run.AddCleanup` 的 workspace callback 等 workflow join／Pi 確認 exit，未確認時保留資源；acquisition 部分失敗或註冊失敗仍由 caller 負責 bounded cleanup。Contracts、artifacts、private Git objects 與 Pi history 不因 process cleanup 被刪除。

## 4. 收取與 resume 邊界

無須保留 Pi process 才能收結果。Workflow 結束後若要延續對話，先處理 unconfirmed cleanup，再重新唯讀核對 deployed recovery、共享 discovery 的父 `pid` 與 `.recovering`；preflight 不是鎖，run 目錄不是 discovery 隔離。以 Report 的 session file 恢復對話，不能拿 contract JSON 代替。

Resume/fork 是新的人工 Pi process，不恢復 Controller workflow state、不重新派工或更改原始 outcome。需要保留原 history 不變可 fork；code-review checkout 已清理，重新查 code 須另備相同 pinned revision 的自有 checkout。操作見 [README](../README.md)，驗證方法見 [VERIFICATION](VERIFICATION.md)。

永久堵塞 stdout/filesystem 不保證固定返回期限；任意 detached 子孫、sandbox、supervisor、crash resume、exactly-once 與外部副作用 rollback 均非承諾。此設計不改變 [DESIGN](../DESIGN.md) 的原有保證與限制。
