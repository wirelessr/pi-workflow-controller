package codechange

import (
	"context"

	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/workflows/review"
)

// deepReviewName is both the deep review's scope under root and the name
// review.Run derives its run-directory entries and cleanup from. Its outputs
// join this workflow's under the same prefix.
const deepReviewName = "deep-review"

// deepReview runs the code-review flow on the pull request the committed
// pull-request record verified, pinned to the head its receipt reported,
// never to the newer head GitHub may show. Every session this workflow
// opened is already confirmed closed, so the review's three reviewers fit
// the run's three live sessions.
//
// A complete or limited report is the Final with a nil error. An incomplete
// report is the Final with the review's error. Without a report the pull
// request record is the Final with the review's error: up to three live
// review sessions this workflow cannot close may remain, so it opens nothing
// more and the run ends.
func deepReview(ctx context.Context, r *engine.Run, published engine.Result) (engine.Result, error) {
	outputs := published.Outputs
	failed := engine.Result{Outputs: outputs}
	recorded, ok := outputs["pull-request"]
	if ok {
		failed.Final = &engine.FinalSelection{Output: "pull-request"}
	}
	pr, err := engine.Decode[pullRequestRecord](ctx, r, recorded)
	if err != nil {
		return failed, err
	}
	scope, err := r.Root().Child(deepReviewName)
	if err != nil {
		return failed, err
	}
	res, err := review.Run(ctx, r, scope, deepReviewName, pr.URL, pr.ReportedHead)
	for key, ref := range res.Outputs {
		outputs[deepReviewName+"-"+key] = ref
	}
	if res.Final == nil {
		return failed, err
	}
	return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: deepReviewName + "-" + res.Final.Output, FileID: res.Final.FileID}}, err
}
