// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"github.com/pingcap/tidb/util/bitmap"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
)

var (
	_ Executor = &TableReaderExecutor{}
	_ Executor = &IndexReaderExecutor{}
	_ Executor = &IndexLookUpExecutor{}
	_ Executor = &IndexMergeLookUpExecutor{}
)

// LookupTableTaskChannelSize represents the channel size of the index double read taskChan.
var LookupTableTaskChannelSize int32 = 50

// lookupTableTask is created from a partial result of an index request which
// contains the handles in those index keys.
type lookupTableTask struct {
	handles []int64
	rowIdx  []int // rowIdx represents the handle index for every row. Only used when keep order.
	rows    []chunk.Row
	cursor  int

	doneCh chan error

	// indexOrder map is used to save the original index order for the handles.
	// Without this map, the original index order might be lost.
	// The handles fetched from index is originally ordered by index, but we need handles to be ordered by itself
	// to do table request.
	indexOrder map[int64]int

	// memUsage records the memory usage of this task calculated by table worker.
	// memTracker is used to release memUsage after task is done and unused.
	//
	// The sequence of function calls are:
	//   1. calculate task.memUsage.
	//   2. task.memTracker = tableWorker.memTracker
	//   3. task.memTracker.Consume(task.memUsage)
	//   4. task.memTracker.Consume(-task.memUsage)
	//
	// Step 1~3 are completed in "tableWorker.executeTask".
	// Step 4   is  completed in "IndexLookUpExecutor.Next".
	memUsage   int64
	memTracker *memory.Tracker
}

//type fetchHandleTask struct {
//	handles []int64
//	doneCh chan error
//	// bellow, maybe use??
//	rowIdx  []int
//	indexOrder map[int64]int
//}

func (task *lookupTableTask) Len() int {
	return len(task.rows)
}

func (task *lookupTableTask) Less(i, j int) bool {
	return task.rowIdx[i] < task.rowIdx[j]
}

func (task *lookupTableTask) Swap(i, j int) {
	task.rowIdx[i], task.rowIdx[j] = task.rowIdx[j], task.rowIdx[i]
	task.rows[i], task.rows[j] = task.rows[j], task.rows[i]
}

// Closeable is a interface for closeable structures.
type Closeable interface {
	// Close closes the object.
	Close() error
}

// closeAll closes all objects even if an object returns an error.
// If multiple objects returns error, the first error will be returned.
func closeAll(objs ...Closeable) error {
	var err error
	for _, obj := range objs {
		if obj != nil {
			err1 := obj.Close()
			if err == nil && err1 != nil {
				err = err1
			}
		}
	}
	return errors.Trace(err)
}

// statementContextToFlags converts StatementContext to tipb.SelectRequest.Flags.
func statementContextToFlags(sc *stmtctx.StatementContext) uint64 {
	var flags uint64
	if sc.InInsertStmt {
		flags |= model.FlagInInsertStmt
	} else if sc.InUpdateStmt || sc.InDeleteStmt {
		flags |= model.FlagInUpdateOrDeleteStmt
	} else if sc.InSelectStmt {
		flags |= model.FlagInSelectStmt
	}
	if sc.IgnoreTruncate {
		flags |= model.FlagIgnoreTruncate
	} else if sc.TruncateAsWarning {
		flags |= model.FlagTruncateAsWarning
	}
	if sc.OverflowAsWarning {
		flags |= model.FlagOverflowAsWarning
	}
	if sc.IgnoreZeroInDate {
		flags |= model.FlagIgnoreZeroInDate
	}
	if sc.DividedByZeroAsWarning {
		flags |= model.FlagDividedByZeroAsWarning
	}
	if sc.PadCharToFullLength {
		flags |= model.FlagPadCharToFullLength
	}
	return flags
}

// handleIsExtra checks whether this column is a extra handle column generated during plan building phase.
func handleIsExtra(col *expression.Column) bool {
	if col != nil && col.ID == model.ExtraHandleID {
		return true
	}
	return false
}

func splitRanges(ranges []*ranger.Range, keepOrder bool) ([]*ranger.Range, []*ranger.Range) {
	if len(ranges) == 0 || ranges[0].LowVal[0].Kind() == types.KindInt64 {
		return ranges, nil
	}
	idx := sort.Search(len(ranges), func(i int) bool { return ranges[i].HighVal[0].GetUint64() > math.MaxInt64 })
	if idx == len(ranges) {
		return ranges, nil
	}
	if ranges[idx].LowVal[0].GetUint64() > math.MaxInt64 {
		signedRanges := ranges[0:idx]
		unsignedRanges := ranges[idx:]
		if !keepOrder {
			return append(unsignedRanges, signedRanges...), nil
		}
		return signedRanges, unsignedRanges
	}
	signedRanges := make([]*ranger.Range, 0, idx+1)
	unsignedRanges := make([]*ranger.Range, 0, len(ranges)-idx)
	signedRanges = append(signedRanges, ranges[0:idx]...)
	signedRanges = append(signedRanges, &ranger.Range{
		LowVal:     ranges[idx].LowVal,
		LowExclude: ranges[idx].LowExclude,
		HighVal:    []types.Datum{types.NewUintDatum(math.MaxInt64)},
	})
	unsignedRanges = append(unsignedRanges, &ranger.Range{
		LowVal:      []types.Datum{types.NewUintDatum(math.MaxInt64 + 1)},
		HighVal:     ranges[idx].HighVal,
		HighExclude: ranges[idx].HighExclude,
	})
	if idx < len(ranges) {
		unsignedRanges = append(unsignedRanges, ranges[idx+1:]...)
	}
	if !keepOrder {
		return append(unsignedRanges, signedRanges...), nil
	}
	return signedRanges, unsignedRanges
}

// rebuildIndexRanges will be called if there's correlated column in access conditions. We will rebuild the range
// by substitute correlated column with the constant.
func rebuildIndexRanges(ctx sessionctx.Context, is *plannercore.PhysicalIndexScan, idxCols []*expression.Column, colLens []int) (ranges []*ranger.Range, err error) {
	access := make([]expression.Expression, 0, len(is.AccessCondition))
	for _, cond := range is.AccessCondition {
		newCond, err1 := expression.SubstituteCorCol2Constant(cond)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		access = append(access, newCond)
	}
	ranges, _, err = ranger.DetachSimpleCondAndBuildRangeForIndex(ctx, access, idxCols, colLens)
	return ranges, err
}

/*
  tableWorker1       tableWorker2    tableWorker3
       ^                ^                ^
       |                |                |
	   |                |                |
	   |                |                |
	   __________________________________
						 ^
						 |
						 |
						 |
						andWoker  // indexWorkers are all finished
       ____________________________________
     	^                ^                ^
		|				 |				  |
		|				 |                |
		|                |                |
Index[1]Worker   Index[2]Worker ... Index[n]Worker

*/

// Like IndexLookUpExecutor, but some fields become array.
type IndexMergeLookUpExecutor struct {
	baseExecutor

	table           table.Table
	indices         []*model.IndexInfo
	physicalTableID int64
	keepOrders      []bool
	descs           []bool
	rangess         [][]*ranger.Range
	dagPBs          []*tipb.DAGRequest

	handleIdx int

	tableRequest    *tipb.DAGRequest
	columns         []*model.ColumnInfo
	indexStreamings []bool
	tableStreaming  bool
	*dataReaderBuilder

	idxPlans []plannercore.PhysicalPlan
	tblPlans []plannercore.PhysicalPlan

	// first 's' for index, second 's' for column
	idxColss [][]*expression.Column
	colLenss [][]int

	// just think doubleread,second step is the same as indexlookup
	// andworder send task to tableworker
	resultCh   chan *lookupTableTask
	resultCurr *lookupTableTask

	// use it to let indexworker andworker tableworker know 'finish' and exit
	finished chan struct{}

	// now just set it invalid
	feedback *statistics.QueryFeedback

	// to store all indeices return rowid
	// then sort them and merger get the satisfy conditions rowid
	rowids [][]int

	// TODO (how to use them)
	// corColInIdxSide bool
	// corColInTblSide bool
	// corColInAccess  bool

	idxWorkerWg sync.WaitGroup
	tblWorkerWg sync.WaitGroup
	andWokerWg  sync.WaitGroup

	mulType int
}

func (e *IndexMergeLookUpExecutor) Close() error {
	log.Print("CALL Close")
	return nil
}

// Next implements Exec Next interface.
func (e *IndexMergeLookUpExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	req.Reset()
	for {
		resultTask, err := e.getResultTask()
		if err != nil {
			return errors.Trace(err)
		}
		if resultTask == nil {
			return nil
		}
		for resultTask.cursor < len(resultTask.rows) {
			req.AppendRow(resultTask.rows[resultTask.cursor])
			resultTask.cursor++
			if req.NumRows() >= e.maxChunkSize {
				return nil
			}
		}
	}
}

func (e *IndexMergeLookUpExecutor) getResultTask() (*lookupTableTask, error) {
	if e.resultCurr != nil && e.resultCurr.cursor < len(e.resultCurr.rows) {
		return e.resultCurr, nil
	}
	task, ok := <-e.resultCh
	if !ok {
		return nil, nil
	}
	if err := <-task.doneCh; err != nil {
		return nil, errors.Trace(err)
	}

	// Release the memory usage of last task before we handle a new task.
	if e.resultCurr != nil {
		//e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

func (e *IndexMergeLookUpExecutor) Open(ctx context.Context) error {
	kvRangess := make([][]kv.KeyRange, 0, 2)
	for i := 0; i < len(e.idxPlans); i++ {
		oneKVRanges, err := distsql.IndexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.indices[i].ID, e.rangess[i], e.feedback)
		if err != nil {
			// now not need, because it is invalid
			// e.feedback.Invalidate()
			return errors.Trace(err)
		}
		kvRangess = append(kvRangess, oneKVRanges)
	}
	err := e.open(ctx, kvRangess)
	if err != nil {
		e.feedback.Invalidate()
	}

	return errors.Trace(err)
}

func (e *IndexMergeLookUpExecutor) open(ctx context.Context, kvRangess [][]kv.KeyRange) error {
	// TODO memory tracker

	e.finished = make(chan struct{})
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))

	// TODO corCol

	// andWorker will write to workCh and tableWorker will read from workCh,
	// so fetching index and getting table data can run concurrently.
	workCh := make(chan *lookupTableTask, 1)
	fetchCh := make(chan *lookupTableTask, len(kvRangess))

	go e.startAndWorker(ctx, workCh, fetchCh, len(kvRangess))

	for i := 0; i < len(kvRangess); i++ {
		e.idxWorkerWg.Add(1)
		go e.startIndexWorker(ctx, kvRangess[i], fetchCh, i)
	}
	//make sure all indexWorker finish
	e.idxWorkerWg.Done()

	go e.startTableWorker(ctx, workCh)

	return nil
}

// (1)do sort handles and do 'and' operation
// (2)build lookuptask, and send to tableWorker
func (e *IndexMergeLookUpExecutor) startAndWorker(ctx context.Context, workCh chan<- *lookupTableTask, fetchCh <-chan *lookupTableTask, total int) {
	worker := &andWorkerForIndexMerge{
		fetchCh:  fetchCh,
		workCh:   workCh,
		resultCh: e.resultCh,
		handles:  make([][]int64, len(e.idxPlans)),
		mulType:  e.mulType,
		bitmaps: make([]*bitmap.Bitmapset,len(e.idxPlans)),
	}

	e.andWokerWg.Add(1)
	go func() {
		worker.fetchLoop(total, ctx)
		if worker.mulType == 1 {
			worker.getFinalHanlesForAnd(ctx)
		} else if worker.mulType == 3 {
			//worker.getFinalHanlesForOr(ctx)
		}

		close(workCh) //why ??
		close(e.resultCh)
		e.andWokerWg.Done()
	}()
	//e.andWokerWg.Done()
	e.andWokerWg.Wait()

}

// according to kvrange to get handle
func (e *IndexMergeLookUpExecutor) startIndexWorker(ctx context.Context, kvRanges []kv.KeyRange, fetchCh chan<- *lookupTableTask, which int) error {
	// TODO
	if e.runtimeStats != nil {
		collExec := true
		e.dagPBs[which].CollectExecutionSummaries = &collExec
	}

	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPBs[which]).
		SetDesc(e.descs[which]).
		SetKeepOrder(e.keepOrders[which]).
		SetStreaming(e.indexStreamings[which]).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()

	if err != nil {
		return errors.Trace(err)
	}

	result, err := distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, e.feedback, getPhysicalPlanIDs([]plannercore.PhysicalPlan{e.idxPlans[which]}))

	if err != nil {
		return errors.Trace(err)
	}

	result.Fetch(ctx)
	worker := &indexWorkerForIndexMerge{
		fetchCh:      fetchCh,
		finished:     e.finished,
		resultCh:     e.resultCh,
		keepOrder:    e.keepOrders[which],
		batchSize:    e.maxChunkSize,
		maxBatchSize: e.ctx.GetSessionVars().IndexLookupSize,
		maxChunkSize: e.maxChunkSize,
	}

	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}

	// now have mul-indexworker,we change this to open()
	// e.idxWorkerWg.Add(1)
	go func() {
		ctx1, cancel := context.WithCancel(ctx)
		count, err := worker.fetchHandles(ctx1, result, which)
		if err != nil {
			e.feedback.Invalidate()
		}
		cancel()
		if err := result.Close(); err != nil {
			log.Error("close Select result failed:", errors.ErrorStack(err))
		}
		if e.runtimeStats != nil {
			copStats := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.idxPlans[len(e.idxPlans)-1].ExplainID())
			copStats.SetRowNum(count)
			copStats = e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.tblPlans[0].ExplainID())
			copStats.SetRowNum(count)
		}
		e.ctx.StoreQueryFeedback(e.feedback)
		//close(fetchCh)
		// TODO can close resultCh????
		// close(e.resultCh)
		// e.idxWorkerWg.Done()
	}()

	return nil
}

// according handles to get rows
func (e *IndexMergeLookUpExecutor) startTableWorker(ctx context.Context, workCh <-chan *lookupTableTask) {
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		worker := &tableWorkerForIndexMerge{
			workCh:         workCh,
			finished:       e.finished,
			buildTblReader: e.buildTableReader,
			keepOrder:      false, //e.keepOrder,
			handleIdx:      e.handleIdx,
			isCheckOp:      false, //e.isCheckOp,
			memTracker:     memory.NewTracker("tableWorker", -1),
		}
		//worker.memTracker.AttachTo(e.memTracker)
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			worker.pickAndExecTask(ctx1)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

func (e *IndexMergeLookUpExecutor) buildTableReader(ctx context.Context, handles []int64) (Executor, error) {
	tableReaderExec := &TableReaderExecutor{
		baseExecutor:    newBaseExecutor(e.ctx, e.schema, e.id+"_tableReader"),
		table:           e.table,
		physicalTableID: e.physicalTableID,
		dagPB:           e.tableRequest,
		streaming:       e.tableStreaming,
		feedback:        statistics.NewQueryFeedback(0, nil, 0, false),
		//corColInFilter:  nil,//e.corColInTblSide,
		plans: e.tblPlans,
	}
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, handles)
	if err != nil {
		log.Error(err)
		return nil, errors.Trace(err)
	}
	return tableReader, nil
}

// andWorkerForMulIndex
type andWorkerForIndexMerge struct {
	fetchCh  <-chan *lookupTableTask
	finished <-chan struct{}
	// to store all indexWoker return handles
	workCh   chan<- *lookupTableTask
	resultCh chan<- *lookupTableTask
	handles  [][]int64
	bitmaps []*bitmap.Bitmapset
	mulType  int
}

//type int64arr []int64
//func (a int64arr) Len() int { return len(a) }
//func (a int64arr) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
//func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

func (w *andWorkerForIndexMerge) getFinalHanlesForOr(ctx context.Context) {
	finalHandles := make([]int64, 0, 100)
	added := make(map[int64]int64)
	for i := 0; i < len(w.handles); i++ {
		//finalHandles = append(finalHandles, w.handles[i]...)
		for j := 0; j < len(w.handles[i]); j++ {
			_, ok := added[w.handles[i][j]]
			if ok {
				continue
			} else {
				added[w.handles[i][j]] = w.handles[i][j]
				finalHandles = append(finalHandles, w.handles[i][j])
			}
		}
	}
	handlesCount := len(finalHandles)
	for i := 0; i < handlesCount; i++ {
		handles := make([]int64, 0, 1)
		handles = append(handles, finalHandles[i])
		task := w.buildTableTask(handles)
		select {
		case <-ctx.Done():
			return
		case <-w.finished:
			return //count, nil
		case w.workCh <- task:
			w.resultCh <- task
		default:
			return
		}

	}

}

func (w *andWorkerForIndexMerge) getFinalHanlesForAnd(ctx context.Context) {
	for i := 0; i < len(w.handles); i++ {
		sort.Slice(w.handles[i][:], func(m, n int) bool { return w.handles[i][m] < w.handles[i][n] })
	}
	lengths := make([]int, 0, len(w.handles))
	curs := make([]int, 0, len(w.handles))

	minIndex := -1
	minLen := math.MaxInt64
	for i := 0; i < len(w.handles); i++ {
		tempLen := len(w.handles[i])
		lengths = append(lengths, tempLen)
		if tempLen < minLen {
			minLen = tempLen
			minIndex = i
		}
		curs = append(curs, 0)
	}

	if minLen == 0 {
		// no result can be found
		// tell finish
		return
	}
	finalHandles := make([]int64, 0, minLen)

	for {
		if curs[minIndex] == lengths[minIndex] {
			break
		}
		curHandle := w.handles[minIndex][curs[minIndex]]
		canBeInFinal := true
		someOneFinish := false
		for i := 0; i < len(w.handles); i++ {
			if i == minIndex {
				continue
			}
			if curs[i] == lengths[i] {
				someOneFinish = true
				break
			}

			if w.handles[i][curs[i]] < curHandle {
				curs[i]++
				for (curs[i] < lengths[i]) && (w.handles[i][curs[i]] < curHandle) {
					curs[i]++
				}
				if curs[i] == lengths[i] {
					someOneFinish = true
					break
				} else if w.handles[i][curs[i]] > curHandle {
					canBeInFinal = false
					continue
				}
			}
		}
		if someOneFinish {
			break
		}
		if canBeInFinal {
			finalHandles = append(finalHandles, w.handles[minIndex][curs[minIndex]])
		}
		curs[minIndex]++
	}
	//according to handlesCount to splict array and get task send to tableWorker
	handlesCount := len(finalHandles)

	for i := 0; i < handlesCount; i++ {
		handles := make([]int64, 0, 1)
		handles = append(handles, finalHandles[i])
		task := w.buildTableTask(handles)
		select {
		case <-ctx.Done():
			return
		case <-w.finished:
			return //count, nil
		case w.workCh <- task:
			w.resultCh <- task
		default:
			return
		}

	}

}

func (w *andWorkerForIndexMerge) buildTableTask(handles []int64) *lookupTableTask {
	var indexOrder map[int64]int
	/*if w.keepOrder {
		// Save the index order.
		indexOrder = make(map[int64]int, len(handles))
		for i, h := range handles {
			indexOrder[h] = i
		}
	}*/
	task := &lookupTableTask{
		handles:    handles,
		indexOrder: indexOrder,
	}
	task.doneCh = make(chan error, 1)
	return task
}

func (w *andWorkerForIndexMerge) fetchLoop(total int,ctx context.Context) {
	var task *lookupTableTask
	var ok bool
	for {
		if total == 0 {
			return
		}
		select {
		case task, ok = <-w.fetchCh:
			if !ok {
				return
			}
			handles, which := w.fetchHandles(task)
			// means someone has finished
			hc := len(handles)
			if hc == 0 {
				total--
				//continue
			}
			if w.mulType == 3 {
				fhs := make([]int64,0,0)
				for i := 0; i < hc; i++ {
					if !bitmap.IsABitmapMember(int(handles[i]),w.bitmaps[0]) {
						fhs = append(fhs,handles[i])
						w.bitmaps[0] = bitmap.AddMember(w.bitmaps[0],int(handles[i]))
					}
				}
				task := w.buildTableTask(fhs)
				select {
				case <-ctx.Done():
					return
				case <-w.finished:
					return //count, nil
				case w.workCh <- task:
					w.resultCh <- task
				default:
					return
				}
			} else if w.mulType == 1 {
				log.Println(which)
			}
			//if w.handles[which] == nil {
			//	w.handles[which] = make([]int64, 0, 64)
			//}
			//w.handles[which] = append(w.handles[which], handles...)
		case <-w.finished:
			return
		}
	}
	//sort.Int

}

func (w *andWorkerForIndexMerge) fetchHandles(task *lookupTableTask) ([]int64, int) {
	return task.handles, task.cursor
}

// indexWorkerForMulIndex
type indexWorkerForIndexMerge struct {
	fetchCh      chan<- *lookupTableTask
	finished     <-chan struct{}
	resultCh     chan<- *lookupTableTask
	keepOrder    bool
	batchSize    int
	maxBatchSize int
	maxChunkSize int
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are sent to workCh to be further processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (w *indexWorkerForIndexMerge) fetchHandles(ctx context.Context, result distsql.SelectResult, which int) (count int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("indexWorker panic stack is:\n%s", buf)
			err4Panic := errors.Errorf("%v", r)
			doneCh := make(chan error, 1)
			doneCh <- err4Panic
			w.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			if err != nil {
				err = errors.Trace(err4Panic)
			}
		}
	}()
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, w.maxChunkSize)
	for {
		handles, err := w.extractTaskHandles(ctx, chk, result)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- errors.Trace(err)
			w.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			return count, err
		}
		if len(handles) == 0 {
			task := w.buildTableTask(handles, which)
			w.fetchCh <- task
			return count, nil
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles, which)
		select {
		case <-ctx.Done():
			return count, nil
		case <-w.finished:
			return count, nil
		case w.fetchCh <- task:
			//w.resultCh <- task
		}
	}
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorkerForIndexMerge struct {
	workCh         <-chan *lookupTableTask
	finished       <-chan struct{}
	buildTblReader func(ctx context.Context, handles []int64) (Executor, error)
	keepOrder      bool
	handleIdx      int

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// isCheckOp is used to determine whether we need to check the consistency of the index data.
	isCheckOp bool
}

// pickAndExecTask picks tasks from workCh, and execute them.
func (w *tableWorkerForIndexMerge) pickAndExecTask(ctx context.Context) {
	var task *lookupTableTask
	var ok bool
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("tableWorker panic stack is:\n%s", buf)
			task.doneCh <- errors.Errorf("%v", r)
		}
	}()
	for {
		// Don't check ctx.Done() on purpose. If background worker get the signal and all
		// exit immediately, session's goroutine doesn't know this and still calling Next(),
		// it may block reading task.doneCh forever.
		select {
		case task, ok = <-w.workCh:
			if !ok {
				return
			}
		case <-w.finished:
			return
		}
		err := w.executeTask(ctx, task)
		task.doneCh <- errors.Trace(err)
	}
}

func (w *tableWorkerForIndexMerge) executeTask(ctx context.Context, task *lookupTableTask) error {
	tableReader, err := w.buildTblReader(ctx, task.handles)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	defer terror.Call(tableReader.Close)

	// TODO later will finish memTracker
	//task.memTracker = w.memTracker
	//memUsage := int64(cap(task.handles) * 8)
	//task.memUsage = memUsage
	//task.memTracker.Consume(memUsage)
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Row, 0, handleCnt)
	for {
		chk := tableReader.newFirstChunk()
		err = tableReader.Next(ctx, chunk.NewRecordBatch(chk))
		if err != nil {
			log.Error(err)
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		//memUsage = chk.MemoryUsage()
		//task.memUsage += memUsage
		//task.memTracker.Consume(memUsage)
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			task.rows = append(task.rows, row)
		}
	}
	//memUsage = int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Row{}))
	//task.memUsage += memUsage
	//task.memTracker.Consume(memUsage)
	if w.keepOrder {
		task.rowIdx = make([]int, 0, len(task.rows))
		for i := range task.rows {
			handle := task.rows[i].GetInt64(w.handleIdx)
			task.rowIdx = append(task.rowIdx, task.indexOrder[handle])
		}
		//memUsage = int64(cap(task.rowIdx) * 4)
		//task.memUsage += memUsage
		//task.memTracker.Consume(memUsage)
		sort.Sort(task)
	}

	if w.isCheckOp && handleCnt != len(task.rows) {
		obtainedHandlesMap := make(map[int64]struct{}, len(task.rows))
		for _, row := range task.rows {
			handle := row.GetInt64(w.handleIdx)
			obtainedHandlesMap[handle] = struct{}{}
		}
		return errors.Errorf("handle count %d isn't equal to value count %d, missing handles %v in a batch",
			handleCnt, len(task.rows), GetLackHandles(task.handles, obtainedHandlesMap))
	}

	return nil
}

func (w *indexWorkerForIndexMerge) buildTableTask(handles []int64, which int) *lookupTableTask {
	var indexOrder map[int64]int
	if w.keepOrder {
		// Save the index order.
		indexOrder = make(map[int64]int, len(handles))
		for i, h := range handles {
			indexOrder[h] = i
		}
	}
	task := &lookupTableTask{
		handles:    handles,
		indexOrder: indexOrder,
		cursor:     which, // at this stage, it is for which index
	}
	task.doneCh = make(chan error, 1)
	return task
}

func (w *indexWorkerForIndexMerge) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult) (handles []int64, err error) {
	handles = make([]int64, 0, w.batchSize)
	for len(handles) < w.batchSize {
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return handles, err
		}
		if chk.NumRows() == 0 {
			return handles, nil
		}
		for i := 0; i < chk.NumRows(); i++ {
			handles = append(handles, chk.GetRow(i).GetInt64(0))
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, nil
}

// IndexReaderExecutor sends dag request and reads index data from kv layer.
type IndexReaderExecutor struct {
	baseExecutor

	table           table.Table
	index           *model.IndexInfo
	physicalTableID int64
	keepOrder       bool
	desc            bool
	ranges          []*ranger.Range
	dagPB           *tipb.DAGRequest

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result distsql.SelectResult
	// columns are only required by union scan.
	columns   []*model.ColumnInfo
	streaming bool
	feedback  *statistics.QueryFeedback

	corColInFilter bool
	corColInAccess bool
	idxCols        []*expression.Column
	colLens        []int
	plans          []plannercore.PhysicalPlan
}

// Close clears all resources hold by current object.
func (e *IndexReaderExecutor) Close() error {
	err := e.result.Close()
	e.result = nil
	if e.runtimeStats != nil {
		copStats := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.plans[0].ExplainID())
		copStats.SetRowNum(e.feedback.Actual())
	}
	e.ctx.StoreQueryFeedback(e.feedback)
	return errors.Trace(err)
}

// Next implements the Executor Next interface.
func (e *IndexReaderExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tableReader.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	err := e.result.Next(ctx, req.Chunk)
	if err != nil {
		e.feedback.Invalidate()
	}
	return errors.Trace(err)
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open(ctx context.Context) error {
	var err error
	if e.corColInAccess {
		e.ranges, err = rebuildIndexRanges(e.ctx, e.plans[0].(*plannercore.PhysicalIndexScan), e.idxCols, e.colLens)
		if err != nil {
			return errors.Trace(err)
		}
	}
	kvRanges, err := distsql.IndexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.index.ID, e.ranges, e.feedback)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	return e.open(ctx, kvRanges)
}

func (e *IndexReaderExecutor) open(ctx context.Context, kvRanges []kv.KeyRange) error {
	var err error
	if e.corColInFilter {
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if e.runtimeStats != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}

	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	e.result, err = distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, e.retTypes(), e.feedback, getPhysicalPlanIDs(e.plans))
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	e.result.Fetch(ctx)
	return nil
}

// IndexLookUpExecutor implements double read for index scan.
type IndexLookUpExecutor struct {
	baseExecutor

	table           table.Table
	index           *model.IndexInfo
	physicalTableID int64
	keepOrder       bool
	desc            bool
	ranges          []*ranger.Range
	dagPB           *tipb.DAGRequest
	// handleIdx is the index of handle, which is only used for case of keeping order.
	handleIdx    int
	tableRequest *tipb.DAGRequest
	// columns are only required by union scan.
	columns        []*model.ColumnInfo
	indexStreaming bool
	tableStreaming bool
	*dataReaderBuilder
	// All fields above are immutable.

	idxWorkerWg sync.WaitGroup
	tblWorkerWg sync.WaitGroup
	finished    chan struct{}

	resultCh   chan *lookupTableTask
	resultCurr *lookupTableTask
	feedback   *statistics.QueryFeedback

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// isCheckOp is used to determine whether we need to check the consistency of the index data.
	isCheckOp bool

	corColInIdxSide bool
	idxPlans        []plannercore.PhysicalPlan
	corColInTblSide bool
	tblPlans        []plannercore.PhysicalPlan
	corColInAccess  bool
	idxCols         []*expression.Column
	colLens         []int
}

// Open implements the Executor Open interface.
func (e *IndexLookUpExecutor) Open(ctx context.Context) error {
	var err error
	if e.corColInAccess {
		e.ranges, err = rebuildIndexRanges(e.ctx, e.idxPlans[0].(*plannercore.PhysicalIndexScan), e.idxCols, e.colLens)
		if err != nil {
			return errors.Trace(err)
		}
	}
	kvRanges, err := distsql.IndexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.index.ID, e.ranges, e.feedback)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	err = e.open(ctx, kvRanges)
	if err != nil {
		e.feedback.Invalidate()
	}
	return errors.Trace(err)
}

func (e *IndexLookUpExecutor) open(ctx context.Context, kvRanges []kv.KeyRange) error {
	// We have to initialize "memTracker" and other execution resources in here
	// instead of in function "Open", because this "IndexLookUpExecutor" may be
	// constructed by a "IndexLookUpJoin" and "Open" will not be called in that
	// situation.
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupReader)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.finished = make(chan struct{})
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))

	var err error
	if e.corColInIdxSide {
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.idxPlans)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if e.corColInTblSide {
		e.tableRequest.Executors, _, err = constructDistExec(e.ctx, e.tblPlans)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// indexWorker will write to workCh and tableWorker will read from workCh,
	// so fetching index and getting table data can run concurrently.
	workCh := make(chan *lookupTableTask, 1)
	err = e.startIndexWorker(ctx, kvRanges, workCh)
	if err != nil {
		return errors.Trace(err)
	}
	e.startTableWorker(ctx, workCh)
	return nil
}

// startIndexWorker launch a background goroutine to fetch handles, send the results to workCh.
func (e *IndexLookUpExecutor) startIndexWorker(ctx context.Context, kvRanges []kv.KeyRange, workCh chan<- *lookupTableTask) error {
	if e.runtimeStats != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}

	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.indexStreaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	// Since the first read only need handle information. So its returned col is only 1.
	result, err := distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, e.feedback, getPhysicalPlanIDs(e.idxPlans))
	if err != nil {
		return errors.Trace(err)
	}
	result.Fetch(ctx)
	worker := &indexWorker{
		workCh:       workCh,
		finished:     e.finished,
		resultCh:     e.resultCh,
		keepOrder:    e.keepOrder,
		batchSize:    e.maxChunkSize,
		maxBatchSize: e.ctx.GetSessionVars().IndexLookupSize,
		maxChunkSize: e.maxChunkSize,
	}
	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}
	e.idxWorkerWg.Add(1)
	go func() {
		ctx1, cancel := context.WithCancel(ctx)
		count, err := worker.fetchHandles(ctx1, result)
		if err != nil {
			e.feedback.Invalidate()
		}
		cancel()
		if err := result.Close(); err != nil {
			log.Error("close Select result failed:", errors.ErrorStack(err))
		}
		if e.runtimeStats != nil {
			copStats := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.idxPlans[len(e.idxPlans)-1].ExplainID())
			copStats.SetRowNum(count)
			copStats = e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.tblPlans[0].ExplainID())
			copStats.SetRowNum(count)
		}
		e.ctx.StoreQueryFeedback(e.feedback)
		close(workCh)
		close(e.resultCh)
		e.idxWorkerWg.Done()
	}()
	return nil
}

// startTableWorker launchs some background goroutines which pick tasks from workCh and execute the task.
func (e *IndexLookUpExecutor) startTableWorker(ctx context.Context, workCh <-chan *lookupTableTask) {
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		worker := &tableWorker{
			workCh:         workCh,
			finished:       e.finished,
			buildTblReader: e.buildTableReader,
			keepOrder:      e.keepOrder,
			handleIdx:      e.handleIdx,
			isCheckOp:      e.isCheckOp,
			memTracker:     memory.NewTracker("tableWorker", -1),
		}
		worker.memTracker.AttachTo(e.memTracker)
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			worker.pickAndExecTask(ctx1)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

func (e *IndexLookUpExecutor) buildTableReader(ctx context.Context, handles []int64) (Executor, error) {
	tableReaderExec := &TableReaderExecutor{
		baseExecutor:    newBaseExecutor(e.ctx, e.schema, e.id+"_tableReader"),
		table:           e.table,
		physicalTableID: e.physicalTableID,
		dagPB:           e.tableRequest,
		streaming:       e.tableStreaming,
		feedback:        statistics.NewQueryFeedback(0, nil, 0, false),
		corColInFilter:  e.corColInTblSide,
		plans:           e.tblPlans,
	}
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, handles)
	if err != nil {
		log.Error(err)
		return nil, errors.Trace(err)
	}
	return tableReader, nil
}

// Close implements Exec Close interface.
func (e *IndexLookUpExecutor) Close() error {
	if e.finished == nil {
		return nil
	}

	close(e.finished)
	// Drain the resultCh and discard the result, in case that Next() doesn't fully
	// consume the data, background worker still writing to resultCh and block forever.
	for range e.resultCh {
	}
	e.idxWorkerWg.Wait()
	e.tblWorkerWg.Wait()
	e.finished = nil
	e.memTracker.Detach()
	e.memTracker = nil
	if e.runtimeStats != nil {
		copStats := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.idxPlans[0].ExplainID())
		copStats.SetRowNum(e.feedback.Actual())
	}
	return nil
}

// Next implements Exec Next interface.
func (e *IndexLookUpExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	req.Reset()
	for {
		resultTask, err := e.getResultTask()
		if err != nil {
			return errors.Trace(err)
		}
		if resultTask == nil {
			return nil
		}
		for resultTask.cursor < len(resultTask.rows) {
			req.AppendRow(resultTask.rows[resultTask.cursor])
			resultTask.cursor++
			if req.NumRows() >= e.maxChunkSize {
				return nil
			}
		}
	}
}

func (e *IndexLookUpExecutor) getResultTask() (*lookupTableTask, error) {
	if e.resultCurr != nil && e.resultCurr.cursor < len(e.resultCurr.rows) {
		return e.resultCurr, nil
	}
	task, ok := <-e.resultCh
	if !ok {
		return nil, nil
	}
	if err := <-task.doneCh; err != nil {
		return nil, errors.Trace(err)
	}

	// Release the memory usage of last task before we handle a new task.
	if e.resultCurr != nil {
		e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

// indexWorker is used by IndexLookUpExecutor to maintain index lookup background goroutines.
type indexWorker struct {
	workCh    chan<- *lookupTableTask
	finished  <-chan struct{}
	resultCh  chan<- *lookupTableTask
	keepOrder bool

	// batchSize is for lightweight startup. It will be increased exponentially until reaches the max batch size value.
	batchSize    int
	maxBatchSize int
	maxChunkSize int
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are sent to workCh to be further processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (w *indexWorker) fetchHandles(ctx context.Context, result distsql.SelectResult) (count int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("indexWorker panic stack is:\n%s", buf)
			err4Panic := errors.Errorf("%v", r)
			doneCh := make(chan error, 1)
			doneCh <- err4Panic
			w.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			if err != nil {
				err = errors.Trace(err4Panic)
			}
		}
	}()
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, w.maxChunkSize)
	for {
		handles, err := w.extractTaskHandles(ctx, chk, result)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- errors.Trace(err)
			w.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			return count, err
		}
		if len(handles) == 0 {
			return count, nil
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles)
		select {
		case <-ctx.Done():
			return count, nil
		case <-w.finished:
			return count, nil
		case w.workCh <- task:
			w.resultCh <- task
		}
	}
}

func (w *indexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult) (handles []int64, err error) {
	handles = make([]int64, 0, w.batchSize)
	for len(handles) < w.batchSize {
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return handles, err
		}
		if chk.NumRows() == 0 {
			return handles, nil
		}
		for i := 0; i < chk.NumRows(); i++ {
			handles = append(handles, chk.GetRow(i).GetInt64(0))
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, nil
}

func (w *indexWorker) buildTableTask(handles []int64) *lookupTableTask {
	var indexOrder map[int64]int
	if w.keepOrder {
		// Save the index order.
		indexOrder = make(map[int64]int, len(handles))
		for i, h := range handles {
			indexOrder[h] = i
		}
	}
	task := &lookupTableTask{
		handles:    handles,
		indexOrder: indexOrder,
	}
	task.doneCh = make(chan error, 1)
	return task
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorker struct {
	workCh         <-chan *lookupTableTask
	finished       <-chan struct{}
	buildTblReader func(ctx context.Context, handles []int64) (Executor, error)
	keepOrder      bool
	handleIdx      int

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// isCheckOp is used to determine whether we need to check the consistency of the index data.
	isCheckOp bool
}

// pickAndExecTask picks tasks from workCh, and execute them.
func (w *tableWorker) pickAndExecTask(ctx context.Context) {
	var task *lookupTableTask
	var ok bool
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("tableWorker panic stack is:\n%s", buf)
			task.doneCh <- errors.Errorf("%v", r)
		}
	}()
	for {
		// Don't check ctx.Done() on purpose. If background worker get the signal and all
		// exit immediately, session's goroutine doesn't know this and still calling Next(),
		// it may block reading task.doneCh forever.
		select {
		case task, ok = <-w.workCh:
			if !ok {
				return
			}
		case <-w.finished:
			return
		}
		err := w.executeTask(ctx, task)
		task.doneCh <- errors.Trace(err)
	}
}

// executeTask executes the table look up tasks. We will construct a table reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (w *tableWorker) executeTask(ctx context.Context, task *lookupTableTask) error {
	tableReader, err := w.buildTblReader(ctx, task.handles)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	defer terror.Call(tableReader.Close)

	task.memTracker = w.memTracker
	memUsage := int64(cap(task.handles) * 8)
	task.memUsage = memUsage
	task.memTracker.Consume(memUsage)
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Row, 0, handleCnt)
	for {
		chk := tableReader.newFirstChunk()
		err = tableReader.Next(ctx, chunk.NewRecordBatch(chk))
		if err != nil {
			log.Error(err)
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		memUsage = chk.MemoryUsage()
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			task.rows = append(task.rows, row)
		}
	}
	memUsage = int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Row{}))
	task.memUsage += memUsage
	task.memTracker.Consume(memUsage)
	if w.keepOrder {
		task.rowIdx = make([]int, 0, len(task.rows))
		for i := range task.rows {
			handle := task.rows[i].GetInt64(w.handleIdx)
			task.rowIdx = append(task.rowIdx, task.indexOrder[handle])
		}
		memUsage = int64(cap(task.rowIdx) * 4)
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		sort.Sort(task)
	}

	if w.isCheckOp && handleCnt != len(task.rows) {
		obtainedHandlesMap := make(map[int64]struct{}, len(task.rows))
		for _, row := range task.rows {
			handle := row.GetInt64(w.handleIdx)
			obtainedHandlesMap[handle] = struct{}{}
		}
		return errors.Errorf("handle count %d isn't equal to value count %d, missing handles %v in a batch",
			handleCnt, len(task.rows), GetLackHandles(task.handles, obtainedHandlesMap))
	}

	return nil
}

// GetLackHandles gets the handles in expectedHandles but not in obtainedHandlesMap.
func GetLackHandles(expectedHandles []int64, obtainedHandlesMap map[int64]struct{}) []int64 {
	diffCnt := len(expectedHandles) - len(obtainedHandlesMap)
	diffHandles := make([]int64, 0, diffCnt)
	var cnt int
	for _, handle := range expectedHandles {
		isExist := false
		if _, ok := obtainedHandlesMap[handle]; ok {
			delete(obtainedHandlesMap, handle)
			isExist = true
		}
		if !isExist {
			diffHandles = append(diffHandles, handle)
			cnt++
			if cnt == diffCnt {
				break
			}
		}
	}

	return diffHandles
}

func getPhysicalPlanIDs(plans []plannercore.PhysicalPlan) []string {
	planIDs := make([]string, 0, len(plans))
	for _, p := range plans {
		planIDs = append(planIDs, p.ExplainID())
	}
	return planIDs
}
