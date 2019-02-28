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

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"golang.org/x/tools/container/intsets"
	"log"
	"math"
)

const (
	netWorkFactor      = 1.5
	netWorkStartFactor = 20.0
	scanFactor         = 2.0
	descScanFactor     = 2 * scanFactor
	memoryFactor       = 5.0
	// 0.5 is the looking up agg context factor.
	hashAggFactor      = 1.2 + 0.5
	selectionFactor    = 0.8
	distinctFactor     = 0.8
	cpuFactor          = 0.9
	distinctAggFactor  = 1.6
	createAggCtxFactor = 6
)

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = [...]property.TaskType{property.CopSingleReadTaskType, property.CopDoubleReadTaskType, property.RootTaskType}

var invalidTask = &rootTask{cst: math.MaxFloat64}

// getPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns.
func getPropByOrderByItems(items []*ByItems) (*property.PhysicalProperty, bool) {
	propItems := make([]property.Item, 0, len(items))
	for _, item := range items {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		propItems = append(propItems, property.Item{Col: col, Desc: item.Desc})
	}
	return &property.PhysicalProperty{Items: propItems}, true
}

func (p *LogicalTableDual) findBestTask(prop *property.PhysicalProperty) (task, error) {
	if !prop.IsEmpty() {
		return invalidTask, nil
	}
	dual := PhysicalTableDual{RowCount: p.RowCount}.Init(p.ctx, p.stats)
	dual.SetSchema(p.schema)
	return &rootTask{p: dual}, nil
}

// findBestTask implements LogicalPlan interface.
func (p *baseLogicalPlan) findBestTask(prop *property.PhysicalProperty) (bestTask task, err error) {
	// If p is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		return nil, nil
	}
	// Look up the task with this prop in the task map.
	// It's used to reduce double counting.
	bestTask = p.getTask(prop)
	if bestTask != nil {
		return bestTask, nil
	}

	if prop.TaskTp != property.RootTaskType {
		// Currently all plan cannot totally push down.
		p.storeTask(prop, invalidTask)
		return invalidTask, nil
	}

	bestTask = invalidTask
	childTasks := make([]task, 0, len(p.children))

	// If prop.enforced is true, cols of prop as parameter in exhaustPhysicalPlans should be nil
	// And reset it for enforcing task prop and storing map<prop,task>
	oldPropCols := prop.Items
	if prop.Enforced {
		// First, get the bestTask without enforced prop
		prop.Enforced = false
		bestTask, err = p.findBestTask(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		prop.Enforced = true
		// Next, get the bestTask with enforced prop
		prop.Items = []property.Item{}
	}
	physicalPlans := p.self.exhaustPhysicalPlans(prop)
	prop.Items = oldPropCols

	for _, pp := range physicalPlans {
		// find best child tasks firstly.
		childTasks = childTasks[:0]
		for i, child := range p.children {
			childTask, err := child.findBestTask(pp.GetChildReqProps(i))
			if err != nil {
				return nil, errors.Trace(err)
			}
			if childTask != nil && childTask.invalid() {
				break
			}
			childTasks = append(childTasks, childTask)
		}

		// This check makes sure that there is no invalid child task.
		if len(childTasks) != len(p.children) {
			continue
		}

		// combine best child tasks with parent physical plan.
		curTask := pp.attach2Task(childTasks...)

		// enforce curTask property
		if prop.Enforced {
			curTask = enforceProperty(prop, curTask, p.basePlan.ctx)
		}

		// get the most efficient one.
		if curTask.cost() < bestTask.cost() {
			bestTask = curTask
		}
	}

	p.storeTask(prop, bestTask)
	return bestTask, nil
}

// tryToGetMemTask will check if this table is a mem table. If it is, it will produce a task.
func (ds *DataSource) tryToGetMemTask(prop *property.PhysicalProperty) (task task, err error) {
	if !prop.IsEmpty() {
		return nil, nil
	}
	if !infoschema.IsMemoryDB(ds.DBName.L) {
		return nil, nil
	}

	memTable := PhysicalMemTable{
		DBName:      ds.DBName,
		Table:       ds.tableInfo,
		Columns:     ds.Columns,
		TableAsName: ds.TableAsName,
	}.Init(ds.ctx, ds.stats)
	memTable.SetSchema(ds.schema)

	// Stop to push down these conditions.
	var retPlan PhysicalPlan = memTable
	if len(ds.pushedDownConds) > 0 {
		sel := PhysicalSelection{
			Conditions: ds.pushedDownConds,
		}.Init(ds.ctx, ds.stats)
		sel.SetChildren(memTable)
		retPlan = sel
	}
	return &rootTask{p: retPlan}, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return table dual.
func (ds *DataSource) tryToGetDualTask() (task, error) {
	for _, cond := range ds.pushedDownConds {
		if con, ok := cond.(*expression.Constant); ok && con.DeferredExpr == nil {
			result, err := expression.EvalBool(ds.ctx, []expression.Expression{cond}, chunk.Row{})
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !result {
				dual := PhysicalTableDual{}.Init(ds.ctx, ds.stats)
				dual.SetSchema(ds.schema)
				return &rootTask{
					p: dual,
				}, nil
			}
		}
	}
	return nil, nil
}

// candidatePath is used to maintain required info for skyline pruning.
type candidatePath struct {
	path         *accessPath
	columnSet    *intsets.Sparse // columnSet is the set of columns that occurred in the access conditions.
	isSingleScan bool
	isMatchProp  bool
}

// compareColumnSet will compares the two set. The last return value is used to indicate
// if they are comparable, it is false when both two sets have columns that do not occur in the other.
// When the second return value is true, the value of first:
// (1) -1 means that `l` is a strict subset of `r`;
// (2) 0 means that `l` equals to `r`;
// (3) 1 means that `l` is a strict superset of `r`.
func compareColumnSet(l, r *intsets.Sparse) (int, bool) {
	lLen, rLen := l.Len(), r.Len()
	if lLen < rLen {
		// -1 is meaningful only when l.SubsetOf(r) is true.
		return -1, l.SubsetOf(r)
	}
	if lLen == rLen {
		// 0 is meaningful only when l.SubsetOf(r) is true.
		return 0, l.SubsetOf(r)
	}
	// 1 is meaningful only when r.SubsetOf(l) is true.
	return 1, r.SubsetOf(l)
}

func compareBool(l, r bool) int {
	if l == r {
		return 0
	}
	if l == false {
		return -1
	}
	return 1
}

// compareCandidates is the core of skyline pruning. It compares the two candidate paths on three dimensions:
// (1): the set of columns that occurred in the access condition,
// (2): whether or not it matches the physical property
// (3): does it require a double scan.
// If `x` is not worse than `y` at all factors,
// and there exists one factor that `x` is better than `y`, then `x` is better than `y`.
func compareCandidates(lhs, rhs *candidatePath) int {
	setsResult, comparable := compareColumnSet(lhs.columnSet, rhs.columnSet)
	if !comparable {
		return 0
	}
	scanResult := compareBool(lhs.isSingleScan, rhs.isSingleScan)
	matchResult := compareBool(lhs.isMatchProp, rhs.isMatchProp)
	sum := setsResult + scanResult + matchResult
	if setsResult >= 0 && scanResult >= 0 && matchResult >= 0 && sum > 0 {
		return 1
	}
	if setsResult <= 0 && scanResult <= 0 && matchResult <= 0 && sum < 0 {
		return -1
	}
	return 0
}

func (ds *DataSource) getTableCandidate(path *accessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	pkCol := ds.getPKIsHandleCol()
	candidate.isMatchProp = len(prop.Items) == 1 && pkCol != nil && prop.Items[0].Col.Equal(nil, pkCol)
	candidate.columnSet = expression.ExtractColumnSet(path.accessConds)
	candidate.isSingleScan = true
	return candidate
}

func (ds *DataSource) getIndexCandidate(path *accessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	all, _ := prop.AllSameOrder()
	// When the prop is empty or `all` is false, `isMatchProp` is better to be `false` because
	// it needs not to keep order for index scan.
	if !prop.IsEmpty() && all {
		for i, col := range path.index.Columns {
			if col.Name.L == prop.Items[0].Col.ColName.L {
				candidate.isMatchProp = matchIndicesProp(path.index.Columns[i:], prop.Items)
				break
			} else if i >= path.eqCondCount {
				break
			}
		}
	}
	candidate.columnSet = expression.ExtractColumnSet(path.accessConds)
	candidate.isSingleScan = isCoveringIndex(ds.schema.Columns, path.index.Columns, ds.tableInfo.PKIsHandle)
	return candidate
}

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func (ds *DataSource) skylinePruning(prop *property.PhysicalProperty) []*candidatePath {
	candidates := make([]*candidatePath, 0, 4)
	for _, path := range ds.possibleAccessPaths {
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.ranges) == 0 && !ds.ctx.GetSessionVars().StmtCtx.UseCache {
			return []*candidatePath{{path: path}}
		}
		var currentCandidate *candidatePath
		if path.isTablePath {
			currentCandidate = ds.getTableCandidate(path, prop)
		} else if len(path.accessConds) > 0 || !prop.IsEmpty() || path.forced {
			// We will use index to generate physical plan if:
			// this path's access cond is not nil or
			// we have prop to match or
			// this index is forced to choose.
			currentCandidate = ds.getIndexCandidate(path, prop)
		} else {
			continue
		}
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			result := compareCandidates(candidates[i], currentCandidate)
			if result == 1 {
				pruned = true
				// We can break here because the current candidate cannot prune others anymore.
				break
			} else if result == -1 {
				candidates = append(candidates[:i], candidates[i+1:]...)
			}
		}
		if !pruned {
			candidates = append(candidates, currentCandidate)
		}
	}
	return candidates
}

// findBestTask implements the PhysicalPlan interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (ds *DataSource) findBestTask(prop *property.PhysicalProperty) (t task, err error) {
	//Start-Delete
	if ds.tableInfo.Name.L == "t1" {
		log.Print(ds.possibleAccessPaths)
	}
	//End-Delete

	// If ds is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		return nil, nil
	}

	t = ds.getTask(prop)
	if t != nil {
		return
	}

	// If prop.enforced is true, the prop.cols need to be set nil for ds.findBestTask.
	// Before function return, reset it for enforcing task prop and storing map<prop,task>.
	oldPropCols := prop.Items
	if prop.Enforced {
		// First, get the bestTask without enforced prop
		prop.Enforced = false
		t, err = ds.findBestTask(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		prop.Enforced = true
		if t != invalidTask {
			ds.storeTask(prop, t)
			return
		}
		// Next, get the bestTask with enforced prop
		prop.Items = []property.Item{}
	}
	defer func() {
		if err != nil {
			return
		}
		if prop.Enforced {
			prop.Items = oldPropCols
			t = enforceProperty(prop, t, ds.basePlan.ctx)
		}
		ds.storeTask(prop, t)
	}()

	t, err = ds.tryToGetDualTask()
	if err != nil || t != nil {
		return t, errors.Trace(err)
	}
	t, err = ds.tryToGetMemTask(prop)
	if err != nil || t != nil {
		return t, errors.Trace(err)
	}

	t = invalidTask

	candidates := ds.skylinePruning(prop)
	for _, candidate := range candidates {
		path := candidate.path
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.ranges) == 0 && !ds.ctx.GetSessionVars().StmtCtx.UseCache {
			dual := PhysicalTableDual{}.Init(ds.ctx, ds.stats)
			dual.SetSchema(ds.schema)
			return &rootTask{
				p: dual,
			}, nil
		}
		if path.isTablePath {
			tblTask, err := ds.convertToTableScan(prop, candidate)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if tblTask.cost() < t.cost() {
				t = tblTask
			}
			continue
		}
		idxTask, err := ds.convertToIndexScan(prop, candidate)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if idxTask.cost() < t.cost() {
			t = idxTask
		}
	}

	// now we consider wheather can be MulIndex-Type Reader
	// we just to consider all condition is connected with 'and' or 'or'.
	// if it is 'and', every index accessPath's filter: accessCondtions is index attr condition tableFilters is other conditions
	// if it is 'or' , every index accessPaht's filter: tableFilters is all conditions
	// so, if is 'or' , we need to check all conditions is index attr conditions
	// first, confirm conditions' form
	// second, convert MulIndexPlan
	mulType := 0
	var paths []*accessPath
	if len(ds.possibleAccessPaths) > 2 {
		// TODO later this will be a function to confirm and get the mulType
		// func (ds *Datasource) getMulTypeAndPaths() (int,[]path){}
		// see copTask comments (0/1/2/3)
		// this function also transforms the old path to new path at the 'or' conditions
		// remember keep the the same order with ds.possibleAccessPaths
		mulType,paths = ds.getMulTypeAndPaths()
		//mulType = 1
	}
	if mulType > 0 {
		//paths = ds.possibleAccessPaths[1:]
		log.Print(mulType)
		if ds.tableInfo.Name.L != "t3" {
			return
		}
		mulIndexTask, err := ds.convertToMulIndexScan(prop, paths, mulType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ds.tableInfo.Name.L == "t3" {
			t = mulIndexTask
		}

	}

	return
}

// isTerminalArg()
func (ds *DataSource)  isTerminalArgAndIndxCol(arg expression.Expression) (bool,int){
	if col,ok := arg.(*expression.Column); ok {
		//match index fisrt column
		for i := 0; i < len(ds.tableInfo.Indices); i++ {
			index := ds.tableInfo.Indices[i]
			if col.ColName.L == index.Columns[0].Name.L {
				return true,i+1 //index id start with 1
			}
		}
		return false,-1
	}
	if _,ok := arg.(*expression.Constant); ok {
		return true,-1
	}
	return false,-1
}

// function args must be col and value
func (ds *DataSource)  checkScalarFunctionIsALeaf(function *expression.ScalarFunction) bool{
	args := function.GetArgs()
	for i := 0; i < len(args); i++ {
		ok ,_ := ds.isTerminalArgAndIndxCol(args[i])
		if !ok {
			return false
		}
	}
	return true
}


// opType 0 for and
// opType 1 for or
func (ds *DataSource) checkConditionSatisfyMulIndex(expressionFilter expression.Expression, opType string) ([]*accessPath,bool){
	conditions := make([]expression.Expression,0,3)
	//switch opType {
	//case 0:

	//case 1:
		// all condition cols are index-cols
		// function first are 'or'
		tempFunction,ok := expressionFilter.(*expression.ScalarFunction)
		if !ok {
			return nil,false
		}
		// all arg1 will save there


		if !(tempFunction.FuncName.L == opType) {
			return nil,false
		}

		for {
			args := tempFunction.GetArgs()
			arg0, ok := args[0].(*expression.ScalarFunction)
			if !ok {
				return nil,false
			}
			if arg0.FuncName.L == opType {
				//arg1 must leaf
				arg1, ok := args[1].(*expression.ScalarFunction)
				if !ok {
					return nil,false
				}
				isLeaf := ds.checkScalarFunctionIsALeaf(arg1)
				if !isLeaf {
					return nil,false
				}
				conditions = append(conditions,arg1)
				tempFunction = arg0
				continue
			}else {
			   // must be leaf and can return true
				isLeaf := ds.checkScalarFunctionIsALeaf(arg0)
				if !isLeaf {
					return nil,false
				}

				arg1, ok := args[1].(*expression.ScalarFunction)
				if !ok {
					return nil,false
				}
				isLeaf = ds.checkScalarFunctionIsALeaf(arg1)
				if !isLeaf {
					return nil,false
				}
				conditions = append(conditions,arg0)
				conditions = append(conditions,arg1)
				break

			}
		}

	//}
	// if 'or', we will do second checking, all col is index-col
	// if so, can
	// need a hash table to know which ,we have been added.
	updatedPath := make([]int, len(ds.possibleAccessPaths) - 1)


	tempPaths := make([]*accessPath,len(ds.possibleAccessPaths)-1)

	if opType == "or" {
		for i := 0 ; i < len(conditions); i++ {
			function, _  := conditions[i].(*expression.ScalarFunction)
			args := function.GetArgs()
			//ds.isTerminalArgAndIndxCol(args[])
			for j := 0; j < len(args); j++ {
				ok,index := ds.isTerminalArgAndIndxCol(args[j])
				if ok && (index > -1) {
					if updatedPath[index] == 0 {
						updatedPath[index] = len(tempPaths)
						path := ds.possibleAccessPaths[index]
						path.tableFilters = make([]expression.Expression,0,1)
						path.accessConds = []expression.Expression{conditions[i]}
						tempPaths[index] = path
						} else {
							path := tempPaths[index]
							// not append,need to construct new funtion 'or' two function
							tempFunction := path.accessConds[0]
							newFunction := expression.NewFunctionInternal(ds.ctx, "or", types.NewFieldType(mysql.TypeTiny), tempFunction,conditions[i])
							path.accessConds = []expression.Expression{newFunction}//append(path.accessConds, conditions[i])
							tempPaths[index] = path
						}
				}
			}
		}
		}

	// get new range for every path
	// maybe not there to get new range just push it to convertMulIndex
	// for i := 0; i < len(tempPaths); i++ {
	//	index := updatedPath[i]
	//	if index != 0{
	//		indexInfo := ds.tableInfo.Indices[index]
			/*(idxCols, colLengths := expression.IndexInfo2Cols(is.schema.Columns, is.Index)
			if len(idxCols) == 0 {
				return ranger.FullRange(), nil
			}
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, idxCols, colLengths)
			if err != nil {
				return nil, err
			}*/
	//	}
	//}






	log.Print(999)

	return tempPaths,true
}

func (ds *DataSource) getMulTypeAndPaths() (int, []*accessPath) {
	if ds.tableInfo.Name.L != "t3" {
		//log.Print("test 'or' 'and'  conditions")
		return 0,nil
	}
	//tempPaths := ds.possibleAccessPaths[1:]
	firstIndexPath := ds.possibleAccessPaths[1]


	if len(firstIndexPath.accessConds) ==0 {
		if len(firstIndexPath.tableFilters) != 1 {
			return 0,nil
		}
		tempPaths, ok := ds.checkConditionSatisfyMulIndex(firstIndexPath.tableFilters[0],"or")
		if ok {
			return 3, tempPaths
		}else {
			return 0,nil
		}
	}else {
		// consider all double read
		tempPaths, ok := ds.checkConditionSatisfyMulIndex(firstIndexPath.tableFilters[0],"and")
		if ok {
			return 1, tempPaths
		}else {
			return 0,nil
		}

	}
	//return 0,nil
}

func isCoveringIndex(columns []*expression.Column, indexColumns []*model.IndexColumn, pkIsHandle bool) bool {
	for _, col := range columns {
		if pkIsHandle && mysql.HasPriKeyFlag(col.RetType.Flag) {
			continue
		}
		if col.ID == model.ExtraHandleID {
			continue
		}
		isIndexColumn := false
		for _, indexCol := range indexColumns {
			isFullLen := indexCol.Length == types.UnspecifiedLength || indexCol.Length == col.RetType.Flen
			if col.ColName.L == indexCol.Name.L && isFullLen {
				isIndexColumn = true
				break
			}
		}
		if !isIndexColumn {
			return false
		}
	}
	return true
}

// If there is a table reader which needs to keep order, we should append a pk to table scan.
func (ts *PhysicalTableScan) appendExtraHandleCol(ds *DataSource) {
	if len(ds.schema.TblID2Handle) > 0 {
		return
	}
	pkInfo := model.NewExtraHandleColInfo()
	ts.Columns = append(ts.Columns, pkInfo)
	handleCol := ds.newExtraHandleSchemaCol()
	ts.schema.Append(handleCol)
	ts.schema.TblID2Handle[ds.tableInfo.ID] = []*expression.Column{handleCol}
}

// convertToIndexScan converts the DataSource to index scan with idx.
func (ds *DataSource) convertToIndexScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	path := candidate.path
	idx := path.index
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		IdxCols:          path.idxCols,
		IdxColLens:       path.idxColLens,
		AccessCondition:  path.accessConds,
		Ranges:           path.ranges,
		filterCondition:  path.indexFilters,
		dataSourceSchema: ds.schema,
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
	}.Init(ds.ctx)
	statsTbl := ds.statisticTable
	if statsTbl.Indices[idx.ID] != nil {
		is.Hist = &statsTbl.Indices[idx.ID].Histogram
	}
	rowCount := path.countAfterAccess
	cop := &copTask{indexPlan: is}
	if !candidate.isSingleScan {
		// If it's parent requires single read task, return max cost.
		if prop.TaskTp == property.CopSingleReadTaskType {
			return invalidTask, nil
		}
		// On this way, it's double read case.
		ts := PhysicalTableScan{
			Columns:         ds.Columns,
			Table:           is.Table,
			isPartition:     ds.isPartition,
			physicalTableID: ds.physicalTableID,
		}.Init(ds.ctx)
		ts.SetSchema(ds.schema.Clone())
		cop.tablePlan = ts
	} else if prop.TaskTp == property.CopDoubleReadTaskType {
		// If it's parent requires double read task, return max cost.
		return invalidTask, nil
	}
	is.initSchema(ds.id, idx, cop.tablePlan != nil)
	// Only use expectedCnt when it's smaller than the count we calculated.
	// e.g. IndexScan(count1)->After Filter(count2). The `ds.stats.RowCount` is count2. count1 is the one we need to calculate
	// If expectedCnt and count2 are both zero and we go into the below `if` block, the count1 will be set to zero though it's shouldn't be.
	if (candidate.isMatchProp || prop.IsEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		selectivity := ds.stats.RowCount / path.countAfterAccess
		rowCount = math.Min(prop.ExpectedCnt/selectivity, rowCount)
	}
	is.stats = property.NewSimpleStats(rowCount)
	is.stats.UsePseudoStats = ds.statisticTable.Pseudo
	cop.cst = rowCount * scanFactor
	task = cop
	if candidate.isMatchProp {
		if prop.Items[0].Desc {
			is.Desc = true
			cop.cst = rowCount * descScanFactor
		}
		if cop.tablePlan != nil {
			cop.tablePlan.(*PhysicalTableScan).appendExtraHandleCol(ds)
			cop.doubleReadNeedProj = true
		}
		cop.keepOrder = true
		is.KeepOrder = true
		is.addPushedDownSelection(cop, ds, prop.ExpectedCnt, path)
	} else {
		expectedCnt := math.MaxFloat64
		if prop.IsEmpty() {
			expectedCnt = prop.ExpectedCnt
		} else {
			return invalidTask, nil
		}
		is.addPushedDownSelection(cop, ds, expectedCnt, path)
	}
	if prop.TaskTp == property.RootTaskType {
		task = finishCopTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

// TODO: refactor this part, we should not call Clone in fact.
func (is *PhysicalIndexScan) initSchema(id int, idx *model.IndexInfo, isDoubleRead bool) {
	indexCols := make([]*expression.Column, 0, len(idx.Columns))
	for _, col := range idx.Columns {
		colFound := is.dataSourceSchema.FindColumnByName(col.Name.L)
		if colFound == nil {
			colFound = &expression.Column{
				ColName:  col.Name,
				RetType:  &is.Table.Columns[col.Offset].FieldType,
				UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
			}
		} else {
			colFound = colFound.Clone().(*expression.Column)
		}
		indexCols = append(indexCols, colFound)
	}
	setHandle := false
	for _, col := range is.Columns {
		if (mysql.HasPriKeyFlag(col.Flag) && is.Table.PKIsHandle) || col.ID == model.ExtraHandleID {
			indexCols = append(indexCols, is.dataSourceSchema.FindColumnByName(col.Name.L))
			setHandle = true
			break
		}
	}
	// If it's double read case, the first index must return handle. So we should add extra handle column
	// if there isn't a handle column.
	if isDoubleRead && !setHandle {
		indexCols = append(indexCols, &expression.Column{ID: model.ExtraHandleID, ColName: model.ExtraHandleName, UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID()})
	}
	is.SetSchema(expression.NewSchema(indexCols...))
}

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *copTask, p *DataSource, expectedCnt float64, path *accessPath) {
	// Add filter condition to table plan now.
	indexConds, tableConds := path.indexFilters, path.tableFilters
	if indexConds != nil {
		copTask.cst += copTask.count() * cpuFactor
		count := path.countAfterAccess
		if count >= 1.0 {
			selectivity := path.countAfterIndex / path.countAfterAccess
			count = is.stats.RowCount * selectivity
		}
		stats := &property.StatsInfo{RowCount: count}
		indexSel := PhysicalSelection{Conditions: indexConds}.Init(is.ctx, stats)
		indexSel.SetChildren(is)
		copTask.indexPlan = indexSel
	}
	if tableConds != nil {
		copTask.finishIndexPlan()
		copTask.cst += copTask.count() * cpuFactor
		tableSel := PhysicalSelection{Conditions: tableConds}.Init(is.ctx, p.stats.ScaleByExpectCnt(expectedCnt))
		tableSel.SetChildren(copTask.tablePlan)
		copTask.tablePlan = tableSel
	}
}

func matchIndicesProp(idxCols []*model.IndexColumn, propItems []property.Item) bool {
	if len(idxCols) < len(propItems) {
		return false
	}
	for i, item := range propItems {
		if idxCols[i].Length != types.UnspecifiedLength || item.Col.ColName.L != idxCols[i].Name.L {
			return false
		}
	}
	return true
}

func splitIndexFilterConditions(conditions []expression.Expression, indexColumns []*model.IndexColumn,
	table *model.TableInfo) (indexConds, tableConds []expression.Expression) {
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		if isCoveringIndex(expression.ExtractColumns(cond), indexColumns, table.PKIsHandle) {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// convertToTableScan converts the DataSource to table scan.
func (ds *DataSource) convertToTableScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	// It will be handled in convertToIndexScan.
	if prop.TaskTp == property.CopDoubleReadTaskType {
		return invalidTask, nil
	}

	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
	}.Init(ds.ctx)
	ts.SetSchema(ds.schema)
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	path := candidate.path
	ts.Ranges = path.ranges
	ts.AccessCondition, ts.filterCondition = path.accessConds, path.tableFilters
	rowCount := path.countAfterAccess
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	task = copTask
	// Only use expectedCnt when it's smaller than the count we calculated.
	// e.g. IndexScan(count1)->After Filter(count2). The `ds.stats.RowCount` is count2. count1 is the one we need to calculate
	// If expectedCnt and count2 are both zero and we go into the below `if` block, the count1 will be set to zero though it's shouldn't be.
	if (candidate.isMatchProp || prop.IsEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		selectivity := ds.stats.RowCount / rowCount
		rowCount = math.Min(prop.ExpectedCnt/selectivity, rowCount)
	}
	ts.stats = property.NewSimpleStats(rowCount)
	ts.stats.UsePseudoStats = ds.statisticTable.Pseudo
	copTask.cst = rowCount * scanFactor
	if candidate.isMatchProp {
		if prop.Items[0].Desc {
			ts.Desc = true
			copTask.cst = rowCount * descScanFactor
		}
		ts.KeepOrder = true
		copTask.keepOrder = true
		ts.addPushedDownSelection(copTask, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt))
	} else {
		expectedCnt := math.MaxFloat64
		if prop.IsEmpty() {
			expectedCnt = prop.ExpectedCnt
		} else {
			return invalidTask, nil
		}
		ts.addPushedDownSelection(copTask, ds.stats.ScaleByExpectCnt(expectedCnt))
	}
	if prop.TaskTp == property.RootTaskType {
		task = finishCopTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

// convertToMulIndexScan converts the DataSource to MulIndexScan.
func (ds *DataSource) convertToMulIndexScan(prop *property.PhysicalProperty, paths []*accessPath, mulType int) (task task, err error) {
	indexPlans := make([]PhysicalPlan,0)
	// build new index range
	for _,path := range paths {
		log.Print("dead??")
		if path == nil {
			continue
		}
		idx := path.index
		is := PhysicalIndexScan{
			Table:            ds.tableInfo,
			TableAsName:      ds.TableAsName,
			DBName:           ds.DBName,
			Columns:          ds.Columns,
			Index:            idx,
			IdxCols:          path.idxCols,
			IdxColLens:       path.idxColLens,
			AccessCondition:  path.accessConds,
			Ranges:           path.ranges,
			filterCondition:  path.indexFilters,
			dataSourceSchema: ds.schema,
			isPartition:      ds.isPartition,
			physicalTableID:  ds.physicalTableID,
		}.Init(ds.ctx)
		statsTbl := ds.statisticTable
		if statsTbl.Indices[idx.ID] != nil {
			is.Hist = &statsTbl.Indices[idx.ID].Histogram
		}
		is.stats = ds.stats
		// isDoubleRead is false just because of "must be"
		// isDoubleRead should be true, because need handle
		is.initSchema(ds.id, idx, true)
		// get new range
		idxCols, colLengths := expression.IndexInfo2Cols(is.schema.Columns, is.Index)
		//if len(idxCols) == 0 {
		//	return ranger.FullRange(), nil
		//}
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, is.AccessCondition, idxCols, colLengths)
		is.Ranges = res.Ranges
		if err != nil {
			return nil, err
		}
		//return res.Ranges, nil
		indexPlans = append(indexPlans, is)
	}
	ts := PhysicalTableScan{
		Columns:         ds.Columns,
		Table:           ds.tableInfo,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
	}.Init(ds.ctx)
	ts.SetSchema(ds.schema.Clone())
	ts.stats = ds.stats
	copTask := &copTask{indexPlans: indexPlans, mulType: mulType, tablePlan: ts}
	task = copTask
	//TODO this will be add cost

	if prop.TaskTp == property.RootTaskType {
		task = finishCopTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}

	return task, nil
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTask, stats *property.StatsInfo) {
	// Add filter condition to table plan now.
	if len(ts.filterCondition) > 0 {
		copTask.cst += copTask.count() * cpuFactor
		sel := PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.ctx, stats)
		sel.SetChildren(ts)
		copTask.tablePlan = sel
	}
}
