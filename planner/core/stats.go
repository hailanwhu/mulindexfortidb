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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/util/ranger"
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/statistics"
	log "github.com/sirupsen/logrus"
)

func (p *basePhysicalPlan) StatsCount() float64 {
	return p.stats.RowCount
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalTableDual) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	profile := &property.StatsInfo{
		RowCount:    float64(p.RowCount),
		Cardinality: make([]float64, p.Schema().Len()),
	}
	for i := range profile.Cardinality {
		profile.Cardinality[i] = float64(p.RowCount)
	}
	p.stats = profile
	return p.stats, nil
}

func (p *baseLogicalPlan) recursiveDeriveStats() (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	childStats := make([]*property.StatsInfo, len(p.children))
	for i, child := range p.children {
		childProfile, err := child.recursiveDeriveStats()
		if err != nil {
			return nil, err
		}
		childStats[i] = childProfile
	}
	return p.self.DeriveStats(childStats)
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *baseLogicalPlan) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	if len(childStats) == 1 {
		p.stats = childStats[0]
		return p.stats, nil
	}
	if len(childStats) > 1 {
		err := ErrInternal.GenWithStack("LogicalPlans with more than one child should implement their own DeriveStats().")
		return nil, err
	}
	profile := &property.StatsInfo{
		RowCount:    float64(1),
		Cardinality: make([]float64, p.self.Schema().Len()),
	}
	for i := range profile.Cardinality {
		profile.Cardinality[i] = float64(1)
	}
	p.stats = profile
	return profile, nil
}

func (ds *DataSource) getStatsByFilter(conds expression.CNFExprs) (*property.StatsInfo, *statistics.HistColl) {
	profile := &property.StatsInfo{
		RowCount:       float64(ds.statisticTable.Count),
		Cardinality:    make([]float64, len(ds.Columns)),
		HistColl:       ds.statisticTable.GenerateHistCollFromColumnInfo(ds.Columns, ds.schema.Columns),
		UsePseudoStats: ds.statisticTable.Pseudo,
	}
	for i, col := range ds.Columns {
		hist, ok := ds.statisticTable.Columns[col.ID]
		if ok && hist.Count > 0 {
			factor := float64(ds.statisticTable.Count) / float64(hist.Count)
			profile.Cardinality[i] = float64(hist.NDV) * factor
		} else {
			profile.Cardinality[i] = profile.RowCount * distinctFactor
		}
	}
	ds.stats = profile
	selectivity, nodes, err := profile.HistColl.Selectivity(ds.ctx, conds)
	if err != nil {
		log.Warnf("An error happened: %v, we have to use the default selectivity", err.Error())
		selectivity = selectionFactor
	}
	if ds.ctx.GetSessionVars().OptimizerSelectivityLevel >= 1 && ds.stats.HistColl != nil {
		finalHist := ds.stats.HistColl.NewHistCollBySelectivity(ds.ctx.GetSessionVars().StmtCtx, nodes)
		return profile, finalHist
	}
	return profile.Scale(selectivity), nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (ds *DataSource) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		ds.pushedDownConds[i] = expression.PushDownNot(nil, expr, false)
	}
	var finalHist *statistics.HistColl
	ds.stats, finalHist = ds.getStatsByFilter(ds.pushedDownConds)
	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			noIntervalRanges, err := ds.deriveTablePathStats(path)
			if err != nil {
				return nil, err
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.ranges) == 0 {
				ds.possibleAccessPaths[0] = path
				ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
				break
			}
			continue
		}
		noIntervalRanges, err := ds.deriveIndexPathStats(path)
		if err != nil {
			return nil, err
		}
		// If we have empty range, or point range on unique index, just remove other possible paths.
		if (noIntervalRanges && path.index.Unique) || len(path.ranges) == 0 {
			ds.possibleAccessPaths[0] = path
			ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
			break
		}
	}

	shouldConsiderIndexMerge := true
	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			continue
		}
		if len(path.accessConds) != 0 {
			shouldConsiderIndexMerge = false
			break
		}
	}
	if ds.tableInfo.Name.L == "test" {
		log.Println(0)
	}

	// If all index paths' accessCondtions is empty and have index
	if  len(ds.possibleAccessPaths) > 1 && shouldConsiderIndexMerge {
		ds.GetIndexMergeOrPaths()
	}

	if ds.ctx.GetSessionVars().OptimizerSelectivityLevel >= 1 {
		ds.stats.HistColl = finalHist
	}
	return ds.stats, nil
}

func (ds *DataSource) GetIndexMergeOrPaths(){
	if ds.tableInfo.Name.L == "test" {
		log.Println(0)
	}
	for i, cond := range ds.pushedDownConds {

		var indexAccessPaths []*accessPath = nil
		var imPaths = make([]*accessPath, 0 ,0)


		if sf, ok := cond.(*expression.ScalarFunction); !ok || sf.FuncName.L != ast.LogicOr {
			continue
		}
		sf, _ := cond.(*expression.ScalarFunction)
		dnfItems := expression.FlattenDNFConditions(sf)
		for _, item := range dnfItems {
			if sfD, ok := item.(*expression.ScalarFunction); ok && sfD.FuncName.L == ast.LogicAnd {
				cnfItem := expression.FlattenCNFConditions(sfD)
				indexAccessPaths = ds.BuildAccessPathForOr(cnfItem)
			} else {
				tempArgs := []expression.Expression{item}
				indexAccessPaths = ds.BuildAccessPathForOr(tempArgs)
			}
			if indexAccessPaths == nil {
				imPaths = nil
				break
			}
			imPartialPath := ds.GetIndexMergePartialPath(indexAccessPaths)
			// just for test
			if imPartialPath == nil {
				imPaths = nil
				break
			}
			imPaths = append(imPaths, imPartialPath)
		}
		if imPaths != nil {
			possiblePath := ds.CreateIndexMergeOrPath(imPaths, i)
			ds.indexMergeAccessPaths = append(ds.indexMergeAccessPaths, possiblePath)
		}
	}
}

func (ds *DataSource) BuildAccessPathForOr(conditions []expression.Expression) []*accessPath{
	indexCount := len(ds.tableInfo.Indices)
	if indexCount == 0 {
		return nil
	}
	var results = make([]*accessPath, 0, 0)
	for i := 1; i <= indexCount; i++ {
		res, err := ranger.DetachCondAndBuildRangeForIndexMerge(ds.ctx, conditions, ds.possibleAccessPaths[i].idxCols,ds.possibleAccessPaths[i].idxColLens)
		if err != nil {
			return nil
		}
		if res.AccessConds == nil {
			continue
		}
		indexPath := ds.CreateIndexAccessPath(i, res)
		if indexPath == nil {
			return nil
		}
		results = append(results, indexPath)
	}
	if len(results) == 0 {
		results = nil
	}
	return results
}

// this function will get a best indexPath for a con from some alternative paths.
// now we just take the most compitable index
// for exmple:
// (1)
// index1(a,b,c) index2(a,b) index3(a)
// condition: a = 1 will choose index3; a = 1 and b = 2 will choose index2
// (2)
// index1(a) index2(b)
// condition: a = 1 and b = 1
// random choose???
// maybe we can return all of them, and later to choose which one is better
// (3)
// index1(a) index2(a,b,c)
// condition a = 1 and b = 2

func (ds *DataSource) GetIndexMergePartialPath(indexAccessPaths []*accessPath) *accessPath{
	if len(indexAccessPaths) == 1 {
		return indexAccessPaths[0]
	}
	noTableFilter := make([]int,0,0)
	// first see which does not have tableFilter
	for i, ap := range indexAccessPaths {
		if len(ap.tableFilters) == 0 {
			noTableFilter = append(noTableFilter, i)
		}
	}
	// if multiple accessPath does not have tableFilter we need to choose the exact one
	// if all have tableFilter, randomly pick one
	// TODO maybe all be reserved
	if len(noTableFilter) == 0 {
		return indexAccessPaths[0]
	} else if len(noTableFilter) == 1{
		return indexAccessPaths[noTableFilter[0]]
	} else {
		//TODO which one should be choosen???
		whichMax := 0
		max := len(indexAccessPaths[noTableFilter[0]].idxCols)
		for i := 0; i < len(noTableFilter); i++ {
			current := len(indexAccessPaths[noTableFilter[0]].idxCols)
			if  current > max {
				whichMax = i
				max = current
			}
		}
		return indexAccessPaths[noTableFilter[whichMax]]
	}
}


// why not and, network!!
// why merge or, network??

// (1)maybe we will merge some indexPaths
//    for example: index1(a) index2(b)
//    condition : a < 1 or a > 2 or b < 1 or b > 10
//    imPaths will be [a<1,a>2,b<1,b>10] and we can merge it and get [a<1 or a >2 , b < 1 or b > 10]
//    **must: same index and with tableFilter**
//	  eg: index1(a) (a < 1 and b >2) or a > 2     != (a < 1 or a > 2) and b > 2
// (2)IndexMergePath.tableFilters:
//    <1> remove con from PushdownConditions and the remain will be added to tableFitler.
//    <2> after merge operation, any indexPath's tableFilter is not nil, we should add con into
//        tableFilters
// bad case
//  (a=1 and b < 2 and d =4) or a = 3 or c >4
// index(a) index(a,b) index(c)
// will two  a,b may be one need a, access less tuple??? the same number.
func (ds *DataSource) CreateIndexMergeOrPath(indexAccessPaths []*accessPath, which int) *accessPath{


	finalIndexAccessPaths := make([]*accessPath, 0, 0)
	indexCount := len(ds.tableInfo.Indices)
	accessPathCount := len(indexAccessPaths)
	whichIndexes := make([][]int64,indexCount+1,indexCount+1)
	for i := 0; i < indexCount + 1; i++  {
		whichIndexes[i] = make([]int64,0,accessPathCount)
	}
	for i, path := range indexAccessPaths  {
		whichIndexes[path.index.ID] = append(whichIndexes[path.index.ID], int64(i))
	}

	for ixd, ixr := range whichIndexes  {
		c := len(ixr)
		switch c {
		case 0:
			continue
		case 1:
			finalIndexAccessPaths = append(finalIndexAccessPaths,indexAccessPaths[ixr[0]])
		default:
			// TODO maybe somepart can merge

			canBe := true
			newCondtion := expression.ComposeCNFCondition(ds.ctx,indexAccessPaths[ixr[0]].accessConds...)
			if indexAccessPaths[ixr[0]].tableFilters != nil {
				//newCondtion := expression.ComposeCNFCondition(newCondtion, indexAccessPaths[ixr[0]].tableFilters...)
				temp := append(indexAccessPaths[ixr[0]].tableFilters, newCondtion)
				newCondtion = expression.ComposeCNFCondition(ds.ctx,temp...)
			}

			for i := 1; i < c; i++ {
				tempC := expression.ComposeCNFCondition(ds.ctx,indexAccessPaths[ixr[i]].accessConds...)
				if indexAccessPaths[ixr[i]].tableFilters != nil {
					//canBe = false
					temp := append(indexAccessPaths[ixr[i]].tableFilters, tempC)
					tempC = expression.ComposeCNFCondition(ds.ctx,temp...)
				}

				newCondtion = expression.ComposeDNFCondition(ds.ctx, newCondtion, tempC)
			}
			if canBe {
				res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, []expression.Expression{newCondtion}, ds.possibleAccessPaths[ixd].idxCols,ds.possibleAccessPaths[ixd].idxColLens)
				if err != nil {
					return nil
				}
				if res.AccessConds == nil {
					continue
				}
				indexPath := ds.CreateIndexAccessPath(ixd, res)
				if indexPath == nil {
					return nil
				}
				if indexPath.accessConds == nil {
					canBe = false
				} else {
					finalIndexAccessPaths = append(finalIndexAccessPaths,indexPath)
				}

			}
			if !canBe {
				log.Println("false")
				for i := 0; i < c; i++ {
					finalIndexAccessPaths = append(finalIndexAccessPaths,indexAccessPaths[ixr[i]])
				}
			}
		}
	}
	indexMergePath := new(accessPath)
	indexMergePath.isIndexMerge = true
	indexMergePath.indexMergeType = 3
	for i := 0; i < len(ds.pushedDownConds); i++ {
		if i == which {
			continue
		}
		indexMergePath.tableFiltersForIndexMerge = append(indexMergePath.tableFiltersForIndexMerge,ds.pushedDownConds[i])
	}

	for _, ap := range finalIndexAccessPaths {
		if ap.tableFilters != nil {
			indexMergePath.tableFiltersForIndexMerge = append(indexMergePath.tableFiltersForIndexMerge,ds.pushedDownConds[which])
			break
		}
	}
	indexMergePath.partialPathsForIndexMerge = append(indexMergePath.partialPathsForIndexMerge,finalIndexAccessPaths...)
	//indexMergePath.tableFiltersForIndexMerg
	return indexMergePath
}

func (ds *DataSource) CreateIndexAccessPath(which int, res *ranger.DetachRangeResult) *accessPath {
	//copy a new index path and set the ranges
	newAccessPath := new(accessPath)
	oldAccessPath := ds.possibleAccessPaths[which]
	newAccessPath.idxColLens = oldAccessPath.idxColLens
	newAccessPath.idxCols = oldAccessPath.idxCols
	newAccessPath.index = oldAccessPath.index
	newAccessPath.isTablePath = oldAccessPath.isTablePath

	newAccessPath.accessConds = res.AccessConds
	newAccessPath.ranges = res.Ranges
	newAccessPath.tableFilters = res.RemainedConds
	newAccessPath.eqCondCount = res.EqCondCount
	var err error
	newAccessPath.countAfterAccess, err = ds.stats.HistColl.GetRowCountByIndexRanges(ds.ctx.GetSessionVars().StmtCtx, newAccessPath.index.ID, newAccessPath.ranges)
	if err != nil {
		return nil
	}
	//if newAccessPath.countAfterAccess < ds.stats.RowCount {
	//	newAccessPath.countAfterAccess = math.Min(ds.stats.RowCount/selectionFactor, float64(ds.statisticTable.Count))
	//}

	return newAccessPath
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalSelection) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	p.stats = childStats[0].Scale(selectionFactor)
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalUnionAll) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	p.stats = &property.StatsInfo{
		Cardinality: make([]float64, p.Schema().Len()),
	}
	for _, childProfile := range childStats {
		p.stats.RowCount += childProfile.RowCount
		for i := range p.stats.Cardinality {
			p.stats.Cardinality[i] += childProfile.Cardinality[i]
		}
	}
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalLimit) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    math.Min(float64(p.Count), childProfile.RowCount),
		Cardinality: make([]float64, len(childProfile.Cardinality)),
	}
	for i := range p.stats.Cardinality {
		p.stats.Cardinality[i] = math.Min(childProfile.Cardinality[i], p.stats.RowCount)
	}
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (lt *LogicalTopN) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	lt.stats = &property.StatsInfo{
		RowCount:    math.Min(float64(lt.Count), childProfile.RowCount),
		Cardinality: make([]float64, len(childProfile.Cardinality)),
	}
	for i := range lt.stats.Cardinality {
		lt.stats.Cardinality[i] = math.Min(childProfile.Cardinality[i], lt.stats.RowCount)
	}
	return lt.stats, nil
}

// getCardinality will return the Cardinality of a couple of columns. We simply return the max one, because we cannot know
// the Cardinality for multi-dimension attributes properly. This is a simple and naive scheme of Cardinality estimation.
func getCardinality(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) float64 {
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		log.Errorf("Cannot find column %v indices from schema %s", cols, schema)
		return 0
	}
	var cardinality = 1.0
	for _, idx := range indices {
		// It is a very elementary estimation.
		cardinality = math.Max(cardinality, profile.Cardinality[idx])
	}
	return cardinality
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalProjection) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make([]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.stats.Cardinality[i] = getCardinality(cols, p.children[0].Schema(), childProfile)
	}
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalAggregation) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	cardinality := getCardinality(gbyCols, la.children[0].Schema(), childProfile)
	la.stats = &property.StatsInfo{
		RowCount:    cardinality,
		Cardinality: make([]float64, la.schema.Len()),
	}
	// We cannot estimate the Cardinality for every output, so we use a conservative strategy.
	for i := range la.stats.Cardinality {
		la.stats.Cardinality[i] = cardinality
	}
	la.inputCount = childProfile.RowCount
	return la.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose Cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the Cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	leftProfile, rightProfile := childStats[0], childStats[1]
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount * selectionFactor,
			Cardinality: make([]float64, len(leftProfile.Cardinality)),
		}
		for i := range p.stats.Cardinality {
			p.stats.Cardinality[i] = leftProfile.Cardinality[i] * selectionFactor
		}
		return p.stats, nil
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount,
			Cardinality: make([]float64, p.schema.Len()),
		}
		copy(p.stats.Cardinality, leftProfile.Cardinality)
		p.stats.Cardinality[len(p.stats.Cardinality)-1] = 2.0
		return p.stats, nil
	}
	if 0 == len(p.EqualConditions) {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount * rightProfile.RowCount,
			Cardinality: append(leftProfile.Cardinality, rightProfile.Cardinality...),
		}
		return p.stats, nil
	}
	leftKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	rightKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	for _, eqCond := range p.EqualConditions {
		leftKeys = append(leftKeys, eqCond.GetArgs()[0].(*expression.Column))
		rightKeys = append(rightKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	leftKeyCardinality := getCardinality(leftKeys, p.children[0].Schema(), leftProfile)
	rightKeyCardinality := getCardinality(rightKeys, p.children[1].Schema(), rightProfile)
	count := leftProfile.RowCount * rightProfile.RowCount / math.Max(leftKeyCardinality, rightKeyCardinality)
	if p.JoinType == LeftOuterJoin {
		count = math.Max(count, leftProfile.RowCount)
	} else if p.JoinType == RightOuterJoin {
		count = math.Max(count, rightProfile.RowCount)
	}
	cardinality := make([]float64, 0, p.schema.Len())
	cardinality = append(cardinality, leftProfile.Cardinality...)
	cardinality = append(cardinality, rightProfile.Cardinality...)
	for i := range cardinality {
		cardinality[i] = math.Min(cardinality[i], count)
	}
	p.stats = &property.StatsInfo{
		RowCount:    count,
		Cardinality: cardinality,
	}
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalApply) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	leftProfile := childStats[0]
	la.stats = &property.StatsInfo{
		RowCount:    leftProfile.RowCount,
		Cardinality: make([]float64, la.schema.Len()),
	}
	copy(la.stats.Cardinality, leftProfile.Cardinality)
	if la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		la.stats.Cardinality[len(la.stats.Cardinality)-1] = 2.0
	} else {
		for i := la.children[0].Schema().Len(); i < la.schema.Len(); i++ {
			la.stats.Cardinality[i] = leftProfile.RowCount
		}
	}
	return la.stats, nil
}

// Exists and MaxOneRow produce at most one row, so we set the RowCount of stats one.
func getSingletonStats(len int) *property.StatsInfo {
	ret := &property.StatsInfo{
		RowCount:    1.0,
		Cardinality: make([]float64, len),
	}
	for i := 0; i < len; i++ {
		ret.Cardinality[i] = 1
	}
	return ret
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalMaxOneRow) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	p.stats = getSingletonStats(p.Schema().Len())
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalWindow) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	childLen := len(childProfile.Cardinality)
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make([]float64, childLen+1),
	}
	copy(p.stats.Cardinality, childProfile.Cardinality)
	p.stats.Cardinality[childLen] = childProfile.RowCount
	return p.stats, nil
}
