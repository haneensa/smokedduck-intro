import * as R from "ramda"


export function makeWstTable({ columns, rows }) {
  return {
    col_labels: columns,
    row_labels: R.times(R.identity, rows.length),
    data: rows
  }
}

export function opStepVis(opName, lhs, lhs2, rhs, annotations) {
  let tables = null
  if (lhs2) 
    tables = { lhs, lhs2, rhs }
  else 
    tables = { lhs, rhs }

  return {
    css_id: opName,
    tables,
    expr: {
      code: "",
      range: {
        start: {line: 0, ch: 0},
        end: {line: 1, ch: 0}
      }
    },
    annotations
  }
}

function addExprAnnotations(opid, lineageData, annotations) {
  // the columns to outline
  // indexes of attributes in predicate
  lineageData.exprs[opid]?.forEach(([srcIdx, colIdxs]) => {
    let table = (srcIdx == 0)? "lhs" : "lhs2";
    colIdxs.forEach((col) => 
      annotations.push({
        type: "box", 
        target: { table, row: "all", col }
      })
    )
  })
}

function addProjectionAnnotations(opid, lhsObj, lineageData, annotations) {
    let colLineage = lineageData.col[opid]
      // the columns to make arrows to
      colLineage.forEach((inputColIdxs, oCol) => {
        inputColIdxs.forEach((iCol) => {
          annotations.push({
            type: "arrow",
            from: { table: "lhs", row: "header", col: iCol },
            to: { table: "rhs", row: "header", col: oCol }
          })
        })
      })

      // Cross out rejected columns
      R.times((i) => {
        if (colLineage.flat().includes(i)) return;
        annotations.push({
          type: "crossout", 
          target: {
            table: "lhs", 
            row: "all", 
            col: i
          }
        })
      }, lhsObj.columns.length)
}


function addLineageAnnotations(opid, lineageObj, annotations, input_size) {
  if (!lineageObj) return;
  lineageObj[0][1].forEach(([oid, iid]) => {
    let lRow = {table: "lhs", row: iid, col: "all"}
    let rRow = {table: "rhs", row: oid, col: "all"}
    annotations.push({type:"color_set", set: [lRow, rRow]})
    annotations.push({type: "arrow", from: lRow, to: rRow})
  })
  
  let iids = lineageObj[0][1].map(([oid, iid]) => iid)

  R.times((row) => {
    if (!iids.includes(row))
      annotations.push({
        type: "crossout",
        target: { table: "lhs", row, col: "all" }
      })
  }, input_size)

}
// info:
//     str: str, name: str, schema: [str], id: str, depth: int
// op:
//    str: [str]
// results:
//    str: [[srcidx, [oid, iid]]]
export function projection(opid, lineageData, addOns) {
  console.log("projection: ", opid, lineageData)
  let info = lineageData.info[opid]
  let child_opid = lineageData.op[opid][0]
  let lhsObj = lineageData.results[child_opid]
  let rhsObj = lineageData.results[opid]
  let colLineage = lineageData.col[opid]

  console.log(child_opid, lhsObj, rhsObj, colLineage)

  addOns.desc = "Projection - keeping the fields: " + info.schema
  addOns.opType = "projectionOp"
  addOns.tableCaptions.lhs = "output of: " + child_opid

  let annotations = []

  addProjectionAnnotations(opid, lhsObj, lineageData, annotations);

  // color the rows
  R.times((row) => {
    annotations.push({
      type:"color_set", 
      set: [
        {table: "lhs", row, col: "all"},
        {table: "rhs", row, col: "all"}
      ]})
  },  rhsObj.rows.length)

  return opStepVis(info.name, makeWstTable(lhsObj), null, makeWstTable(rhsObj), annotations )
}


export function join(opid, lineageData, addOns) {
  let lchild_opid = lineageData.op[opid][0];
  let rchild_opid = lineageData.op[opid][1];
  let lhsObj = lineageData.results[lchild_opid]
  let lhs2Obj = lineageData.results[rchild_opid]
  let rhsObj = lineageData.results[opid]
  let lineageObj = lineageData.row[opid]
  let colLineage = lineageData.col[opid]
  let info = lineageData.info[opid]

  console.log("join", opid, rhsObj)
  addOns.desc = "matches up tuples of both tables based on condition: " + info.str
  addOns.opType = "joinOp"
  addOns.tableCaptions.lhs = lineageData.info[lchild_opid].name
  addOns.tableCaptions.lhs2 = lineageData.info[rchild_opid].name

  let annotations = []

  // need to know attributes in join condition
  addExprAnnotations(opid, lineageData, annotations)


  if (lineageObj) {
    // lineage is [ (srcid, [(oid, iid),...]), .. ]
    // needto find for each iid in source 0, all iids in source 1 that share
    // the same output oid
    let leftPairs = lineageObj[0][1]
    let rightPairs = lineageObj[1][1]

    R.zip(leftPairs, rightPairs).forEach(([[loid, liid], [roid, riid]]) => {
      let lhsRow = {table: "lhs", col: "all", row: liid },
        lhs2Row = {table: "lhs2", col: "all", row: riid },
        rhsRow = {table: "rhs", col: "all", row: loid }

      annotations.push({
        type:"color_set", 
        set: [ lhsRow, lhs2Row, rhsRow ]
      })
      annotations.push({
        type: "arrow", 
        from: lhsRow, 
        to: lhs2Row
      })
    })
  }
  let lhs = makeWstTable(lhsObj),
    lhs2 = makeWstTable(lhs2Obj);
  let rhs = null;
  if (rhsObj) {
    rhs = makeWstTable(rhsObj);
  }
  return opStepVis(info.name, lhs, lhs2, rhs, annotations )
}

export function simple(opid, lineageData, addOns) {
  let child_opid = lineageData.op[opid][0];
  let lhs = makeWstTable(lineageData.results[child_opid])
  let rhs = makeWstTable(lineageData.results[opid])
  let info = lineageData.info[opid]

  addOns.desc = info.str
  addOns.opType = "simpleAggs"
  addOns.tableCaptions.lhs = lineageData.info[child_opid].name

  let annotations = []
  
  // color the rows
  R.times((row) => {
      let lrow = { table: "lhs", col: "all", row: row },
        rrow = { table: "rhs", col: "all", row: 0 }
      annotations.push({ type: "arrow", from: lrow, to: rrow })
  },  lhs.data.length)

  return opStepVis(info.name, lhs, null, rhs, annotations )
}


export function groupby(opid, lineageData, addOns) {
  let child_opid = lineageData.op[opid][0];
  let lhs = makeWstTable(lineageData.results[child_opid])
  let rhs = makeWstTable(lineageData.results[opid])
  let lineageObj = lineageData.row[opid]
  let info = lineageData.info[opid]

  addOns.desc = "Groups rows and merges their data: " + info.str
  addOns.opType = "hashGroupOp"
  addOns.tableCaptions.lhs = lineageData.info[child_opid].name

  let annotations = []
  let colors = R.times((i) => {
    return { 
      type: "color_set", 
      set: [ { table: "rhs", row: i, col: "all" } ] 
    }
  }, rhs.data.length)

  lineageObj.forEach(([srcid, pairs]) => {
    pairs.forEach(([oid, iid]) => {
      let lrow = { table: "lhs", col: "all", row: iid },
        rrow = { table: "rhs", col: "all", row: oid }
      annotations.push({ type: "arrow", from: lrow, to: rrow })
      colors[oid].set.push(lrow)
    })
  })
  annotations = annotations.concat(colors)

  return opStepVis(info.name, lhs, null, rhs, annotations )
}

export function filter(opid, lineageData, addOns) {
  console.log("filter", lineageData)
  let child_opid = lineageData.op[opid][0];
  let lhs = makeWstTable(lineageData.results[child_opid])
  let rhs = makeWstTable(lineageData.results[opid])
  let lineageObj = lineageData.row[opid]
  let info = lineageData.info[opid]
  console.log("info ", info)
  
  addOns.desc = "Filter - only keeping tuples meeting the condition: " + info.str
  addOns.opType = "filterOp"
  addOns.tableCaptions.lhs = "output of: " + lineageData.op[opid][0]

  let annotations = []
  let boxSet = []

  // the columns to outline
  // indexes of attributes in predicate
  //addExprAnnotations(opid, lineageData, annotations)
  let input_size = lhs.data.length;
  addLineageAnnotations(opid, lineageObj, annotations, input_size)
  annotations = boxSet.concat(annotations)
  return opStepVis(info.name, lhs, null, rhs, annotations )
}

// info:
//     str: str, name: str, schema: [str], id: str, depth: int
// op:
//    str: [str]
// results:
//    str: [[srcidx, [oid, iid]]]
export function scan(opid, lineageData, addOns) {
  console.log("scan", lineageData)
  let info = lineageData.info[opid];
  let lineageObj = lineageData.row[opid]
  let annotations = []
  let lhsObj = lineageData.results[opid+"input"]
  let rhsObj = lineageData.results[opid]
  if (!lhsObj)
    lhsObj = rhsObj;
  let lhs = makeWstTable(lhsObj)
  let rhs = makeWstTable(rhsObj)
  
  addOns.opType = "scanOp"
  addOns.tableCaptions.lhs = "table: " + info.table_name
  
  if (lineageObj) {
    addOns.desc = "Scans the tuples of the input table with filter pushed down" + info.str
    let input_size = lhs.data.length;
    addLineageAnnotations(opid, lineageObj, annotations, input_size)
  } else {
    addOns.desc = "Scans all the tuples of the input table " + info.str
    R.times((row) => {
      let lrow = {table: "lhs", row, col: "all"}
      let rrow = {table: "rhs", row, col: "all"}
      annotations.push({type:"color_set", set: [lrow, rrow]})
      annotations.push({type: "arrow", from: lrow, to: rrow})
    }, rhs.data.length)
  }

  if (opid in lineageData.col) {
    addProjectionAnnotations(opid, lhsObj, lineageData, annotations);
  }


  return opStepVis(info.name, lhs, null, rhs, annotations )
}


export function query(opid, lineageData, addOns) {
  let child_opid = lineageData.op[opid][0]
  let lhs = makeWstTable(lineageData.results[child_opid])
  let rhs = lhs;
  let info = lineageData.info[opid]
  addOns.opType = "queryOp"
  addOns.desc = "Query" + lineageData.qstr
  addOns.tableCaptions.lhs = info.name
  let annotations = []
  return opStepVis(info.name, lhs, null, null, annotations)
}

class Lineage {
  lineageData;

  constructor(lineageData) {
    this.lineageData = lineageData;
  }


}
