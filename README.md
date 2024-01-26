# SQLTutor

<img src="https://github.com/cudbg/sqltutor/raw/main/screenshot.png" style="width:100%"></img>


[Svelte-based SQL visualizer](https://haneensa.github.io/sqltutor).  

Built on top of

* [svelte](https://svelte.dev)
* [SmokedDuck](https://github.com/cudbg/smokedduck) an extension of DuckDB, extended with cutting-edge [provenance instrumentation techniques](https://dl.acm.org/doi/abs/10.1145/3555041.3589731?casa_token=19Ke3CqDM6QAAAAA:pEJpJjX7CwDA8NMaEn41Uj_8ac72lepMlVZ_8lrkt-q3rgkG-xYht4UReTWjtkImmyxpGVYKhduu) compiled into [Wasm](https://github.com/duckdb/duckdb-wasm) module.
* Table visualizer from [pandastutor](https://pandastutor.com/)



Develop and Run

    npm install .
    npx convex dev
    npm run dev

If there's issues with convex

    npx convex init
    npx convex reinit --help

# Future Features

* [ ] support for multithreading
* [ ] example list of prewritten queries
* [ ] track usage statistics

# Specification for lineage data 


```ts
opid: id of operator
Pointers: Array<(outputTupleIdx, inputTupleIdx)>

RowLineage: Array<(sourceTableIndex, Pointers)>

// each element is the list of attribute offsets in the input table's schema
ColIndexes: Array<inputAttrIdx>

// one element for each attribute in the operator's output schema
ColLineage: Array<ColIndexes>

// list of child operator ids
OpLineage: Array<opid>  

// attributes used to filter/join
ExprCols: Array<(sourceTableIndex, ColIndexes)>

Info: {
  str: string // string representation to be displayed
  id: opid
  name: string // operator name
  schema: Array<string>  // attribute names of output schema
}

Tuple: Array<int | string>
Results: {
  columns: Array<string>  // attribute names
  rows: Array<Tuple>
}

Lineage: {
  row: {
    opid: RowLineage
  },
  col: {
    opid: ColLineage
  },
  op: { 
    // "root" is a special opid that stores the root opid in a singleton list
    opid: OpLineage  
  },
  exprs: {
    opid: ExprCols
  },
  info: {
    opid: Info
  },
  results: {
    opid: Results
  },
  qstr: string  // query string
}


```
