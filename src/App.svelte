<script lang="ts">


  import { tick, onMount } from "svelte"
  import QueryPlan from "./QueryPlan.svelte"
  import { ExtractLineage } from "./annotate.ts"
  import LineageDiagram from "./LineageDiagram.svelte"
  import QueryPicker from "./QueryPicker.svelte"
  import Bug from "./Bug.svelte"
  import * as initial_lineage from "./assets/lineage.json"
  import { initDuckDB, lineageData, selectedOpids } from "./stores.ts"
  import {generateUUID} from "./guid"
  import * as arrow from 'apache-arrow';

  const sessionID = generateUUID()

  $lineageData = initial_lineage;
  let conn = null;
  let db = null;
  let msgEl = document.getElementById("msg");
  let queryParams = new URLSearchParams(window.location.search)
  const url = 'http://127.0.0.1:5000';
  let errmsg = null;
  let qplanEl;
  let editorEl;
  let csvEl;
  let newTableName;
  let schemas = [];
  let addedCSVs = [];
  let q = queryParams.get("q") ?? `SELECT cid, name, sum(amount) FROM customers JOIN sales USING (cid) GROUP BY cid, name ORDER BY cid`;
  let csv = `a,b,c,d,e,f,g
0,0,0,0,a,2,c
1,1,1,0,b,4,d
2,2,0,0,c,6,e
3,3,1,0,d,8,cde
4,4,0,0,abc,10,a
5,0,1,0,cde,12,b
6,1,0,0,a,14,c
7,2,1,0,b,16,abc
8,3,0,0,c,18,c
9,4,1,0,d,20,d`;

  async function addTable() {
    console.log("addTable", conn, db);
    if (conn == null || db == null) return;

    if (newTableName) {
      let errorMessage = null;
      if (csv.split("\n").length > 15) {
        errorMessage = "CSV too large for the visualizer to be useful.  Please limit table to 15 rows."
      } else {
        try {
          console.log("Register Table:", newTableName, csv);
          await db.registerFileText(`data.csv`, csv);
          await conn.insertCSVFromPath('data.csv', {
            schema: 'main',
            name: `${newTableName}`,
            detect: true,
            header: true,
            delimiter: ','
            });
      
          await updateTablesList();

          addedCSVs.push({
            name: newTableName,
            csv
          });
          newTableName = null;
        } catch (e) {
          errorMessage = e;
        }
      }
      addedCSVs = addedCSVs;
    } else {
      errmsg = "New table needs a name!";
    }
  }

  async function onSQLSubmit(){
      // run q 
      await conn.query(`pragma enable_lineage`);
      await conn.query(`pragma enable_intermediate_tables`);
      const res = await conn.query(q);
      await conn.query(`pragma disable_intermediate_tables`);
      await conn.query(`pragma disable_lineage`);
      console.log(res);
      // Prepare query
      const stmt = await conn.prepare<{ v: arrow.string }>(
        `select query_id, plan from duckdb_queries_list() where query = ? order by query_id desc limit 1`
      );
      // ... and run the query with materialized results
      const metadata = await stmt.query<{ c1: arrow.Int, c2: arrow.string }>(q);
      const elements = await metadata.toArray().map((row) => row.toJSON());
      if (elements.length == 0) {
        console.log("Something is wrong in duckdb_queries_list ..", elements);
        return;
      }
      let plan_string = elements[0]["plan"];
      let qid = elements[0]["query_id"];
      console.log(plan_string, qid);
      try {
        const plan = JSON.parse(plan_string);
        console.log(plan);
        $lineageData = await ExtractLineage(conn, q, qid, plan);
      } catch (error) {
          console.error('Error parsing Plan JSON:', error);
      }
      
      const tables = await conn.query(`pragma show_tables`);
      for (const [index, element] of tables.toArray().entries()) {
        const table_name = element["name"];
        if (table_name.substring(0, 7) == "LINEAGE") {
          console.log(`Drop Index: ${index}, Element: ${element}, Name: ${table_name}`);
          await conn.query(`drop table ${table_name}`);
        }
      }
      
      await conn.query(`pragma clear_lineage`);
      
  }

  async function updateTablesList() {
    if (conn == null || db == null) return;
    const tables = await conn.query(`pragma show_tables`);
    const elements = await tables.toArray();
    let schemas_temp = [];
    for (const [index, element] of elements.entries()) {
      const table_name = element["name"];
      const tables_info = await conn.query(`pragma table_info(${table_name})`);
      let schema_str = "";
      for (const [j, e] of tables_info.toArray().entries()) {
        if (j != 0) {
          schema_str += `, `;
        }
        schema_str += `${e["name"]}:${e["type"]}`
      }
      schemas_temp.push([table_name, schema_str]);
    }
    schemas = schemas_temp;
  }

  async function init() {
    console.log("InitDuckDB Start")
    let res  = await initDuckDB();
    db = res[0];
    conn = res[1];
    console.log("InitDuckDB End")
    updateTablesList();
  }

  onMount(() => {
    init();
  })

  function reportBug(comment, email) {
    // TODO: send bug to backend
    //addStat(sessionID, q, JSON.stringify(addedCSVs), errmsg, true, comment, email)
  }

	$: cssVarStyles = `--editor-h:${$lineageData.plan_depth*50+100}px;`;
</script>

<style>
   /* Global styles */
  /* Heading styles */
  h1 {
    font-size: 28px;
    margin-bottom: 20px;
  }

  h3 {
    font-size: 20px;
    margin-bottom: 10px;
  }

  /*min-height: var(--editor-h, 30em);*/
  textarea.editor_sql {
    width: 100%;
    min-height: 10em;
    border: 1px solid black;
  }
  textarea.editor_csv {
    width: 100%;
    min-height: 10em;
    border: 1px solid black;
  }
  textarea {
  }
  .viscontainer,.errcontainer {
    margin-top: 3em;
  }
  small {
    font-size: 0.5em;
  }
  a:hover {
    background: var(--bs-highlight-bg);
  }
  .schema {
    padding-left: 0px;
    list-style: none;
  }
  .schema li {
    margin-bottom: .5em;
  }

  .bd-callout {
    padding: 1.25rem;
    margin-top: 1.25rem;
    margin-bottom: 1.25rem;
    background-color: var(--bd-callout-bg, var(--bs-gray-100));
    border-left: 0.25rem solid var(--bd-callout-border, var(--bs-gray-300));
  }

  .bd-callout-danger {
    --bd-callout-bg: rgba(var(--bs-danger-rgb), .075);
    --bd-callout-border: rgba(var(--bs-danger-rgb), .5);
  }

  .schema-list {
    max-height: 200px; /* Adjust the desired height */
    overflow-y: auto;
    border: 1px solid black;
    padding: 5px; /* Add padding for spacing */
  }

</style>

<Bug id="modalBug" reportBug={reportBug} />


<main class="container-fluid">
 <h1 class="display-4">
  SQLTutor Visualizes Query Execution <small><a href="https://github.com/cudbg/sqltutor">GitHub</a></small>
  </h1>
  
  <div class="row">
      <h3>About</h3>
      <p>
        <strong>SQLTutor</strong> visualizes each operator in the SQL query plan.
        Click on an operator to visualize its input and output tables, along with their row/column dependencies
        (called <a href="https://arxiv.org/abs/1801.07237">data provenance</a>).
        You can add new tables using the <mark>CSV</mark> textarea. The CSV should include a header row.
      </p>

      <p style="font-size: smaller;">
        <a href="#" class="link" data-bs-toggle="modal" data-bs-target="#modalBug">Report a bug or feature request</a>.
        <br/>
        <!--Want to help? Contact us!-->
      </p>
  </div> <!-- about section -->

      <div class="row">
          <div class="col">
            <h3>Tables</h3>
            <div class="schema-list">
              <ul class="schema">
                {#each schemas as [name, schema]}
                <li><b>{name}</b>({schema})</li>
                {/each}
              </ul>
            </div> <!--schema-list-->
          </div><!--tables column-->

          <div class="col-md-4">
            <h3>CSV
              <small><input bind:value={newTableName} placeholder="New Table Name" /></small>
            </h3>
            <textarea class="editor_csv" id="csv" bind:this={csvEl} bind:value={csv}></textarea>
            <button class="btn btn-primary" on:click={addTable} style="width:100%;">Add Table</button>
          </div><!--CSV column-->
      </div><!--row-->

<div class="row">
  <h3>
    SQL
  </h3>
  <textarea class="editor_sql" id="q" bind:this={editorEl} bind:value={q}  style="{cssVarStyles}"></textarea>
  <button class="btn btn-primary" on:click={onSQLSubmit} style="width:100%;">Visualize Query</button>
</div>

{#if $lineageData}
<div class="row viscontainer">
  <div class="row" bind:this={qplanEl}>
    <QueryPlan h={$lineageData.plan_depth * 50 + 100} />
  </div>

</div>

<div class="row viscontainer">
    <LineageDiagram opids={$selectedOpids} />
</div>
{/if}
{#if errmsg}
<div class="row errcontainer">
  <div class="col">
    <div class="bd-callout bd-callout-danger" role="alert">
      <h3>
        Could Not Parse Query
        <small>
          <a href="#" data-bs-toggle="modal" data-bs-target="#modalBug">report bug</a>
        </small>
      </h3>
      <pre>{errmsg}</pre>
    </div>
  </div>
</div>
{/if}

<div class="row footer" style="margin-top: 3em;">
  <div class="col-md-8 offset-md-2 text-center" style="border-top: 1px solid grey;">
    <p style="font-size:smaller;">
      See <a href="https://github.com/cudbg/sqltutor">github repo</a> for code.
      Implemented using
      <a href="https://github.com/cudbg/smokedduck">DuckDB version x</a>
      and table vis from <a href="https://pandastutor.com/">pandastutor</a>.
    </p>

    <p style="font-size:smaller;">
      Privacy Policy: By using SQLTutor, your queries, CSVs, user interactions, and IP address are logged and may be
      analyzed for research purposes
      </p>
    </div>
  </div>
</main>
