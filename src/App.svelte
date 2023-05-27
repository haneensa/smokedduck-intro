<script lang="ts">

  import { tick, onMount } from "svelte"
  import QueryPlan from "./QueryPlan.svelte"
  import LineageDiagram from "./LineageDiagram.svelte"
  import QueryPicker from "./QueryPicker.svelte"
  import Bug from "./Bug.svelte"
  import * as initial_lineage from "./assets/lineage.json"
  import { lineageData, selectedOpids } from "./stores.ts"
  import {generateUUID} from "./guid"
  const sessionID = generateUUID()

  $lineageData = initial_lineage;

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
  let q = queryParams.get("q") ?? ``;
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

  function getSchema() {
    try {
      return fetch(url+"/schema", {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
      }).then(response => {
        if (response.ok) {
          $: errmsg = null;
          return response.json()
        } else {
          console.error('Error:', response.status);
          $: errmsg = 'Error: ' + response.status;
        }
      }).then(jsonData => {
        // Process the JSON data as needed
        // For example, you can access properties using jsonData.propertyName
        console.log("schema", jsonData)
        $: schemas = jsonData;
      })
    } catch (error) {
      console.error('Error:', error);
      $: errmsg = 'Error: ' + error;
    }
  }
  
  function RegisterCSV(table_name, csv) {
    const data = {'name': table_name, 'csv':csv};
    try {
      return fetch(url+"/csv", {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
      }).then(response => {
        if (response.ok) {
          $: errmsg = null;
          return response.json();
        } else {
          console.error('Error:', response.status);
          $: errmsg = 'Error: ' + response.status;
        }
      }).then(jsonData => {
        return jsonData; // Return the JSON data to the caller
      })
    } catch (error) {
      console.error('Error:', error);
      $: errmsg = 'Error: ' + error;
    }
  }

  function getQueryLineage(q) {
    const data = {'query': q};
    try {
      return fetch(url+"/sql", {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
      }).then(response => {
        if (response.ok) {
          $: errmsg = null;
          return response.json();
        } else {
          console.error('Error:', response.status);
          $: errmsg = 'Error: ' + response.status;
        }
      }).then(jsonData => {
        $lineageData = jsonData;
        console.log("lineageData", lineageData);
        return jsonData; // Return the JSON data to the caller
      })
    } catch (error) {
      console.error('Error:', error);
      $: errmsg = 'Error: ' + error;
    }
  }

  async function addTable() {
    if (newTableName) {
      try {
        if (csv.split("\n").length > 15) {
          $: errmsg = "CSV too large for the visualizer to be useful.  Please limit table to 15 rows."
          return
        }
        RegisterCSV(newTableName, csv)
        addedCSVs.push({
          name: newTableName,
          csv
        })
      } catch(e) {
        $: errmsg = e;
      }
      newTableName = null;
      getSchema()
      $: addedCSVs = addedCSVs
    } else {
      $: errmsg = "New table needs a name!"
    }
  }

  function onSQLSubmit(){
    $: {
      getQueryLineage(q);
    }
  }

  function onSelectQuery(query) {
    console.log("selected dropdown", query)
    $: q = query
  }

  onMount(() => {
    getSchema();
  })

  function reportBug(comment, email) {
    // TODO: send bug to backend
    //addStat(sessionID, q, JSON.stringify(addedCSVs), errmsg, true, comment, email)
  }

</script>

<style>
  textarea.editor_sql {
    width: 100%;
    min-height: 10em;
    border: 1px solid black;
  }
  textarea.editor_csv {
    width: 100%;
    min-height: 20em;
    border: 1px solid black;
  }
  textarea {
    font-family: monospace;
  }
  .loading {
    text-align: center;
    padding: 10em;
    display: none;
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

</style>

<Bug id="modalBug" reportBug={reportBug} />


<main class="container-xxl">
  <h1>
    SQLTutor Visualizes Query Execution <small><a href="https://github.com/cudbg/sqltutor">github</a></small>
  </h1>
  <div class="row">
      <h3>About</h3>
      <p>
      <strong>SQLTutor</strong> visualizes each operator in the SQL query plan.  
      Click on an operator to visualize its input and output tables, along with their row/column dependencies (called <a href="https://arxiv.org/abs/1801.07237">data provenance</a>) .  
      You can add new tables using the <mark>CSV</mark> textarea.  The CSV should include a header row.
      Use <mark>&leftarrow;</mark> and <mark>&rightarrow;</mark> to visualize the prev/next operator.  </p>

      <p style="font-size: smaller;">
      <!--<a target="_blank" href={`https://docs.google.com/forms/d/e/1FAIpQLSeqdk3ZqQms92iaGq5rKV6yUdnhLcRllc8igQPl1KGUwfCEUw/viewform?usp=pp_url&entry.351077705=${encodeURI(q)}&entry.1154671727=${encodeURI(csv)}&entry.1900716371=${encodeURI(errmsg)}`} class="link">Report a bug</a>. -->
      <a href="#" class="link" data-bs-toggle="modal" data-bs-target="#modalBug">Report a bug or feature request</a>. 
      <br/>
      <a href="https://cudbg.github.io/sql2pandas/" class="link">Learning Pandas too?  Check out sql2pandas</a>
      <!--Want to help? Contact us!-->
      </p>
  </div>
  <div class="row">
    <div class="col-md-9">
      <h3>Tables</h3>
      <ul class="schema">
        {#each schemas as [name, schema] }
          <li><b>{name}</b>({schema})</li>
        {/each}
      </ul>
    </div>

    <div class="col-md-3">
      <h3>CSV 
        <small><input bind:value={newTableName} placeholder="New Table Name"/></small> </h3>
      <textarea class="editor_csv" id="csv" bind:this={csvEl} bind:value={csv} />
      <button class="btn btn-primary" on:click={addTable} style="width:100%;">Add Table</button>
    </div>

  </div>
  <div class="row">
      <h3>
        SQL
        <small><QueryPicker onSelect={onSelectQuery}/></small>
      </h3>
      <textarea class="editor_sql" id="q" bind:this={editorEl} bind:value={q} />
      <button class="btn btn-primary" on:click={onSQLSubmit} style="width:100%;">Visualize Query</button>
  </div>


    {#if $lineageData}
    <div class="row viscontainer">
      <div class="col-md-4" bind:this={qplanEl}>
        <QueryPlan h={$lineageData.plan_depth*50+100}   />
      </div>
      <div class="col-md-8">
        <LineageDiagram opids={$selectedOpids}  />
      </div>
    </div>
    {/if}
    {#if errmsg}
    <div class="row errcontainer">
      <div class="col-md-12">
        <div class="bd-callout bd-callout-danger" role="alert">
          <h3>
            Could Not Parse Query <small>
              <!--<a target="_blank" href={`https://docs.google.com/forms/d/e/1FAIpQLSeqdk3ZqQms92iaGq5rKV6yUdnhLcRllc8igQPl1KGUwfCEUw/viewform?usp=pp_url&entry.351077705=${encodeURI(q)}&entry.1154671727=${encodeURI(csv)}&entry.1900716371=${encodeURI(errmsg)}`} class="link">report bug</a>-->
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
        <a href="">DuckDB version x</a>
        and table vis from <a href="https://pandastutor.com/">pandastutor</a>.
      </p>

      <p style="font-size:smaller;">
      Privacy Policy: By using SQLTutor, your queries, csvs, user interactions, and IP address are logged and may be analyzed for research purposes. Nearly all web services collect this basic information from users in their server logs. SQLTutor does not collect any personally identifiable information from its users. 
      </p>


    </div>
  </div>




</main>


