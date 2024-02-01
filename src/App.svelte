<script lang="ts">

  import LineageStep from "./LineageStep.svelte"
  import LineageExample from "./LineageExample.svelte"
  import { tick, onMount } from "svelte"
  import QueryPlan from "./QueryPlan.svelte"
  import { ExtractLineage } from "./annotate.ts"
  import QueryPicker from "./QueryPicker.svelte"
  import Bug from "./Bug.svelte"
  import * as initial_lineage from "./assets/lineage.json"
  import { initDuckDB, lineageData, selectedOpids } from "./stores.ts"
  import {generateUUID} from "./guid"
  import * as arrow from 'apache-arrow';
  
  import { ConvexHttpClient } from "convex/browser";
  import { api } from "../convex/_generated/api.js";
  
  const client = new ConvexHttpClient(import.meta.env.VITE_CONVEX_URL);
  window.client = client;

  const sessionID = generateUUID()

  $lineageData = initial_lineage;
  let visEl;
  let queryParams = new URLSearchParams(window.location.search)
  let qplanEl;
  let newTableName;
  let q = queryParams.get("q") ?? `SELECT cid, name, sum(amount) FROM customers JOIN sales USING (cid) GROUP BY cid, name ORDER BY cid`;

  onMount(() => {
  })

  function reportBug(comment, email) {
  }

	$: cssVarStyles = `--editor-h:${$lineageData.plan_depth*50+100}px;`;
</script>

<style>
   /* Global styles */
  /* Heading styles */
  .row {
    text-align: left;
  }

  /* Add space below h2 */
  h2 {
      margin-bottom: 20px; /* Adjust the value as needed */
  }

  /* Add space above h3 */
  h3 {
      margin-bottom: 20px; /* Adjust the value as needed */
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
 <h1 class="display-5 text-start">SmokedDuck  <small><a href="https://github.com/cudbg/smokedduck/" target="_blank">GitHub</a></small> </h1>
  
  <div class="row ">
      <h2>Introduction</h2>
      <p>
      There is a growing recognition of the importance of data governance <a href="https://dl.acm.org/doi/10.1145/3524284" target="_blank">[1]</a>
      which involves managing, securing, and ensuring the quality and compliance of organizational data.
      An important piece is lineage, which maintains the relationship between data programs’ input and output.
      </p>
      <p>
      But, today’s lineage solutions track dependencies at the granularity of files and tables. 
      This means a user can track which files/tables an output of a data program depends on.
      Its coarse grained nature (e.g. output of Q(R) depends on input relation R) limits its usage. 
      </p>
<div class="row">
    <div class="col-md-5 viscontainer">
      <p>
      For instance, how do you trace an error in a single row of data through your data warehouse back to its source(s)? How do you delete all rows of data about a user throughout your data lake? How do you figure out why an outlying value in your chart is so high?
      </p>
      <p>
      Fine-grained (row) provenance, also known as row-level lineage, quickly answers these questions and more [
<a href="https://dspace.mit.edu/handle/1721.1/132280" target="_blank">2</a>, 
<a href="http://www.vldb.org/pvldb/vol6/p553-wu.pdf" target="_blank">3</a>,
<a href="https://www.vldb.org/conf/2004/RS22P1.PDF" target="_blank">4</a>,
<a href="https://arxiv.org/abs/1805.02622" target="_blank">5</a>].
      It tracks relationship between input and output of a data program at the row-level.
      The visualization on the right shows lineage as arrows and represent how tuples are combined to construct the output.   
      For example, the first two rows in Sales relation  were combined with the first row from Customers  and used to compute the first two rows in the output. 
      </p>
      <p>
      This rich class of metadata has historically been too expensive to capture for analytical workloads (>2−1000× [
<a href="http://www.cs.iit.edu/~dbgroup/bibliography/AF18.html" target="_blank">5</a> ,
<a href="https://arxiv.org/abs/1805.11517" target="_blank">6</a>]) - until today. We present SmokedDuck, the first fine-grained provenance research database built for scale.
      </p>
      <p>
      SmokedDuck is a fork of DuckDB instrumented to provide fine-grained provenance. It is designed to capture lineage with low overhead, with an average of 0.15x on TPCH workload lower that prior approaches, and fast enough to avoid blocking user’s interactions. We are releasing this experimental prototype to support researchers and academics in using lineage to support provenance-powered use cases. 
      </p>
    </div>
    

    <div class="col-md-2 viscontainer">
    </div>
    <div class="col-md-5 viscontainer">
      <div id="vis-example">
        {#each [$selectedOpids].flat() as opid}
          <LineageExample {opid}  />
        {/each}
      </div>
    </div>
</div>
      <p>
      SmokedDuck uses physical instrumentation to capture lineage. The current version instrumentation is limited to single threaded execution and supports all logical relational operators. The physical operators currently supported are: Hash Join, Piecewise Merge Join, Nested Loop Join, Block Nested Loop Join, Cross Product, Filter, Table Scan, Limit, Streaming Limit, Order By, Hash Aggregate, Perfect Hash Aggregate. 
      </p>

      <h2>Installation</h2>
      <p>
      <code>pip3 install smokedduck</code>
      </p>

      <h2>Python API</h2>
      <h3>Capturing Lineage</h3>
<p>
Fine-grained lineage tracks the relationships between inputs and outputs of a query. The following Python <a href="https://github.com/cudbg/smokedduck/blob/notebook_example/smokedduck/totalsalespercustomer.ipynb" target="_blank">notebook</a> illustrates how to use SmokedDuck Python API to capture and query. The notebook loads Customers and Sales relations, then computes the total amount paid by each customer as follow:
</p>

<div>
<code>
con.execute('SELECT cid, name, sum(amount) FROM customers JOIN sales USING (cid) GROUP BY cid, name ORDER BY cid', capture_lineage='lineage').df()
</code>
</div>

<p style="margin-top: 20px;">
The <code>execute()</code> takes the argument <code>capture_lineage='lineage'</code> 
 which enables lineage capture during query execution.
SmokedDuck uses an eager approach [<a href="https://arxiv.org/abs/1801.07237" target="_blank">8</a>] to capture lineage where, 
during query execution, it computes and stores dependencies between input and output records for each operator.  
</p>
  <h3>Querying Lineage</h3>
<p>
After evaluating the query, the user can access lineage for individual physical operators or access query level lineage.  We provide several extension APIs to access lineage output.
</p>
<p>
The most common use case is to find the input rows that contributed to a given set of output rows (backward tracing), or vice versa (forward tracing).  We support these using the <code>backward()</code> and <code>forward()</code> api calls.  For instance:
</p>
<p>
In some cases, you may want to know more details on how input rows were combined at each operator to derive each output row of interest.   The provenance literature defines many provenance models suitable for different use cases.   SmokedDuck supports APIs for the common models:  <code>Why()</code>, <code>Polynomial()</code>, <code>Lineage()</code> which format the lineage output based on different provenance models.
</p>
<p>
You can read more details about the provenance models and the API in the <a href="https://github.com/cudbg/smokedduck/blob/lineage_capture_v4/README.md#extensions" target="_blank">README.md</a>.
</p>

<h3>Visualizing Lineage</h3>
<p>
      The following visualization breaks down lineage for the example query and uses the <a href="https://github.com/haneensa/sqltutor" target="_blank">SQLTutor</a>
 application that we built to visualize the physical query plan and its captured lineage information. Play around with it see the data flow in any* DuckDB query!
</p>

{#if $lineageData}
<div class="col-md-7 viscontainer">
  <div class="row viscontainer">
    <div class="row" bind:this={qplanEl}>
      <QueryPlan h={$lineageData.plan_depth * 50 + 100} />
    </div>
  </div>
</div>

<div class="col-md-5 viscontainer">
  <div class="row viscontainer">
    <div id="vis" bind:this={visEl}>
      {#each [$selectedOpids].flat() as opid}
        <LineageStep {opid}  />
      {/each}
    </div>
  </div>
</div>

{/if}

<div class="row" style="margin-top: 30px;">
  <h2>Development</h2>
  <h3>Roadmap</h3>
  <div>
  <ul>
    <li>Add support for lineage capture and query with multi-threading execution</li>
    <li>Add support for all DuckDB’s physical operators (AsOf, IE, Positional joins, Window)</li>
    <li>Extend API with Provenance Polynomial Evaluation</li>
    <li>Provide SmokedDuck as DuckDB extension</li>
    <li>Support lineage compression</li>
  </ul>
  </div>
</div>


<div class="row footer" style="margin-top: 3em;">
  <div class="col-md-8 offset-md-2 text-center" style="border-top: 1px solid grey;">
    <p>
Please reach out to us if you're interested in using SmokedDuck or if you run into any issues.
You can reach us quickly by creating a GitHub issue or emailing us at <a href="mailto:smokedduck@gmail.com" target="_blank">smokedduck@gmail.com</a>
    </p>
    </div>
  </div>
</main>
