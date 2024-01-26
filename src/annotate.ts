function OperatorName(name) {
  let parts = name.split('_');
  // Remove the last part (which is assumed to be a number)
  parts.pop();
  // Join the remaining parts back together
  return parts.join('_');
}

function GetOpID(name) {
  let parts = name.split('_');
  return parts[parts.length - 1];
}
let Joins = ["HASH_JOIN", "CROSS_PRODUCT", "INDEX_JOIN", "PIECEWISE_MERGE_JOIN", "NESTED_LOOP_JOIN", "BLOCKWISE_NL_JOIN"];
let Filters = ["FILTER", "ORDER_BY", "LIMIT", "STREAMING_LIMIT"]

async function getLineage(conn, qid, name) {
  let tname = `LINEAGE_${qid}_${name}_0`;
  console.log("Get Lineage for ", tname);
  let op_name = OperatorName(name);
  if (op_name == "PROJECTION" || op_name == "UNGROUPED_AGGREGATE") return [];
  const ldata = await conn.query(`select * from ${tname}`);
  console.log(ldata.toArray().map((row) => row.toJSON()));
  const is_join = Joins.includes(op_name);
  if (is_join) {
    let lhs_data = [];
    let rhs_data = [];
    //  if seq_scan and filter_push not in column_lineage
    for (const [index, element] of ldata.toArray().entries()) {
      lhs_data.push([element["out_index"], element["lhs_index"]]);
      rhs_data.push([element["out_index"], element["rhs_index"]]);
    }
    return [[0, lhs_data], [1, rhs_data]];
  } else {
    let data = [];
    //  if seq_scan and filter_push not in column_lineage
    for (const [index, element] of ldata.toArray().entries()) {
      data.push([parseInt(element["out_index"]), parseInt(element["in_index"])]);
    }
    return [[0, data]];
  }
}

function getInfo(qid, node, name) {
  /*
  "name": str, # operator name
  "str": str, # operator describtion
  "schema":  [str], # [alias(col) for each col in current operator]
  "id": str, # operator unique id
  "depth": int, # operator depth within the query plan
  "table_name"? : str, # if "name" is SEQ_SCAN,
  */
  if (!node.hasOwnProperty("extra")) {
    return {"str": ""};
  }
    
  let extra_info = node["extra"].split("[INFOSEPARATOR]")
  let op_name = OperatorName(name);
  let opid = GetOpID(name);
  let results = {"str": "", "id": name,
    "display_name": op_name, "id_number": opid, "name": op_name,
    "schema": node["alias"]}

  results["table_name"] = node["table_name"];
  results["str"]  += node["str"];
  

  console.log("Info: ", op_name, results);
  return results;
}

function get_ref(agg_cols) {
  let out = [];
  for (const [index, element] of agg_cols.entries()) {
      // Use a regular expression to match all numbers with prefix #
      const regex: RegExp = /#(\d+)/g;
      const matches: RegExpMatchArray | null = element[0].match(regex);
      if (matches) {
          // Extracted numbers are in the second capturing group
          const numbers: number[] = matches.map(match => parseInt(match[1]));
          console.log("Extracted numbers:", numbers);
          output.push(numbers)
      } else {
          console.log("No numbers found.");
      }
  }
  return out;
}

function getColumnLevelLineage(qid, node, name) {
  if (!node.hasOwnProperty("extra")) {
    return {"str": ""};
  }
    
  let extra_info = node["extra"].split("[INFOSEPARATOR]")
  let op_name = OperatorName(name);
  let opid = GetOpID(name);

  let str : string = "";
  let col_ref : number[][] = [];
  let alias : string[] = [];
  let table_name : string = "";

  if (node["children"].length > 0) {
    table_name = node["children"][0]["name"];
  }
  
  if (op_name == "SEQ_SCAN") {
    str  = `Input: ${node["table"]}`
    table_name = node["table"];
    if (extra_info.length > 1 && extra_info.includes("#DEL")) {
      let cols = extra_info[1].trim().split("\n");
      console.log("---> ", cols, extra_info)
      alias = cols.map((x) => x.split("#DEL")[0]);
      let col_ref_str : string[] = cols.map((x) => x.split("#DEL")[1].split(','))
      col_ref = col_ref_str.map((x) => x.map((i) => parseInt((i.replace(/\D/g, '')))))
    }

    if (extra_info.length > 2) {
      str += (" | " + extra_info[2].trim().replace(/\n/g, ''));
    }
  } else if (Filters.includes(op_name)) {
    // get alias from child node
    alias = node["children"][0]["alias"];
    col_ref = node["children"][0]["col_ref"];
    str = node["extra"];
  } else if (op_name == "PROJECTION") {
    let cols = extra_info[1].trim().split("\n");
    // passed from child to parent
    alias = cols.map((x) => x.split("#DEL")[0]);
    let col_ref_str : string[] = cols.map((x) => x.split("#DEL")[1].split(','))
    col_ref = col_ref_str.map((x) => x.map((i) => parseInt((i.replace(/\D/g, '')))))
    if (!node["children"][0].hasOwnProperty("alias")) {
      let child_alias = node["children"][0]["alias"];
      // use index in original alias to access child alias name
      alias = child_alias.map((x) => 
        (x[0]=='#' && child_alias.length > parseInt(x.replace(/\D/g, ''))) 
        ? child_alias[parseInt(x.replace(/\D/g, ''))] : x);
    } 
    str = ` Cols: [${alias.join(',')}]`;
  } else if (op_name == "HASH_GROUP_BY" || op_name == "PERFECT_HASH_GROUP_BY") {
    let aggs = extra_info[0].trim().split("\n");
    let child_col_lineage = [];
    let gb_keys = [];
    let agg_cols = [];
    if (node["children"].length > 0) {
      child_col_lineage = node["children"][0]["alias"];
    }
    
    for (const [index, element] of aggs.entries()) {
      console.log(element);
      if (element[0] == "#") {
        let i = parseInt(element.replace(/\D/g, ''));
        if (child_col_lineage.length > i) {
          gb_keys.push(child_col_lineage[i]);
        } else {
          gb_keys.push(element);
        }
      } else {
        agg_cols.push(element);
      }
    }

    alias = gb_keys.concat(agg_cols);
    col_ref = Array.from({ length: gb_keys.length + 1 }, (_, index) => [index]);
    col_ref = col_ref.concat(get_ref(agg_cols));
    str = `Key: [${gb_keys.join(',')}]`;
    str += `, Aggs: [${agg_cols.join(',')}]`;
  } else if (op_name == "UNGROUPED_AGGREGATE") {
    let cols = extra_info[0].trim().split("\n");
    alias = cols.map((x) => x.split("#DEL")[0]);
    let col_ref_str : string[] = cols.map((x) => x.split("#DEL")[1].split(','))
    col_ref = col_ref_str.map((x) => x.map((i) => parseInt((i.replace(/\D/g, '')))))
  } else if (op_name != "HASH_JOIN" && Joins.includes(op_name)) {
    table_name += "+" + node["children"][1]["name"];
    //let join_cond = extra_info[0].trim().replace(/\n/g, '');
    str = extra_info; // join_cond;
    // left child
    if (node["children"][0].hasOwnProperty("alias")) {
      alias = node["children"][0]["alias"];
    }
    if (node["children"][1].hasOwnProperty("alias")) {
      alias = alias.concat( node["children"][1]["alias"] );
    }
    console.log("N", node)
    let n = alias.length;
    col_ref = Array.from({ length: n + 1 }, (_, index) => [index]);
  } else if (op_name == "HASH_JOIN") {
    table_name += "+" + node["children"][1]["name"];
    let right_projection = extra_info[1].trim().replace(/\n/g, '');
    let left = node["children"][0]; // TODO: handle delim join
    let left_alias = [];
    let right_alias = [];
    if (node["children"][0].hasOwnProperty("alias")) {
      left_alias = node["children"][0]["alias"];
    }
    if (node["children"][1].hasOwnProperty("alias")) {
      right_alias = node["children"][1]["alias"];
    }
    node["col_ref_left"] = Array.from({ length: left_alias.length + 1 }, (_, index) => [index]);
    alias = left_alias;
    node["col_ref_right"] = []
    str += extra_info; // join_cond;

    if (right_projection.length > 0) {
    } else {
      node["col_ref_right"] = Array.from({ length: right_alias.length + 1 }, (_, index) => [index]);
      alias = alias.concat(right_alias);
    }

    col_ref = Array.from({ length: alias.length + 1 }, (_, index) => [index]);
  }

  node["col_ref"] = col_ref;
  node["alias"] = alias;
  node["str"] = str;
  node["table_name"] = table_name;

  console.log(op_name, node);
  return col_ref;
}

async function getIntermediate(conn, node, tname) {
  const ldata = await conn.query(`select * from ${tname}`);
  let schemaArray: string[] | undefined;
  console.log(ldata.toArray().map((row) => row.toJSON()));
  let data = [];
  //  if seq_scan and filter_push not in column_lineage
  for (const [index, element] of ldata.toArray().entries()) {
    if (!schemaArray) {
      schemaArray = Object.keys(element);
    }
    const valuesArray = Object.values(element).map(String);
    data.push(valuesArray);
  }
  
  if (node.hasOwnProperty("alias") && node["alias"].length == schemaArray.length) {
    schemaArray = node["alias"];
  }

  return {"rows": data, "columns": schemaArray, "cardinality": data.length};
}

async function extractMetadata(conn, qid, node, depth, plan_data) {
  console.log(node["name"], depth);
  const name = node["name"];
  let op_name = OperatorName(name);
  depth += 1;
  plan_data["plan_depth"] = Math.max(depth, plan_data["plan_depth"]);

  // lineage per operator
  plan_data["row"][name] = await getLineage(conn, qid, name);

  // serialize tree
  if (node.children && Array.isArray(node.children)) {
    const children: string[] = [].concat(...node.children.map((child) => child.name));
    plan_data["op"][name] = children;
  }

  for (const child of node["children"]) {
    await extractMetadata(conn, qid, child, depth, plan_data);
  }


  // extract column information
  let col_ref  = getColumnLevelLineage(qid, node, name);
  if (col_ref.length > 0) {
    plan_data["col"][name] = col_ref;
  }

  plan_data["info"][name] = getInfo(qid, node, name);
  plan_data["info"][name]["depth"] = depth;
  
  // extract intermediate tables
  let tname = `LINEAGE_${qid}_${name}_100`;
  plan_data["results"][name] = await getIntermediate(conn, node, tname);

  if (op_name == "SEQ_SCAN") {
    tname = `${node["table"]}`;
    plan_data["results"][`${name}input`] = await getIntermediate(conn, node, tname);
  }
}

export async function ExtractLineage(conn,q, qid, plan) {
  // traverse plan
  // extract info: col info, lineage, intermediate data, etc.
  let plan_data = {};
  plan_data["op"] = {"root": [plan["name"]]};
  plan_data["qstr"] = q;
  plan_data["plan_depth"] = 0;
  plan_data["info"] = {};
  plan_data["row"] = {};
  plan_data["col"] = {};
  plan_data["exprs"] = {};
  plan_data["expr"] = {};
  plan_data["results"] = {};


  // addDelimJoinAnnotation(plan)
  await extractMetadata(conn, qid, plan, 0, plan_data);
  console.log("Lineage Data: ", plan_data, JSON.stringify(plan_data));
  return plan_data;
}
