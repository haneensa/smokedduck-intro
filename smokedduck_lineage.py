# TODO: 1. simple aggregate column lineage
#       2. top n and limit 
#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy
import json

def runQuery(con, q, qname):
    json_fname = "{}.json".format(qname)
    con.execute("PRAGMA enable_profiling = 'json';")
    con.execute("PRAGMA profiling_output = '{}';".format(json_fname))

    con.execute("PRAGMA trace_lineage = 'ON';")
    con.execute("PRAGMA intermediate_tables = 'ON';")
    df = con.execute(q).fetchdf() 
    con.execute("PRAGMA intermediate_tables = 'OFF';")
    con.execute("PRAGMA trace_lineage = 'OFF';")
    con.execute("PRAGMA disable_profiling;")
    qid_df = con.execute("SELECT query_id FROM duckdb_queries_list() WHERE query=?", [q]).fetchdf()
    qid = qid_df['query_id'][0]
    
    # read query plan file
    with open(json_fname, 'r') as f:
        plan = json.load(f)
    return df, qid, plan


# get column mapping for each operator to filter intermediates
def OperatorName(name):
    return "_".join(name.split("_")[:-1])
    
def getColumnLevelLineage(qid : int, plan : dict):   
    """
    return operator level column lineage:
    "original": str, # plan["extra_info"]
    "table_name":  str, # table_name
    "alias":  [str], # [alias(col) for each col in current operator]
    "column_ref": [int] # [indexFromChild(col) for each col in current operator]
    """
    if "extra_info" not in plan:
        return {}
    
    extra_info = plan["extra_info"].split("[INFOSEPARATOR]")
    results = {"original": extra_info}
    op_name = OperatorName(plan['name'])    
    
    if len(plan["children"]) > 0:
        results["table_name"] = plan['children'][0]["name"]

    if op_name == "SEQ_SCAN":
        # Schema: table_name\nINFOSEPARATOR\ncolname_i for i in projection_list\nINFOSEPARATOR\nFilters: ..
        results["table_name"] = extra_info[0].strip()
        if len(extra_info) > 1:
            cols =  extra_info[1].strip().split("\n")
            results["alias"] = [x.split("#DEL#")[0] for x in cols]
            results["col_ref"] = [x.split("#DEL#")[1].split(',') for x in cols]
            results["col_ref"] = [[int(i) for i in inner_lst] for inner_lst in results["col_ref"]]
        if len(extra_info) > 2:
            results["filter_push"] = 1
    elif op_name in ["FILTER", "ORDER_BY", "LIMIT"]:
        # TODO: filter does have projection push down. handle it
        if "alias" in plan["children"][0]["column_lineage"]:
            results["alias"] = plan["children"][0]["column_lineage"]["alias"]
            results["col_ref"] = plan["children"][0]["column_lineage"]["col_ref"]
    elif op_name == "PROJECTION":
        # Schema: hasFunction\nINFOSEPARATOR\nalias_i.col_1,col_2,..,col_n.name_i for i in projection_list
        cols =  extra_info[1].strip().split("\n")
        results["alias"] = [x.split("#DEL#")[0] for x in cols]
        results["col_ref"] = [x.split("#DEL#")[1].split(',') for x in cols]
        results["col_ref"] = [[int(i) for i in inner_lst if i.isdigit()] for inner_lst in results["col_ref"]]
    elif op_name in ["HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY"]:
        aggs = extra_info[0].strip().split("\n")

        if len(plan["children"]) > 0:
            child_col_lineage = plan["children"][0]["column_lineage"]["alias"]
            gb_keys = [child_col_lineage[int(x[1:])] for x in aggs if x.startswith("#")]
            aggs_cols = [x for x in aggs if not x.startswith("#")]
        else:
            gb_keys = [x for x in aggs if x.startswith("#")]
            aggs_cols = [x for x in aggs if not x.startswith("#")]
        results["alias"] = gb_keys + aggs_cols
        results["col_ref"] = [i for i in range(len(results["alias"]))]
    elif op_name == "SIMPLE_AGGREGATE":
        cols = extra_info[0].strip().split("\n")
        results["alias"] = [x.split("#DEL#")[0] for x in cols]
        results["col_ref"] = [x.split("#DEL#")[1].split(',') for x in cols]
        results["col_ref"] = [[int(i) for i in inner_lst if i.isdigit()] for inner_lst in results["col_ref"]]
    elif op_name == "HASH_JOIN":
        results["table_name"] += "+" + plan['children'][1]["name"]
        results["alias"] = []
        right_projection = extra_info[1].strip()
        lchild_col_lineage = plan["children"][0]["column_lineage"]
        rchild_col_lineage = plan["children"][1]["column_lineage"]
        right_alias, left_alias = [], []
        if "alias" in rchild_col_lineage:
            right_alias = rchild_col_lineage["alias"]
        if "alias" in lchild_col_lineage:
            left_alias = lchild_col_lineage["alias"]
        results["col_ref"] = [i for i in range(len(left_alias) + len(right_alias) )]
        results["alias"] += left_alias
        
        if len(right_projection) > 0:
            for i in right_projection.split('\n'):
                col = right_alias[int(i[1:])]
                results["alias"] += [col]
        else:
            results["alias"] += right_alias
        
    return results
    
def getLineagePerOperatortoPlan(con, qid, plan):
    # traverse the query plan, and 
    op_name = plan['name']
    l_name = "LINEAGE_{}_{}".format(qid, op_name)        
    ## for now, just merge data using python.
    ## TODO: enable querying logical operator lineage
    op_name = OperatorName(op_name)    
    if  op_name in ["SEQ_SCAN", "ORDER_BY", "FILTER", "LIMIT"]:
        extra_info = plan["extra_info"].split("[INFOSEPARATOR]")
        if op_name == "SEQ_SCAN" and len(extra_info) < 2:
            # one to one mapping, no need to persist
            return []
        else:
            l_name = l_name + "_0"
            op_lineage = con.execute("select * from {}".format(l_name)).fetchdf()[["in_index", "out_index"]]
    elif op_name in ["HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY"]:
        sink = con.execute("select * from {}".format(l_name+"_0")).fetchdf()
        src = con.execute("select * from {}".format(l_name+"_1")).fetchdf()
        op_lineage = pd.merge(sink, src, how='inner', left_on='out_index', right_on='in_index')
        op_lineage = op_lineage[["in_index_x", "out_index_y"]].rename({'in_index_x':'in_index', 'out_index_y':'out_index'}, axis=1)
    elif op_name in ["HASH_JOIN"]:
        sink = con.execute("select * from {}".format(l_name+"_0")).fetchdf()
        src = con.execute("select * from {}".format(l_name+"_1")).fetchdf()
        op_lineage = pd.merge(sink, src, how='inner', left_on='out_index', right_on='rhs_index')
        op_lineage = op_lineage[["in_index", "lhs_index", "out_index_y"]].rename({"in_index":"rhs_index", "out_index_y":"out_index"}, axis=1)
    elif op_name in ["CROSS_PRODUCT", "INDEX_JOIN", "PIECEWISE_MERGE_JOIN", "NESTED_LOOP_JOIN", "BLOCKWISE_NL_JOIN"]:
        op_lineage = con.execute("select * from {}".format(l_name+"_1")).fetchdf()
    else:
        return []

    return op_lineage
    
def getIntermediates(con, qid, plan):
    # start from leaf node, get base table, then merge and propagate intermediate table 
    name = plan['name']
    l_name = "LINEAGE_{}_{}_100".format(qid, name)        
    op_name = OperatorName(name)    
    base_table = []
    if op_name == "SEQ_SCAN":
        table_name = plan["column_lineage"]["table_name"]
        base_table = con.execute("SELECT * FROM {}".format(table_name)).fetchdf()
        # TODO: check the case where input == output and avoid duplicating data
    if op_name == "ORDER_BY":
        input_df = plan["children"][0]["output"]
        intermediate = pd.merge(plan["lineage"], input_df, how='inner', left_on='in_index',  right_index=True, ).sort_values(by="out_index").drop(columns=["out_index", "in_index"])
    else:
        intermediate = con.execute("select * from {}".format(l_name)).fetchdf()
    return intermediate, base_table

# column: operator_id : operator_id_column_lineage for each operator in plan
# row: operator_id : operator_id_row_lineage for each operator in plan

# operator_id_row_lineage: 
# [ [oid, iid] for each tuple ]

# operator_id_column_lineage: array[array] 
# where the index is the index of the dst table
# and the inner array has a list of src columns that map to the dst
def getRowLineage(qid, plan, depth, lineage_json):
    """
    {opid: Array[ Array[srcidx, Array[oid, iid]] ] 
    """ 
    if "lineage" not in plan:
        return {}
    if len(plan['lineage']) == 0:
        return {}
  
    lineage_out = {}
    lineage = []
    op_name = OperatorName(plan["name"])    
    if op_name in ["HASH_JOIN", "CROSS_PRODUCT", "INDEX_JOIN", "PIECEWISE_MERGE_JOIN", "NESTED_LOOP_JOIN", "BLOCKWISE_NL_JOIN"]:
        lineage_lhs = [[int(row['out_index']), int(row['lhs_index'])] for index, row in plan["lineage"].iterrows()]
        lineage_rhs = [[int(row['out_index']), int(row['rhs_index'])] for index, row in plan["lineage"].iterrows()]
        lineage_out[plan["name"]] = [[0, lineage_lhs]]
        lineage_out[plan["name"]].append([1, lineage_rhs])
    else:
        if (op_name == "SEQ_SCAN" and "filter_push" not in plan["column_lineage"]) or len(plan["lineage"]) == 0:
            return {}
        else:
            lineage = [[int(row['out_index']), int(row['in_index'])] for index, row in plan["lineage"].iterrows()]
            lineage_out[plan["name"]] = [[0, lineage]]
    return lineage_out
        
def getInfo(qid, plan, depth, lineage_json):
    """
    return info : dict(opid : opinfo)

    opinfo:
        "name": str, # operator name
        "str": str, # operator describtion
        "schema":  [str], # [alias(col) for each col in current operator]
        "id": str, # operator unique id
        "depth": int, # operator depth within the query plan
        "table_name"? : str, # if "name" is SEQ_SCAN

    "col": dict(opid : column_ref)
        "column_ref": [int] # [indexFromChild(col) for each col in current operator]

    results: dict(opid : input)
        "input": [], # row input data to this operator
    """
    info = {}
    opid = plan['name']
    op_name = OperatorName(opid)    
    info["str"] = plan["extra_info"]

    info["name"] = op_name

    if op_name == "SEQ_SCAN":
        info["table_name"] = plan["column_lineage"]["table_name"]
    
    if 'column_lineage' in plan and "alias" in plan['column_lineage']:
        info["schema"] = plan['column_lineage']["alias"]

    info["id"] = opid
    info["depth"] = depth
    
    return {opid : info}
     
def getResults(qid, plan, depth, lineage_json):
    res = {}
    if "input" in plan:
        base = plan["input"]
        col_names = list(base.columns)
         
        # Convert datetime columns to strings
        for col in list(base.columns):
            base[col] = base[col].astype(str)
            
        base_rows = [row.values.tolist() for index, row in base.iterrows()]
        res[plan["name"]+"input"] = {"rows": base_rows, "columns": col_names}
    
    if "output" in plan:
        base = plan["output"]
        col_names = list(base.columns)
         
        # Convert datetime columns to strings
        for col in list(base.columns):
            base[col] = base[col].astype(str)
            
        base_rows = [row.values.tolist() for index, row in base.iterrows()]
        if "alias" in plan['column_lineage']:
            alias = plan['column_lineage']["alias"]
        else:
            alias = col_names
        
        res[plan["name"]] = {"rows": base_rows, "columns": alias}
        
    return res

def serializeLineage(qid, plan, depth=0, lineage_json={}):
     # traverse the query plan, and 
    if depth == 0:
        # setup query level summary
        lineage_json["qstr"] = plan["extra-info"]
        lineage_json["exprs"] = {}
        lineage_json["plan_depth"] = 0
        
        lineage_json["op"] = {"root" : [plan["children"][0]["name"]]}
        # "col": {"233": [[0, 1], [2], [3]], "236": [[0], [4], [3]]}, 
        lineage_json["col"] = {}
        # "exprs": {"280": [[0, [0]], [1, [1]]]},
        lineage_json["exprs"] = {}
        
        # intermediate tables
        lineage_json["results"] = {}
        # setup info:
        lineage_json["info"] = {}
        
        lineage_json["row"] = {}
    else:
        lineage_json["op"][plan["name"]] = [c["name"] for c in plan['children']]
        info = getInfo(qid, plan, depth, lineage_json)
        lineage_json["info"].update(info)
        
        res = getResults(qid, plan, depth, lineage_json)
        lineage_json["results"].update(res)
        
        if "col_ref" in plan["column_lineage"]:
            col_ref = plan['column_lineage']["col_ref"]
            lineage_json["col"][plan['name']] = col_ref

        lineage_out = getRowLineage(qid, plan, depth, lineage_json)
        lineage_json["row"].update(lineage_out)

    lineage_json["plan_depth"] = max(depth, lineage_json["plan_depth"])
    
    depth += 1    
    
    for idx, c in enumerate(plan['children']):  
        serializeLineage(qid, c, depth, lineage_json)

    return lineage_json

                        
def extractMetadata(con, qid, plan, depth=0):
    depth += 1    
    for idx, c in enumerate(plan['children']):  
        c["lineage"] = getLineagePerOperatortoPlan(con, qid, c)
        
        extractMetadata(con, qid, c, depth)

        c["column_lineage"] = getColumnLevelLineage(qid, c)
        print("column lineage for {} ".format(c["name"]), c["column_lineage"])
        
        intermediate, base_table = getIntermediates(con, qid, c)
        c["output"] = intermediate
        if len(base_table) > 0:
            c["input"] = base_table

        #print("Intermediates for {} ".format(c['name']), c["output"])
            

## Done - Add leaf node to indicate scan from base table since SEQ_SCAN can have filter pushed down
## add projection list for operators that have projection push down ("e.g. scan, join, filter")


def process(con, q, qname):
    print("**********{}**********".format(qname))
    print(q)
    df, qid, plan = runQuery(con, q, qname)
    print("\noutput:\n", df)
    print("\nPlan:\n", plan)
    lineage_json = {}
    extractMetadata(con, qid, plan, 0)
    lineage_json = serializeLineage(qid, plan, 0, {})
    #print("\nlineage json:\n", lineage_json)

    with open('{}.json'.format(qname), 'w') as outfile:
        json.dump(lineage_json, outfile)

