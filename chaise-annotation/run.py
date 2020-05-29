import csv, json, requests, sys
import numpy as np
from collections import defaultdict
import matplotlib.pyplot as plt


# ----------------------------- utility functions -------------------------- #
def sorted_default_dict(d):
    return sorted(d.items(), key=lambda x:x[1], reverse=True)

def table_name(*args):
    return args[0] + ":" + args[1]

def find_table_index(tables, t_name):
    for idx, t in enumerate(tables):
        if t == t_name:
            return idx
    return -1

# -------------- global variables used by different functions --------------/
empty_annots = [
    "tag:isrd.isi.edu,2016:generated",
    "tag:isrd.isi.edu,2016:immutable",
    "tag:isrd.isi.edu,2016:non-deletable",
    "tag:isrd.isi.edu,2018:required",
]

table_annotations = [
    "tag:misd.isi.edu,2015:display",
    "tag:isrd.isi.edu,2016:table-alternatives",
    "tag:isrd.isi.edu,2016:generated",
    "tag:isrd.isi.edu,2016:immutable",
    "tag:isrd.isi.edu,2016:non-deletable"
    "tag:isrd.isi.edu,2016:app-links",
    "tag:isrd.isi.edu,2016:table-display",
    "tag:isrd.isi.edu,2016:visible-columns",
    "tag:isrd.isi.edu,2016:visible-foreign-keys",
    "tag:isrd.isi.edu,2016:export",
    "tag:isrd.isi.edu,2019:export",
    "tag:isrd.isi.edu,2018:citation",
    "tag:isrd.isi.edu,2019:source_definitions",

]

column_annotations = [
    "tag:misd.isi.edu,2015:display",
    "tag:isrd.isi.edu,2016:column-display",
    "tag:isrd.isi.edu,2016:generated",
    "tag:isrd.isi.edu,2016:immutable",
    "tag:isrd.isi.edu,2018:required",
    "tag:isrd.isi.edu,2017:asset",
]

key_annotations = [
    "tag:misd.isi.edu,2015:display",
    "tag:isrd.isi.edu,2017:key-display",
]

fkey_annotations = [
    "tag:isrd.isi.edu,2016:foreign-key"
]

type_annots = {
    "table": table_annotations,
    "column": column_annotations,
    "key": key_annotations,
    "fkey": fkey_annotations
}


def add_annotation(type, annot_name, annot_val, counted_annot, most_used_annotations):
    """
    given an annotation, add it if it's not already counted
    """
    if annot_name in counted_annot:
        return True

    # make sure the annotation string is valid
    if annot_name not in type_annots[type]:
        return False

    #make sure the value is valid
    if not annot_val and annot_name not in empty_annots:
        return False

    counted_annot[annot_name] = 1
    most_used_annotations[annot_name] += 1
    return True

def get_schema_info(schema_file):
    """
    Given the location of schema_file, will read the whole JSON file and
    create the variables that are used in other places.
    most probably should be rewritten in a much better way :)
    """
    table_names = []
    table_fk_counts = []
    table_annot_counts = []
    ignored_table_names = []
    most_used_annotations = defaultdict(int)
    table_w_invalid_annots = defaultdict(int)
    table_column_counts = []
    constraints = {}
    with open("schema/" + schema_file) as schemafile:
        data = json.load(schemafile)
        for sch in data['schemas']:
            for t in data['schemas'][sch]['tables']:
                t_name = table_name(sch, t)

                if (sch in ['_ermrest', '_ermrest_history', '_acl_admin']
                   or sch in ['scratch', 'cirm_rbk', 'data_commons', 'etl_util', 'public', 'gudmap_meta', 'gudmap_raw', 'gudmap_submissions']
                   or t in 'wufoo' or sch in ['protwis_schema', 'protwis_mgmt', 'iobox_data', 'public']):
                   ignored_table_names.append(t_name)
                   continue

                table = data['schemas'][sch]['tables'][t]
                table_names.append(t_name)

                #------------------------ most used annot ---------------------#

                counted_annot = {}
                # table annotations
                for t_annot in table['annotations']:
                    if not add_annotation("table", t_annot, table['annotations'][t_annot], counted_annot, most_used_annotations):
                        table_w_invalid_annots[t_name] += 1

                # column annotations
                for c in table['column_definitions']:
                    if c['name'] not in ['RID', 'RMB', 'RCB', 'RMT', 'RCT']:
                        for c_annot in c['annotations']:
                            if not add_annotation("column", c_annot, c['annotations'][c_annot], counted_annot, most_used_annotations):
                                table_w_invalid_annots[t_name] += 1

                # key annotations
                for k in table['keys']:
                    for k_annot in k['annotations']:
                        if not add_annotation("key", k_annot, k['annotations'][k_annot], counted_annot, most_used_annotations):
                            table_w_invalid_annots[t_name] += 1

                # fkeys annotations
                for fk in table['foreign_keys']:
                    for fk_annot in k['annotations']:
                        if not add_annotation("fkey", fk_annot, fk['annotations'][fk_annot], counted_annot, most_used_annotations):
                            table_w_invalid_annots[t_name] += 1

                table_annot_counts.append(len(counted_annot))

                #-------------------- number of columns -------------------#
                table_column_counts.append(len(table['column_definitions']))

                #-------------------- number of foreignkeys -------------------#
                if "foreign_keys" not in table:
                    table_fk_counts.append(0)
                else:
                    table_fk_counts.append(len(table['foreign_keys']))

                #-------------------------- constraints -----------------------#
                if "foreign_keys" not in table:
                    continue

                # foreign_keys is an array
                for fk in table['foreign_keys']:
                    cons = fk['names'][0]
                    t1 = fk['foreign_key_columns'][0]['table_name']
                    s1 = fk['foreign_key_columns'][0]['schema_name']
                    t2 = fk['referenced_columns'][0]['table_name']
                    s2 = fk['referenced_columns'][0]['schema_name']

                    if cons[0] not in constraints:
                        constraints[cons[0]] = {}
                    constraints[cons[0]][cons[1]] = [table_name(s1, t1), table_name(s2, t2)]
    return [table_names, ignored_table_names, most_used_annotations, table_annot_counts, table_w_invalid_annots, table_fk_counts, table_column_counts, constraints]

# run the analysis
def get_chaise_usage(file_name, constraints, table_names, table_mapping={}, fk_mapping={}):
    """
    Find the tables that are used in the log requests (either as the end table or part of facet)
    table_mapping: since the csv files (log files) are based on historical data,
                   some table names might have been updated. I added this so I can
                   map multiple table names to one table.
    fk_mapping: the same concept as table_mapping but for foreign keys
    most probably should be rewritten in a much better way :)
    """
    used_tables = defaultdict(int)
    end_tables = defaultdict(int)
    invalid_tables = defaultdict(int)
    facet_cnt = 0
    invalid_facet_nodes = defaultdict(int)
    invalid_facet_cnt = 0
    invalid_table_cnt = 0
    row_cnt = 0

    with open("data/" + file_name, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)

        for row in csvreader:
            t_name = row['c_table']
            row_cnt += 1

            if t_name in table_mapping:
                t_name = table_mapping[t_name]

            # ignore the logs that don't have invalid table name
            if (t_name not in table_names):
                invalid_tables[t_name] += 1
                invalid_table_cnt += 1
                continue

            # add the c_table
            used_tables[row['c_table']] += 1
            end_tables[row['c_table']] += 1

            ok = True
            try:
                facets = json.loads(row['c_facet'])
            except ValueError:
                ok = False

            if not ok or not 'and' in facets:
                continue

            for facet in facets['and']:
                if not isinstance(facet['source'], list):
                    continue
                for facet_source_node in facet['source']:
                    if isinstance(facet_source_node, dict):
                        if "inbound" in facet_source_node:
                            c_name = facet_source_node['inbound']
                        elif "outbound" in facet_source_node:
                            c_name = facet_source_node['outbound']
                        else:
                            continue

                        facet_cnt += 1
                        if c_name[0] not in constraints or c_name[1] not in constraints[c_name[0]]:
                            node_name = c_name[0] + ":" + c_name[1]
                            if node_name in fk_mapping:
                                c_name = fk_mapping[node_name]
                            else:
                                print("row is: ", row)
                                invalid_facet_cnt += 1
                                invalid_facet_nodes[node_name] += 1
                                continue
                        f_tables = constraints[c_name[0]][c_name[1]]
                        used_tables[f_tables[0]] += 1
                        used_tables[f_tables[1]] += 1

    return [used_tables, invalid_tables, invalid_table_cnt, len(end_tables), facet_cnt, invalid_facet_cnt, invalid_facet_nodes, row_cnt]

def get_hist_data(arr, table_names, label, draw_diagram=False):
    """
    Reformat the data so it can be used for genrating a plot
    """
    arr_no_zero = list(filter(lambda x : x >0, arr))
    print("cnt w any:", len(arr_no_zero))
    arr_no_zero_a = np.array(arr_no_zero)

    # print("histogram:", np.histogram(table_fk_counts_no_zero, bins=10))

    print("median:", np.percentile(arr_no_zero_a, 50, interpolation='higher'), ", 95%:", np.percentile(arr_no_zero_a, 95, interpolation='higher'))
    max_val =np.max(arr_no_zero_a)
    max_tables = []
    for idx, el in enumerate(arr):
        if el == max_val:
            max_tables.append(table_names[idx])
    print("max: ", max_val, ", element max: ", max_tables)

    if draw_diagram:
        _ = plt.hist(arr_no_zero, bins='auto')
        plt.xlabel('number of ' + label)
        plt.ylabel('number of tables')
        plt.xticks(arr_no_zero)
        plt.show()

def get_unique_table_summary(cases):
    """
    The main function to run the rest of analysis.
    Sample input format:
    cases = [
      {
          "table_mapping": "refer to get_chaise_usage for more info",
          "fk_mapping": "refer to get_chaise_usage for more info",
          "schema_location": "the location of schema ",
          "file_names": ["location of log data"]
      }
    ]
    """
    for case in cases:
        table_names, ignored_table_names, most_used_annotations, table_annot_counts, table_w_invalid_annots, table_fk_counts, table_column_counts, constraints = get_schema_info(case['schema_location'])

        print("\n========================================== for",case['schema_location'],"==========================================")
        print("# all tables:", len(table_names), ", #ignored tables:", len(ignored_table_names))
        # print("ignored tables: ", ignored_table_names)

        print("\nfk details:")
        get_hist_data(table_fk_counts, table_names, "foreign keys")


        print("\nannot details:")
        get_hist_data(table_annot_counts, table_names, "annotations")
        # print("most used annotations:", sorted_default_dict(most_used_annotations))
        # print("cnt table w invalid annots:", len(table_w_invalid_annots))
        # print("table w all invalid annots:", [(t) for t in table_w_invalid_annots if table_annot_counts[find_table_index(table_names, t)] == 0])

        print("\ncolumn details:")
        get_hist_data(table_column_counts, table_names, "columns")

        # print("\nchaise usage stuff:")
        #
        # for file_name in case['file_names']:
        #     used_tables, invalid_tables, invalid_table_cnt, end_table_cnt, f_cnt, invalid_facet_cnt, invalid_facet_nodes, row_cnt = get_chaise_usage(file_name, constraints, table_names, case['table_mapping'], case['fk_mapping'])
        #
        #     # list of unused tables
        #     unused_tables = []
        #     for t in table_names:
        #         if t not in used_tables:
        #             unused_tables.append(t)
        #
        #     print("\n--------------------for",file_name,"--------------------")
        #     print("#requests:", row_cnt, ",#requests w invalid end table:", invalid_table_cnt,", #used end tables:", end_table_cnt, ", #used tables:", f'{len(used_tables):,}', ", #processed facet node:", f_cnt, ", #invalid facet nodes: ", invalid_facet_cnt)
        #     # print("\ninvalid_tables, cnt:", len(invalid_tables),"list:", [*invalid_tables])
            # print("\ninvalid facet nodes:", invalid_facet_nodes)


# uncomment the following to run the annotation analysis
# get_unique_table_summary([
#     {
#         "table_mapping": {"Protocol:Keyword": "Vocabulary:Keyword", "Vocab:Species": "Vocabulary:Species", "Gene_Expression:Image_Scene": "Gene_Expression:Processed_Image"},
#         "fk_mapping": {
#             "Gene_Expression:Image_Scene_Image_fkey": ["Gene_Expression", "Processed_Image_Reference_Image_fkey"],
#             "Cell_Line:Reporter_Cell_Line_NCBI_GeneID_fkey": ["Cell_Line", "Reporter_Cell_Line_Gene_Reporter_Cell_Line_fkey"]
#         },
#         "schema_location": "rbk.json",
#         "file_names": ["rbk_all.csv", "rbk_recordset.csv", "rbk_record.csv", "rbk_recordedit.csv"]
#     },
#     {
#         "table_mapping": {},
#         "fk_mapping": {},
#         "schema_location": "facebase.json",
#         "file_names": ["facebase_all.csv", "facebase_recordset.csv", "facebase_record.csv", "facebase_recordedit.csv"]
#     },
#     {
#         "table_mapping": {},
#         "fk_mapping": {},
#         "schema_location": "synapse.json",
#         "file_names": ["synapse_all.csv", "synapse_recordset.csv", "synapse_record.csv", "synapse_recordedit.csv"]
#     },
#     {
#         "table_mapping": {},
#         "fk_mapping": {},
#         "schema_location": "cirm.json",
#         "file_names": ["cirm_all.csv", "cirm_recordset.csv", "cirm_record.csv", "cirm_recordedit.csv"]
#     }
# ])


# -- The following functions are used for getting the number of rows in different deployments --#

def get_num_rows_table(server, catalog_num, table_name, cookieval):
    """
    Given the correct inputs, sends a request to ERMrest and returns the cnt(*) of table
    """
    headers = {'Content-type': 'application/json'}
    cookies = {'webauthn': cookieval}
    try:
        r = requests.get('https://%s/ermrest/catalog/%s/aggregate/%s/cnt:=cnt(*)' % (server, catalog_num, table_name), headers=headers, cookies=cookies)
        return r.json()[0]["cnt"]
    except:
        print("error while loading", table_name, "error:", r.content)
        return -1

def get_num_rows_catalog(cases):
    """
    Prints an array of tables, and their corresponding number of rows.
    Sample input format:
    cases = [
      {
        'schema_location': 'the location of schema json file in schema folder',
        'catalog_num': 'the catalog id',
        'server': 'server location',
        'cookieval': 'the value of webauthn cookie (since we're sending request to server, this ensures all rows are available)'
      }
    ]
    """
    for case in cases:
        print("\n--------------------for", case['schema_location'], "--------------------")

        table_names = get_schema_info(case['schema_location'])[0]
        total_count = 0
        row_counts = defaultdict()
        for t_name in table_names:
            curr_count = get_num_rows_table(case['server'], case['catalog_num'], t_name, case['cookieval'])
            if curr_count == -1:
                return
            row_counts[t_name] = curr_count
            total_count += curr_count

        print("total count:", total_count)
        v = sorted_default_dict(row_counts)
        # print(v)
        tables = []
        counts = []
        for val in v:
            tables.append(val[0])
            counts.append(val[1])
        print("tables: ", tables)
        print("counts:", counts)

# uncomment the following to get the number of rows (you need to update cookie value)
# get_num_rows_catalog([
#     {
#         'schema_location': 'facebase.json',
#         'catalog_num': '1',
#         'server': 'www.facebase.org',
#         'cookieval': '' # Add your synapse webauthn cookie here
#     },
#     {
#         'schema_location': 'synapse.json',
#         'catalog_num': '1',
#         'server': 'synapse.isrd.isi.edu',
#         'cookieval': '' # Add your synapse webauthn cookie here
#     },
#     {
#         'schema_location': 'cirm.json',
#         'catalog_num': '1',
#         'server': 'cirm.isrd.isi.edu',
#         'cookieval': '' # Add your cirm webauthn cookie here
#     },
#     {
#         'schema_location': 'rbk.json',
#         'catalog_num': '2',
#         'server': 'dev.rebuildingakidney.org',
#         'cookieval': '' # Add your rbk webauthn cookie here
#     }
# ])
