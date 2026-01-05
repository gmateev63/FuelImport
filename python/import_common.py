import os
import xlrd
import datetime
import mysql.connector
import glob
import app_params as p
import app_logger as l

_WB_DATAMODE = None  # book.datemode

def create_equal_str_from_dict(dict,div):
    i = 0
    result = ""
    for k,val in dict.items():
        dv = ""
        if i>0: dv = div
        if isinstance(val,str): val = "'" + val + "'"
        else: val = str(val)
        result += dv + k + "=" + val 
        i = i + 1
    return result

def create_two_simple_str_from_dict(dict):
    result = ["",""]
    i = 0
    for k,val in dict.items():
        dv = ""
        if i>0: dv = ","
        result[0] += dv + k
        if val=='': val='NULL'
        elif isinstance(val,str): val = "'" + val + "'"
        else: val = str(val)
        if val=='None': val='NULL'
        result[1] += dv + val
        i = i + 1

    return result

def insert_or_update(proc_params,data_keys,data_values,callback_f,ws_row,my_c,base_file_name,row_id,seq):
    valid_keys = True
    lst_keys = list(data_keys.items())

    for itm in lst_keys:
        if itm[1]==None: valid_keys = False

    if valid_keys:
        sql = f"select pk from {proc_params['table_name']} where "
        sql += create_equal_str_from_dict(data_keys," and ")
        my_c.execute(sql)
        res = my_c.fetchone()

        if res == None:
            sql = f"select get_sequence('{proc_params['table_name']}')"
            my_c.execute(sql)
            pk = my_c.fetchone()[0]
            sql_from_keys = create_two_simple_str_from_dict(data_keys)
            sql_from_values = create_two_simple_str_from_dict(data_values)
            sql = "insert into " + proc_params['table_name'] + "(pk,"
            sql += sql_from_keys[0] + "," + sql_from_values[0] + ",userid) select " + str(pk) + "," 
            sql += sql_from_keys[1] + "," + sql_from_values[1] + ",0"
            my_c.execute(sql)
        else:
            pk = res[0]
            if proc_params['with_update']:
                sql = "update " + proc_params['table_name'] + " set "
                sql += create_equal_str_from_dict(data_values,",")
                sql += ",userid=0 where pk=" + str(pk)
                my_c.execute(sql)

        if callback_f: callback_f(ws_row,my_c,pk,seq)
        my_c.execute('commit')
    else:
        row_str = ' | '.join(str(e.value) for e in ws_row)
        l.logger.error(f"Error in line: {row_str}")
        sql = "insert into log_import_py(import_py_type_id,file_name,row_id,info,userid) " \
            f"select {proc_params['import_type']},'{base_file_name}',{row_id+1},left('{row_str}',250),0"
        my_c.execute(sql)
        my_c.execute('commit')

def get_file_sequence(my_c,sequence_id):
    sql = f"select get_sequence('{sequence_id}')"
    my_c.execute(sql)
    res = my_c.fetchone()
    seq = str(res[0])
    my_c.execute('commit')
    return(seq)

def move_file(pth,file_path,move_dir,seq):
    base_name = os.path.basename(file_path)
    dest_path = pth + move_dir + '/' + seq + '_' + base_name
    os.rename(file_path,dest_path)

def get_date(inp,table_name=None,my_c=None,ws_row=None):
    result = None
    is_err = False
    try:
        py_date = datetime.datetime(*xlrd.xldate_as_tuple(inp,_WB_DATAMODE))
        result = py_date.strftime("%Y-%m-%d")
    except:
        is_err = True
        #l.logger.error(f"Error in converting date {inp}")
    
    if is_err:
        d_day = inp[0:2]
        d_month = inp[3:5]
        d_year = inp[6:10]
        result = f"{d_year}-{d_month}-{d_day}"
    return result

def lookup_by_name(inp,table_name,my_c,ws_row=None):
    inp = str(inp).strip()
    sql = f"select pk from {table_name} where name like '{inp}'"
    my_c.execute(sql)
    result = None
    for row in my_c: result = row[0]
    if not result:
        sql = f"select get_sequence('{table_name}')"
        my_c.execute(sql)
        for row in my_c: result = row[0]
        sql = f"insert into {table_name} (pk,name,userid) select {result},'{inp}',0"
        my_c.execute(sql)
        if table_name=='tyre_types_list':
            sql = f"update tyre_types_list set short_name=name where pk={result}"
            my_c.execute(sql)
        my_c.execute('commit')
    return result

def lookup_car(inp,table_name,my_c,ws_row=None):
    inp = str(inp).strip()
    sql = f"select pk from {table_name} where reg_no like '{inp}'"
    my_c.execute(sql)
    result = None
    for row in my_c: result = row[0]
    return result

def lookup_kind_car(inp,table_name,my_c,ws_row):
    inp = str(inp).strip()
    sql = f"select kind_id from {table_name} where reg_no like '{inp}'"
    my_c.execute(sql)
    result = None
    for row in my_c: result = row[0]
    return result    

def valid_file(file_path,label_sheet_no,label_row_no,pars):    
    wb = xlrd.open_workbook(file_path)
    ws = wb.sheet_by_index(label_sheet_no)
    label_row = ws.row(label_row_no)

    for point in pars:
        par_lbl = point["lbl"]
        par_ind = point["ind"]

        result = True
        if par_lbl != label_row[par_ind].value:
            result = False
            break

    return result

def import_file(proc_params,file_path,seq,my_c,fields,callback_f):
    global _WB_DATAMODE

    result = False    
    wb = xlrd.open_workbook(file_path)
    _WB_DATAMODE = wb.datemode
    ws = wb.sheet_by_index(proc_params['label_sheet_no'])

    row_id = 0
    is_break = False
    for ws_row in ws.get_rows():
        if row_id > proc_params['label_row_no']:      
            val = ws_row[proc_params['break_column_index']].value.strip()
            if val!=None and val!="":
                data_values = {}
                data_keys = {}
                for point in fields:
                    ind = point["ind"]
                    
                    try:
                        func = point["func"]
                    except KeyError:
                        func = None

                    try:
                        db_lookup_table = point["db_lookup_table"]
                    except KeyError:
                        db_lookup_table = None

                    val = ws_row[ind].value
                    if func != None: val = func(val,db_lookup_table,my_c,ws_row)
                    l.logger.debug(f"{row_id} - ind={ind} - {val}")

                    if point["is_key"]: data_keys[point["db_field"]] = val
                    else: data_values[point["db_field"]] = val

                insert_or_update(proc_params,data_keys,data_values,callback_f,ws_row,my_c,os.path.basename(file_path),row_id,seq)

        row_id += 1

    result = True # TODO ?

    return result

def main(proc_params,fields,callback_func):
    my_conn = mysql.connector.connect(user=p.mysql_usr, password=p.mysql_pwd,host=p.mysql_host,database=p.mysql_db)
    my_c = my_conn.cursor(buffered=True)
    files_path = f"{p.import_path}/{proc_params['subfolder_name']}/import/*.xls*"
    files = glob.glob(files_path)
    for file_path in files:
        seq = get_file_sequence(my_c,proc_params['sequence_name'])
        move_dir = f"/{proc_params['subfolder_name']}/not_imported"
        if valid_file(file_path,proc_params['label_sheet_no'],proc_params['label_row_no'],fields):
            if import_file(proc_params,file_path,seq,my_c,fields,callback_func): 
                move_dir = f"/{proc_params['subfolder_name']}/imported_ok"
        move_file(p.import_path,file_path,move_dir,seq)

    if my_conn: my_conn.close()