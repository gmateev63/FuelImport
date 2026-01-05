import app_params as p
import app_logger as l
import import_common as c

proc_params = ({
    "import_type":3, # fuel_eko
    "table_name":"fuel_charges_import_v2",
    "subfolder_name":"fuel",
    "label_sheet_no":0,
    "label_row_no":0,
    "sequence_name":"fuel_eko_import_seq",
    "break_column_index":2,
    "with_update":False
})

def asseble_date(inp,table_name=None,my_c=None,ws_row=None):
    data_0 = inp.strip()
    data = data_0[6:10] + "-" + data_0[3:5] + "-" + data_0[0:2] + " " + ws_row[12].value.strip()
    return(data)

def set_supplier(inp,table_name=None,my_c=None,ws_row=None):
    return(5)

fields = ({
        "db_field":"station",
        "is_key":False,
        "lbl":"Plant",
        "ind":0
        },{
        "db_field":"order_no",
        "is_key":False,
        "lbl":"Billing Document",
        "ind":2
        },{
        "db_field":"charge_type",
        "is_key":False,
        "lbl":"Material",
        "ind":3
        },{
        "db_field":"ddate",
        "is_key":True,
        "lbl":"Date",
        "ind":4,
        "func":asseble_date
        },{
        "db_field":"litres",
        "is_key":False,
        "lbl":"Bill.qty",
        "ind":5
        },{
        "db_field":"price_per_liter",
        "is_key":False,
        "lbl":"FinPr",
        "ind":6
        },{
        "db_field":"total_price",
        "is_key":False,
        "lbl":"Tot Amount",
        "ind":9
        },{
        "db_field":"total_vat",
        "is_key":False,
        "lbl":"Vat Value",
        "ind":11
        },{
        "db_field":"card_no",
        "is_key":True,
        "lbl":"Number",
        "ind":13
        },{
        "db_field":"reg_no",
        "is_key":False,
        "lbl":"Name",
        "ind":14
        },{
        "db_field":"mileage",
        "is_key":False,
        "lbl":"Km stand",
        "ind":15
        },{
        "db_field":"supplier_id",
        "is_key":False,
        "lbl":"Plant",
        "ind":0,
        "func":set_supplier     
})

def get_charge_type_id(fuel_type,my_c):
    sql = f"select pk from fuel_types_list where erp_code like {fuel_type}"
    my_c.execute(sql)
    rec = my_c.fetchone()
    if rec==None: fuel_type_id = 'NULL'
    else: fuel_type_id = rec[0]
    return fuel_type_id

def callback_func(ws_row,my_c,pk,seq):
    charge_type = ws_row[3].value[0:9]
    charge_type_id = get_charge_type_id(charge_type,my_c)

    product_type_id = 0 # fuel
    if charge_type_id=='NULL': product_type_id = 1 # no fuel

    sql = f"update fuel_charges_import_v2 set seq_no={seq}, \
        product_type_id={product_type_id},charge_type_id={charge_type_id} where pk={pk}"
    my_c.execute(sql)

if __name__== "__main__": c.main(proc_params,fields,callback_func)