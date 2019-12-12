from .views import *
import pymysql, re, json
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import numpy as np
from django.shortcuts import HttpResponse
from rest_framework.views import APIView


def DBConnection():
    user = 'taxgenie'
    passw = 'taxgenie*#8102*$'
    host = '15.206.93.178'
    port = 3306
    database ='taxgenie_efilling'
    mydb =create_engine('mysql+pymysql://' + user + ':' +passw + '@' + host + ':' +str(port) + '/' + database , echo = False)
    return  mydb
mydb = DBConnection()

def func(x):
    try:
        if (re.match(r'[\s\w\d]+[/.-]+[\w\d]+[./-]+[\w\d\W]+',str(x))):
            return pd.to_datetime(x, errors='coerce', format='%d-%m-%Y')

        elif (re.match(r'(?:\d{5})[.](?:\d{1,})|(?:\d{5})', str(x))):
            x = datetime.fromtimestamp(timestamp=(x - 25569) * 86400).strftime('%d-%m-%Y')
            return pd.to_datetime(x, errors='coerce', format='%d-%m-%Y')

        elif (re.match(r'[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)', str(x))):
            x = time.strftime('%d-%m-%Y', time.localtime(x))
            return pd.to_datetime(x, errors='coerce', format='%d-%m-%Y')
    except:
        return pd.NaT

class upload_engine(APIView):
    def post(self,request):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        dataUploadEngine =request.data['data']
        typeData = dataUploadEngine['type']
        customerGSTIN = dataUploadEngine['GSTIN']
        pan_num = customerGSTIN[2:-3]
        try:
            con = pymysql.connect(host="15.206.93.178", user="taxgenie", password="taxgenie*#8102*$",db='taxgenie_efilling')
            map1 = pd.read_sql("select column_header from s3_file_details_mapping where id ='" + dataUploadEngine['template_id'] + "'",con)
        except Exception as e:
            print(str(e),'Database exception')
            return HttpResponse(json.dumps({"reason": str(e), "status":"Connection Error"}))

        if (len(map1) == 0):
            return HttpResponse(json.dumps({"reason": "No template available. Please create template to proceed","status": "Connection Error"}))
        else:
            bb = map1.to_json(orient='records').replace("\\", '')[1:-1]
            print(bb,"This is bb")
            data = re.search(r'(?=\[)(.*\])', str(bb)).group(1)
            dt = {}
            dt.update({'Reference_id':'Reference_id'})
            dt.update({'reason':'reason'})
            dt.update({'Status':'Status'})
            dt.update({'invoiceFinancialPeriod':'invoiceFinancialPeriod'})
            if typeData == 'GSTR1-Sales':
                dt.update({'sellerID':'sellerID'})
            if typeData == 'GSTR2-Purchase':
                dt.update({'buyerID':'buyerID'})
            dt.update({'financialPeriod':'financialPeriod'})
            dt.update({'gstnStatus':'gstnStatus'})
            dt.update({'RawFileRef_Id':'RawFileRef_Id'})
            dt.update({'invoiceStatus':'invoiceStatus'})
            for d in eval(data):
                dt.update(d)

            if dataUploadEngine['Action_Status']=='TG-FILE' and typeData=='GSTR1-Sales':
                try:
                    raw=pd.read_excel(BASE_DIR+"/TG-FILE/"+dataUploadEngine['filename'])
                except Exception as e:
                    print(str(e))
                    return HttpResponse(json.dumps({'reason' : str(e),'status' : 'FAIL'}))
                if (len(raw) == 0):
                    return HttpResponse(json.dumps({"reason": "No Records passed TG validation. Check the error file.", "status": "FAIL"}))
                else:
                    #####################   Column Mapping  #####################################################
                    stage_tb = pd.DataFrame()
                    for user, sys in dt.items():
                        stage_tb[sys] = raw[user]

                    ########################### Validation Function Call #########################################
                    df_final_s = stage_tb
                    print(df_final_s['invoiceFinancialPeriod'],'df_final_s')

########################################################   DATABASE DUMPING START  ###################################################################################################
                    print("##################################################### DB DUMPING START #################################################################################")
                    pan_check = df_final_s.loc[df_final_s['Status'] == 'Success']
                    print(len(pan_check), 'pan check success data')
                    if (len(pan_check)==0):
                        df_final_s.astype(str).to_sql(name='upload_sales_result_table', con=mydb, if_exists='append',index=False, chunksize=500)
                        return HttpResponse(json.dumps({"reason": "PAN VALIDATION FAILED. PLEASE CHECK FILE CONTAINS VALID GSTIN","status": "Fail"}))
                    else:
                        try:
                            df_final_s.astype(str).to_sql(name='upload_sales_result_table', con=mydb, if_exists='append', index=False,chunksize=500)
                        except Exception as e:
                            print(str(e))
                            return HttpResponse(json.dumps({"reason": str(e),"status": "Connection Error"}))

                        dataSuccess = pd.read_sql("SELECT * FROM upload_sales_result_table where Reference_id in "+str(tuple(df_final_s['Reference_id'].unique()))+" and Status='Success' ",mydb)
                        if(len(dataSuccess) == 0):
                            return HttpResponse(json.dumps({"reason": "All Invoice Records with status Fail", "status": "FAIL"}))
                        else:
                            dataSuccess.rename(columns={
                                'Buyer Gstin': 'buyerGSTIN',
                                'Invoice Type': 'typeofinvoice',
                                'Invoice SubType': 'invoiceSubType',
                                'Invoice Date': 'invoiceDate',
                                'Invoice No': 'invoiceNo',
                                'Invoice Value': 'invoiceValue',
                                'quantity': 'quantity',
                                'Reverse Charge': 'reverseCharge',
                                'Seller Gstin': 'sellerGSTIN',
                                'unit': 'unit',
                                'Place Of Supply': 'pos',
                                'Taxable Value': 'taxableValue',
                                'Igst Rate': 'igstRate',
                                'Cgst Rate': 'cgstRate',
                                'Sgst Rate': 'sgstOrUgstRate',
                                'Igst Amount': 'igstAmt',
                                'Cgst Amount': 'cgstAmt',
                                'Sgst Amount': 'sgstOrUgstAmt',
                                'Cess Amount': 'cessAmt',
                                'Hsn Code': 'HSNorSAC',
                                'Rate': 'gstRate',
                                'invoiceFinancialPeriod': 'invoiceFinancialPeriod',
                                'Reference_id': 'Reference_id'}, inplace=True)

                            print(set(dataSuccess['invoiceFinancialPeriod'].unique()))

                            for process in set(dataSuccess['invoiceFinancialPeriod'].unique()):
                                print(process, 'Financial Year')

                                dataSalesHeader = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
                                dataSalesHeader = dataSalesHeader.applymap(lambda x: x.strip() if type(x) == str else x)
                                dataSalesHeader = dataSalesHeader.fillna('')
                                dataSuccess = dataSuccess.fillna('')
                                dataSuccess['buyerGSTIN'] = dataSuccess['buyerGSTIN'].astype(str).str.replace('nan', '')

                                header_ls = ['buyerGSTIN', 'typeofinvoice', 'invoiceSubType', 'invoiceDate', 'invoiceNo',
                                             'invoiceValue', 'sellerGSTIN', 'pos', 'reverseCharge', 'Reference_id',
                                             'invoiceFinancialPeriod', 'sellerID', 'financialPeriod', 'gstnStatus', 'invoiceStatus']
                                for i in header_ls:
                                    if i not in set(dataSuccess.columns.tolist()):
                                        dataSuccess[i] = np.nan

                                dataUnique = dataSuccess[header_ls].drop_duplicates(subset=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'],keep='first')
                                dataReplace = dataUnique.loc[((dataUnique['sellerGSTIN'].isin(dataSalesHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataSalesHeader['invoiceNo']))& (dataUnique['typeofinvoice'].isin(dataSalesHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataSalesHeader['buyerGSTIN']))& (dataUnique['invoiceFinancialPeriod'].isin(dataSalesHeader['invoiceFinancialPeriod'])))]
                                print(len(dataReplace),'data to replace')

                                dataInsert = dataUnique[['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'invoiceSubType', 'gstnStatus', 'sellerID','invoiceStatus', 'financialPeriod', 'buyerGSTIN', 'invoiceDate', 'reverseCharge', 'pos','invoiceFinancialPeriod']].loc[~((dataUnique['sellerGSTIN'].isin(dataSalesHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataSalesHeader['invoiceNo'])) & (dataUnique['typeofinvoice'].isin(dataSalesHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataSalesHeader['buyerGSTIN'])))]
                                print(len(dataInsert),'data to insert')

                                if (len(dataInsert) != 0):
                                    dataInsert.to_sql('sales_invoice_header', mydb, if_exists='append', index=False, chunksize=100)

                                if (len(dataReplace) != 0):
                                    print("INSIDE UPDATE")
                                    count = 0

                                    for index, replace in dataReplace.iterrows():
                                        count = count + 1
                                        updatedAt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        query = """update sales_invoice_header a
                                        set
                                        a.invoiceSubType='""" + str(replace['invoiceSubType']) + """',
                                        a.invoiceDate = '""" + str(replace['invoiceDate']) + """',
                                        a.reverseCharge = '""" + str(replace['reverseCharge']) + """',
                                        a.invoiceFinancialPeriod ='""" + str(replace['invoiceFinancialPeriod']) + """',
                                        a.pos= '""" + str(replace['pos']) + """',
                                        a.updatedAt= '""" + updatedAt + """',
                                        a.financialPeriod= '""" + str(replace['financialPeriod']) + """',
                                        a.invoiceStatus= '""" + str(replace['invoiceStatus']) + """'
                                        where
                                        a.sellerGSTIN= '""" + str(replace['sellerGSTIN']) + """'
                                        and a.buyerGSTIN='""" + str(replace['buyerGSTIN']) + """'
                                        and a.invoiceNo='""" + str(replace['invoiceNo']) + """'
                                        and a.typeofinvoice='""" + str(replace['typeofinvoice']) + """'
                                        and a.invoiceFinancialPeriod='""" + str(replace['invoiceFinancialPeriod']) + """'
                                        and a.sellerGSTIN like '%%""" + pan_num + """%%' """
                                        with mydb.begin() as conn:
                                            conn.execute(query)
                                    print("number of replace count ", count)

                                dataSalesItem = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
                                dataSalesItem = dataSalesItem.applymap(lambda x: x.strip() if type(x) == str else x)
                                dataSalesItem = dataSalesItem.fillna('')

                                finalDataSalesItem = pd.merge(left=dataSuccess, right=dataSalesItem,on=['sellerGSTIN','invoiceNo','typeofinvoice','buyerGSTIN'],how='left')

                                ls1 = tuple(finalDataSalesItem['invoiceHeaderID'].dropna().unique())
                                print(len(finalDataSalesItem), 'data after merging for item table')
                                print(len(ls1), 'number of invoice header ID to be deleted from sales invoice items table')

                                if (len(ls1) > 1):
                                    mydb.execute("Delete from sales_invoice_items where invoiceHeaderID in "+str(ls1)+"")
                                elif (len(ls1) == 1):
                                    mydb.execute("Delete from sales_invoice_items where invoiceHeaderID = "+ str(ls1[0])+"")

                                item_ls = ['quantity', 'unit', 'taxableValue', 'igstRate', 'cgstRate', 'sgstOrUgstRate','igstAmt',
                                           'cgstAmt', 'sgstOrUgstAmt', 'cessAmt', 'HSNorSAC', 'invoiceHeaderID']

                                for i in item_ls:
                                    if i not in set(finalDataSalesItem.columns.tolist()):
                                        finalDataSalesItem[i] = np.nan

                                finalDataSalesItem = finalDataSalesItem[item_ls]
                                finalDataSalesItem['srNo'] = ''
                                finalDataSalesItem['srNo'] = finalDataSalesItem.groupby('invoiceHeaderID').cumcount() + 1
                                print(len(finalDataSalesItem), 'records to insert into sales invoice item table')

                                finalDataSalesItem.to_sql(name='sales_invoice_items', con=mydb, if_exists='append', index=False, chunksize=50)

                                print("DUMPING DONE")
                            return HttpResponse(json.dumps({"reason":"Sales upload successful","status":"success"}), status=status.HTTP_201_CREATED)

    #############################################   PURCHASE PART  ##############################################################################

            elif dataUploadEngine['Action_Status'] == 'TG-FILE' and typeData == 'GSTR2-Purchase':
                print('INSIDE PURCHASE UPLOAD ENGINE')
                try:
                    raw = pd.read_excel(BASE_DIR+"/TG-FILE/"+ dataUploadEngine['filename'])
                except Exception as e:
                    print(str(e))
                    return HttpResponse(json.dumps({'reason': str(e), 'status': 'FAIL'}))
                if (len(raw) == 0):
                    return HttpResponse(json.dumps({"reason": "No Records passed TG validation", "status": "FAIL"}))
                else:
                    #####################   Column Mapping  #####################################################
                    stage_tb = pd.DataFrame()
                    for user, sys in dt.items():
                        stage_tb[sys] = raw[user]
#################################################################################################################
                    df_final_s = stage_tb

######################################################################   DATABASE DUMPING START  ###################################################################################################
                    print("##################################################### DB DUMPING START #################################################################################")

                    pan_check = df_final_s.loc[df_final_s['Status'] == 'Success']
                    print(len(pan_check),'pan check success data')
                    if (len(pan_check) == 0):
                        df_final_s.astype(str).to_sql(name='upload_purchase_result_table', con=mydb, if_exists='append',index=False, chunksize=500)
                        return HttpResponse(json.dumps({"reason": "Fail data from result purchase table", "status": "success"}))
                    else:
                        try:
                            df_final_s.astype(str).to_sql(name='upload_purchase_result_table', con=mydb, if_exists='append',index=False, chunksize=500)
                        except Exception as e:
                            print(str(e))
                            return HttpResponse(json.dumps({"reason": str(e), "status": "Connection Error"}))

                        dataSuccess1 = pd.read_sql("SELECT * FROM upload_purchase_result_table where Reference_id in " +str(tuple(df_final_s['Reference_id'].unique()))+ " and Status='Success' ",mydb)
                        print(len(dataSuccess1), "Success data from result purchase table")

                        if (len(dataSuccess1) == 0):
                            return HttpResponse(json.dumps({"reason": "All Invoice Records with status Fail", "status": "success"}))
                        else:
                            dataSuccess1.rename(columns={
                                'Buyer Gstin': 'buyerGSTIN',
                                'Invoice Type': 'typeofinvoice',
                                'Invoice SubType': 'invoiceSubType',
                                'Invoice Date': 'invoiceDate',
                                'Invoice No': 'invoiceNo',
                                'Invoice Value': 'invoiceValue',
                                'quantity': 'quantity',
                                'Reverse Charge': 'reverseCharge',
                                'Seller Gstin': 'sellerGSTIN',
                                'unit': 'unit',
                                'Place Of Supply': 'pos',
                                'Taxable Value': 'taxableValue',
                                'Igst Rate': 'igstRate',
                                'Cgst Rate': 'cgstRate',
                                'Sgst Rate': 'sgstOrUgstRate',
                                'Igst Amount': 'igstAmt',
                                'Cgst Amount': 'cgstAmt',
                                'Sgst Amount': 'sgstOrUgstAmt',
                                'Cess Amount': 'cessAmt',
                                'Hsn Code': 'HSNorSAC',
                                'Rate': 'gstRate',
                                'invoiceFinancialPeriod': 'invoiceFinancialPeriod',
                                'Reference_id': 'Reference_id'}, inplace=True)

                            for process in set(dataSuccess1['invoiceFinancialPeriod'].unique()):
                                print(process,'Financial Year')
                                dataPurchaseHeader =pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
                                print(len(dataPurchaseHeader),'data for process')
                                dataPurchaseHeader = dataPurchaseHeader.applymap(lambda x: x.strip() if type(x) == str else x)
                                dataPurchaseHeader = dataPurchaseHeader.fillna('')
                                dataSuccess = dataSuccess1.loc[dataSuccess1['invoiceFinancialPeriod'] == process]
                                dataSuccess = dataSuccess.fillna('')
                                dataSuccess.to_csv(r"C:\Users\Admin\Desktop\HeaderId issue\Upload ABFL Testing\DataSuccess.csv")

                                header_ls = ['buyerGSTIN', 'typeofinvoice', 'invoiceSubType', 'invoiceDate', 'invoiceNo',
                                             'invoiceValue', 'sellerGSTIN', 'pos', 'reverseCharge', 'Reference_id',
                                             'invoiceFinancialPeriod', 'buyerID', 'financialPeriod', 'gstnStatus',
                                             'invoiceStatus']

                                for i in header_ls:
                                    if i not in set(dataSuccess.columns.tolist()):
                                        dataSuccess[i] = ''

                                dataSuccess['sellerGSTIN'] = dataSuccess['sellerGSTIN'].astype(str).str.replace('nan','')

                                dataUnique = dataSuccess[header_ls].drop_duplicates(subset=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'], keep='first')
                                dataReplace = dataUnique.loc[((dataUnique['sellerGSTIN'].isin(dataPurchaseHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataPurchaseHeader['invoiceNo'])) & (dataUnique['typeofinvoice'].isin(dataPurchaseHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataPurchaseHeader['buyerGSTIN'])) & (dataUnique['invoiceFinancialPeriod'].isin(dataPurchaseHeader['invoiceFinancialPeriod'])))]
                                print(dataReplace, 'data to replace')


#++++++++++++++++++++++++++++++++++++++++++++Insert change 9th Decemeber++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                                index1 = pd.MultiIndex.from_arrays([dataPurchaseHeader[col] for col in ['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN']])
                                index2 = pd.MultiIndex.from_arrays([dataUnique[col] for col in['sellerGSTIN', 'invoiceNo', 'typeofinvoice','buyerGSTIN']])
                                dataInsert= dataUnique[['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'invoiceSubType', 'gstnStatus', 'buyerID','invoiceStatus', 'financialPeriod', 'buyerGSTIN', 'invoiceDate', 'reverseCharge', 'pos','invoiceFinancialPeriod']].loc[~index2.isin(index1)]
                                # dataInsert = dataUnique[['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'invoiceSubType', 'gstnStatus', 'buyerID','invoiceStatus', 'financialPeriod', 'buyerGSTIN', 'invoiceDate', 'reverseCharge', 'pos','invoiceFinancialPeriod']].loc[~((dataUnique['sellerGSTIN'].isin(dataPurchaseHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataPurchaseHeader['invoiceNo'])) & (dataUnique['typeofinvoice'].isin(dataPurchaseHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataPurchaseHeader['buyerGSTIN'])))]
                                print(dataInsert, 'data to insert')
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                                if (len(dataInsert) != 0):
                                    dataInsert.to_sql('purchase_invoice_header', mydb, if_exists='append', index=False,chunksize=100)

                                if (len(dataReplace) != 0):
                                    print("INSIDE UPDATE")
                                    count = 0
                                    for index, replace in dataReplace.iterrows():
                                        count = count + 1
                                        updatedAt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        query = """update purchase_invoice_header a
                                        set
                                        a.invoiceSubType='""" + str(replace['invoiceSubType']) + """',
                                        a.invoiceDate = '""" + str(replace['invoiceDate']) + """',
                                        a.reverseCharge = '""" + str(replace['reverseCharge']) + """',
                                        a.invoiceFinancialPeriod ='""" + str(replace['invoiceFinancialPeriod']) + """',
                                        a.pos= '""" + str(replace['pos']) + """',
                                        a.updatedAt= '""" + updatedAt + """',
                                        a.financialPeriod= '""" + str(replace['financialPeriod']) + """',
                                        a.invoiceStatus= '""" + str(replace['invoiceStatus']) + """'
                                        where
                                        a.sellerGSTIN= '""" + str(replace['sellerGSTIN']) + """'
                                        and a.buyerGSTIN='""" + str(replace['buyerGSTIN']) + """'
                                        and a.invoiceNo='""" + str(replace['invoiceNo']) + """'
                                        and a.typeofinvoice='""" + str(replace['typeofinvoice']) + """'
                                        and a.invoiceFinancialPeriod='""" + str(replace['invoiceFinancialPeriod']) + """'
                                        and a.buyerGSTIN like '%%""" + pan_num + """%%' """
                                        with mydb.begin() as conn:
                                            conn.execute(query)
                                    print("number of replace count ", count)

                                dataPurchaseItem = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
                                dataPurchaseItem.to_csv(r"C:\Users\Admin\Desktop\HeaderId issue\Upload ABFL Testing\DataBase_Fetch.csv")
                                dataPurchaseItem = dataPurchaseItem.applymap(lambda x: x.strip() if type(x) == str else x)
                                dataPurchaseItem = dataPurchaseItem.fillna('')
                                print(len(dataPurchaseItem), 'data from invoice header for item table df creation')

                                finalDataPurchaseItem = pd.merge(left=dataSuccess, right=dataPurchaseItem,on=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'],how='left')
                                finalDataPurchaseItem.to_csv(r"C:\Users\Admin\Desktop\HeaderId issue\Upload ABFL Testing\Raw_match_id.csv")

                                ls1 = tuple(finalDataPurchaseItem['invoiceHeaderID'].dropna().unique())
                                print(len(finalDataPurchaseItem), 'data after merging for item table')
                                print(len(ls1), 'number of invoice header ID to be deleted from purchase invoice items table')

                                if (len(ls1) > 1):
                                    mydb.execute("Delete from purchase_invoice_items where invoiceHeaderID in " + str(ls1) + "")
                                elif (len(ls1) == 1):
                                    mydb.execute("Delete from purchase_invoice_items where invoiceHeaderID = " + str(ls1[0]) + "")
                                item_ls = ['quantity', 'unit', 'taxableValue', 'igstRate', 'cgstRate', 'sgstOrUgstRate', 'igstAmt',
                                           'cgstAmt', 'sgstOrUgstAmt', 'cessAmt', 'HSNorSAC', 'invoiceHeaderID']

                                for i in item_ls:
                                    if i not in set(finalDataPurchaseItem.columns.tolist()):
                                        finalDataPurchaseItem[i] = np.nan

                                finalDataPurchaseItem = finalDataPurchaseItem[item_ls]
                                finalDataPurchaseItem['srNo'] = ''
                                finalDataPurchaseItem['srNo'] = finalDataPurchaseItem.groupby('invoiceHeaderID').cumcount() + 1
                                print(len(finalDataPurchaseItem), 'records to insert into purchase invoice item table')

                                finalDataPurchaseItem.to_sql(name='purchase_invoice_items', con=mydb, if_exists='append', index=False,chunksize=50)

                            return HttpResponse(json.dumps({"reason": "PURCHASE UPLOAD SUCCESSFUL", "status": "success"}),status=status.HTTP_201_CREATED)
            else:
                return HttpResponse(json.dumps({"reason": "Condition not satisfied for sales or purchase.", "status": "FAIL"}))

# from .views import *
# import pymysql, re, json
# from sqlalchemy import create_engine
# from datetime import datetime
# import pandas as pd
# import numpy as np
# from django.shortcuts import HttpResponse
# from rest_framework.views import APIView
# from .Updated_Sales_Validation import Sales
#
# def DBConnection():
#     user = 'taxgenie'
#     passw = 'taxgenie*#8102*$'
#     host = '15.206.93.178'
#     port = 3306
#     database = 'taxgenie_efilling'
#     mydb = create_engine('mysql+pymysql://' + user + ':' +passw + '@' + host + ':' +str(port) + '/' + database , echo = False)
#     return  mydb
# mydb = DBConnection()
#
# def func(x):
#     try:
#         if (re.match(r'[\s\w\d]+[/.-]+[\w\d]+[./-]+[\w\d\W]+', str(x))):
#             return pd.to_datetime(x, errors='coerce', format='%d-%m-%Y')
#
#         elif (re.match(r'(?:\d{5})[.](?:\d{1,})|(?:\d{5})', str(x))):
#             x = datetime.fromtimestamp(timestamp=(x - 25569) * 86400).strftime('%d-%m-%Y')
#             return pd.to_datetime(x, errors='coerce', format='%d-%m-%Y')
#
#         elif (re.match(r'[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)', str(x))):
#             x = time.strftime('%d-%m-%Y', time.localtime(x))
#             return pd.to_datetime(x, errors='coerce', format='%d-%m-%Y')
#     except:
#         return pd.NaT
#
# class upload_engine(APIView):
#     def post(self,request):
#         BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#         dataUploadEngine = request.data['data']
#         typeData = dataUploadEngine['type']
#         customerGSTIN = dataUploadEngine['GSTIN']
#         pan_num = customerGSTIN[2:-3]
#
#         try:
#             con = pymysql.connect(host="localhost", user="root", password="Taxgenie@2018",db='taxgenie_efilling2')
#             map1 = pd.read_sql("select column_header from s3_file_details_mapping where id ='" + dataUploadEngine['template_id'] + "'",con)
#             con.close()
#         except Exception as e:
#             print(str(e),'Database exception')
#             return HttpResponse(json.dumps({"reason": str(e), "status":"Connection Error"}))
#
#         if (len(map1) == 0):
#             return HttpResponse(json.dumps({"reason": "No template available. Please create template to proceed","status": "Connection Error"}))
#         else:
#             bb = map1.to_json(orient='records').replace("\\", '')[1:-1]
#             data = re.search(r'(?=\[)(.*\])', str(bb)).group(1)
#             dt = {}
#             dt.update({'Reference_id':'Reference_id'})
#             dt.update({'reason': 'reason'})
#             dt.update({'Status': 'Status'})
#             dt.update({'invoiceFinancialPeriod': 'invoiceFinancialPeriod'})
#             if typeData == 'GSTR1-Sales':
#                 dt.update({'sellerID': 'sellerID'})
#             if typeData == 'GSTR2-Purchase':
#                 dt.update({'buyerID': 'buyerID'})
#             dt.update({'financialPeriod': 'financialPeriod'})
#             dt.update({'gstnStatus': 'gstnStatus'})
#             dt.update({'RawFileRef_Id': 'RawFileRef_Id'})
#             dt.update({'invoiceStatus': 'invoiceStatus'})
#             for d in eval(data):
#                 dt.update(d)
#
#             if dataUploadEngine['Action_Status']=='TG-FILE' and typeData=='GSTR1-Sales':
#                 try:
#                     raw=pd.read_excel(BASE_DIR+"/TG-FILE/"+dataUploadEngine['filename'])
#                 except Exception as e:
#                     print(str(e))
#                     return HttpResponse(json.dumps({'reason' : str(e),'status' : 'FAIL'}))
#                 if (len(raw) == 0):
#                     return HttpResponse(json.dumps({"reason": "No Records passed TG validation. Check the error file.", "status": "FAIL"}))
#                 else:
#                     #####################   Column Mapping  #####################################################
#                     stage_tb = pd.DataFrame()
#                     for user, sys in dt.items():
#                         stage_tb[sys] = raw[user]
#
#                     ########################### Validation Function Call #########################################
#                     df_final_s = stage_tb
#                     print(df_final_s['invoiceFinancialPeriod'],'df_final_s')
#
# ########################################################   DATABASE DUMPING START  ###################################################################################################
#                     print("##################################################### DB DUMPING START #################################################################################")
#                     pan_check = df_final_s.loc[df_final_s['Status'] == 'Success']
#                     print(len(pan_check), 'pan check success data')
#                     if (len(pan_check)==0):
#                         return HttpResponse(json.dumps({"reason": "PAN VALIDATION FAILED. PLEASE CHECK FILE CONTAINS VALID GSTIN","status": "Fail"}))
#                     else:
#                         try:
#                             df_final_s.astype(str).to_sql(name='upload_sales_result_table', con=mydb, if_exists='append', index=False,chunksize=500)
#                         except Exception as e:
#                             print(str(e))
#                             return HttpResponse(json.dumps({"reason": str(e),"status": "Connection Error"}))
#
#                         dataSuccess = pd.read_sql("SELECT * FROM upload_sales_result_table where Reference_id in "+str(tuple(df_final_s['Reference_id'].unique()))+" and Status='Success' ",mydb)
#                         if(len(dataSuccess) == 0):
#                             return HttpResponse(json.dumps({"reason": "All Invoice Records with status Fail", "status": "FAIL"}))
#                         else:
#                             dataSuccess.rename(columns={
#                                 'Buyer Gstin': 'buyerGSTIN',
#                                 'Invoice Type': 'typeofinvoice',
#                                 'Invoice SubType': 'invoiceSubType',
#                                 'Invoice Date': 'invoiceDate',
#                                 'Invoice No': 'invoiceNo',
#                                 'Invoice Value': 'invoiceValue',
#                                 'quantity': 'quantity',
#                                 'Reverse Charge': 'reverseCharge',
#                                 'Seller Gstin': 'sellerGSTIN',
#                                 'unit': 'unit',
#                                 'Place Of Supply': 'pos',
#                                 'Taxable Value': 'taxableValue',
#                                 'Igst Rate': 'igstRate',
#                                 'Cgst Rate': 'cgstRate',
#                                 'Sgst Rate': 'sgstOrUgstRate',
#                                 'Igst Amount': 'igstAmt',
#                                 'Cgst Amount': 'cgstAmt',
#                                 'Sgst Amount': 'sgstOrUgstAmt',
#                                 'Cess Amount': 'cessAmt',
#                                 'Hsn Code': 'HSNorSAC',
#                                 'Rate': 'gstRate',
#                                 'invoiceFinancialPeriod': 'invoiceFinancialPeriod',
#                                 'Reference_id': 'Reference_id'}, inplace=True)
#
#                             print(set(dataSuccess['invoiceFinancialPeriod'].unique()))
#
#                             for process in set(dataSuccess['invoiceFinancialPeriod'].unique()):
#                                 print(process, 'Financial Year')
#
#                                 dataSalesHeader = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
#                                 dataSalesHeader = dataSalesHeader.applymap(lambda x: x.strip() if type(x) == str else x)
#                                 dataSalesHeader = dataSalesHeader.fillna('')
#                                 dataSuccess = dataSuccess.fillna('')
#                                 dataSuccess['buyerGSTIN'] = dataSuccess['buyerGSTIN'].astype(str).str.replace('nan', '')
#
#                                 header_ls = ['buyerGSTIN', 'typeofinvoice', 'invoiceSubType', 'invoiceDate', 'invoiceNo',
#                                              'invoiceValue', 'sellerGSTIN', 'pos', 'reverseCharge', 'Reference_id',
#                                              'invoiceFinancialPeriod', 'sellerID', 'financialPeriod', 'gstnStatus', 'invoiceStatus']
#                                 for i in header_ls:
#                                     if i not in set(dataSuccess.columns.tolist()):
#                                         dataSuccess[i] = np.nan
#
#                                 dataUnique = dataSuccess[header_ls].drop_duplicates(subset=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'],keep='first')
#                                 dataReplace = dataUnique.loc[((dataUnique['sellerGSTIN'].isin(dataSalesHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataSalesHeader['invoiceNo']))& (dataUnique['typeofinvoice'].isin(dataSalesHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataSalesHeader['buyerGSTIN']))& (dataUnique['invoiceFinancialPeriod'].isin(dataSalesHeader['invoiceFinancialPeriod'])))]
#                                 print(len(dataReplace),'data to replace')
#
#                                 dataInsert = dataUnique[['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'invoiceSubType', 'gstnStatus', 'sellerID','invoiceStatus', 'financialPeriod', 'buyerGSTIN', 'invoiceDate', 'reverseCharge', 'pos','invoiceFinancialPeriod']].loc[~((dataUnique['sellerGSTIN'].isin(dataSalesHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataSalesHeader['invoiceNo'])) & (dataUnique['typeofinvoice'].isin(dataSalesHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataSalesHeader['buyerGSTIN'])))]
#                                 print(len(dataInsert),'data to insert')
#
#                                 if (len(dataInsert) != 0):
#                                     dataInsert.to_sql('sales_invoice_header', mydb, if_exists='append', index=False, chunksize=100)
#
#                                 if (len(dataReplace) != 0):
#                                     print("INSIDE UPDATE")
#                                     count = 0
#                                     for index, replace in dataReplace.iterrows():
#                                         count = count + 1
#                                         updatedAt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                                         query = """update sales_invoice_header a
#                                         set
#                                         a.invoiceSubType='""" + str(replace['invoiceSubType']) + """',
#                                         a.invoiceDate = '""" + str(replace['invoiceDate']) + """',
#                                         a.reverseCharge = '""" + str(replace['reverseCharge']) + """',
#                                         a.invoiceFinancialPeriod ='""" + str(replace['invoiceFinancialPeriod']) + """',
#                                         a.pos= '""" + str(replace['pos']) + """',
#                                         a.updatedAt= '""" + updatedAt + """',
#                                         a.financialPeriod= '""" + str(replace['financialPeriod']) + """',
#                                         a.invoiceStatus= '""" + str(replace['invoiceStatus']) + """'
#                                         where
#                                         a.sellerGSTIN= '""" + str(replace['sellerGSTIN']) + """'
#                                         and a.buyerGSTIN='""" + str(replace['buyerGSTIN']) + """'
#                                         and a.invoiceNo='""" + str(replace['invoiceNo']) + """'
#                                         and a.typeofinvoice='""" + str(replace['typeofinvoice']) + """'
#                                         and a.invoiceFinancialPeriod='""" + str(replace['invoiceFinancialPeriod']) + """'
#                                         and a.sellerGSTIN like '%%""" + pan_num + """%%' """
#                                         with mydb.begin() as conn:
#                                             conn.execute(query)
#                                     print("number of replace count ", count)
#
#                                 dataSalesItem = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
#
#                                 dataSalesItem = dataSalesItem.applymap(lambda x: x.strip() if type(x) == str else x)
#                                 dataSalesItem = dataSalesItem.fillna('')
#
#                                 finalDataSalesItem = pd.merge(left=dataSuccess, right=dataSalesItem, on=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'],how='left')
#
#                                 ls1 = tuple(finalDataSalesItem['invoiceHeaderID'].dropna().unique())
#                                 print(len(finalDataSalesItem), 'data after merging for item table')
#                                 print(len(ls1), 'number of invoice header ID to be deleted from sales invoice items table')
#
#                                 if (len(ls1) > 1):
#                                     mydb.execute("Delete from sales_invoice_items where invoiceHeaderID in " + str(ls1) + "")
#                                 elif (len(ls1) == 1):
#                                     mydb.execute("Delete from sales_invoice_items where invoiceHeaderID = " + str(ls1[0]) + "")
#
#                                 item_ls = ['quantity', 'unit', 'taxableValue', 'igstRate', 'cgstRate', 'sgstOrUgstRate', 'igstAmt',
#                                            'cgstAmt', 'sgstOrUgstAmt', 'cessAmt', 'HSNorSAC', 'invoiceHeaderID']
#
#                                 for i in item_ls:
#                                     if i not in set(finalDataSalesItem.columns.tolist()):
#                                         finalDataSalesItem[i] = np.nan
#
#                                 finalDataSalesItem = finalDataSalesItem[item_ls]
#                                 finalDataSalesItem['srNo'] = ''
#                                 finalDataSalesItem['srNo'] = finalDataSalesItem.groupby('invoiceHeaderID').cumcount() + 1
#                                 print(finalDataSalesItem, 'records to insert into sales invoice item table')
#
#
#                                 finalDataSalesItem.to_sql(name='sales_invoice_items', con=mydb, if_exists='append', index=False, chunksize=50)
#
#                                 print("DUMPING DONE")
#                             return HttpResponse(json.dumps({"reason":"Sales upload successful","status":"success"}), status=status.HTTP_201_CREATED)
#
#     #############################################   PURCHASE PART  ##############################################################################
#
#             elif dataUploadEngine['Action_Status'] == 'TG-FILE' and typeData == 'GSTR2-Purchase':
#                 print('INSIDE PURCHASE UPLOAD ENGINE')
#                 try:
#                     raw = pd.read_excel(BASE_DIR+"/TG-FILE/"+ dataUploadEngine['filename'])
#                 except Exception as e:
#                     print(str(e))
#                     return HttpResponse(json.dumps({'reason': str(e), 'status': 'FAIL'}))
#                 if (len(raw) == 0):
#                     return HttpResponse(json.dumps({"reason": "No Records passed TG validation", "status": "FAIL"}))
#                 else:
#                     #####################   Column Mapping  #####################################################
#                     stage_tb = pd.DataFrame()
#                     for user, sys in dt.items():
#                         stage_tb[sys] = raw[user]
#                     ##############################################################################################
#                     df_final_s = stage_tb
#
# ######################################################################   DATABASE DUMPING START  ###################################################################################################
#                     print("##################################################### DB DUMPING START #################################################################################")
#
#                     pan_check = df_final_s.loc[df_final_s['Status'] == 'Success']
#                     print(len(pan_check),'pan check success data')
#                     if (len(pan_check) == 0):
#                         return HttpResponse(json.dumps({"reason": "PAN VALIDATION FAILED. PLEASE CHECK FILE CONTAINS VALID GSTIN", "status": "Fail"}))
#                     else:
#                         try:
#                             df_final_s.astype(str).to_sql(name='upload_purchase_result_table', con=mydb, if_exists='append',index=False, chunksize=500)
#                         except Exception as e:
#                             print(str(e))
#                             return HttpResponse(json.dumps({"reason": str(e), "status": "Connection Error"}))
#
#                         dataSuccess1 = pd.read_sql("SELECT * FROM upload_purchase_result_table where Reference_id in " +str(tuple(df_final_s['Reference_id'].unique()))+ " and Status='Success' ",mydb)
#                         print(len(dataSuccess1), "Success data from result purchase table")
#                         dataSuccess1.to_csv(r"C:\Users\Admin\Desktop\DATA FROM RESULT TABLE.csv")
#
#                         if (len(dataSuccess1) == 0):
#                             return HttpResponse(json.dumps({"reason": "All Invoice Records with status Fail", "status": "FAIL"}))
#                         else:
#                             dataSuccess1.rename(columns={
#                                 'Buyer Gstin': 'buyerGSTIN',
#                                 'Invoice Type': 'typeofinvoice',
#                                 'Invoice SubType': 'invoiceSubType',
#                                 'Invoice Date': 'invoiceDate',
#                                 'Invoice No': 'invoiceNo',
#                                 'Invoice Value': 'invoiceValue',
#                                 'quantity': 'quantity',
#                                 'Reverse Charge': 'reverseCharge',
#                                 'Seller Gstin': 'sellerGSTIN',
#                                 'unit': 'unit',
#                                 'Place Of Supply': 'pos',
#                                 'Taxable Value': 'taxableValue',
#                                 'Igst Rate': 'igstRate',
#                                 'Cgst Rate': 'cgstRate',
#                                 'Sgst Rate': 'sgstOrUgstRate',
#                                 'Igst Amount': 'igstAmt',
#                                 'Cgst Amount': 'cgstAmt',
#                                 'Sgst Amount': 'sgstOrUgstAmt',
#                                 'Cess Amount': 'cessAmt',
#                                 'Hsn Code': 'HSNorSAC',
#                                 'Rate': 'gstRate',
#                                 'invoiceFinancialPeriod': 'invoiceFinancialPeriod',
#                                 'Reference_id': 'Reference_id'}, inplace=True)
#
#                             for process in set(dataSuccess1['invoiceFinancialPeriod'].unique()):
#                                 print(process,'Financial Year')
#                                 dataPurchaseHeader = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
#                                 print("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'")
#                                 print(len(dataPurchaseHeader),'data for process')
#                                 dataPurchaseHeader.to_csv(r"C:\Users\Admin\Desktop\Data from sales invoice header.csv")
#                                 dataPurchaseHeader = dataPurchaseHeader.applymap(lambda x: x.strip() if type(x) == str else x)
#                                 dataPurchaseHeader = dataPurchaseHeader.fillna('')
#                                 dataSuccess = dataSuccess1.loc[dataSuccess1['invoiceFinancialPeriod'] == process]
#                                 dataSuccess = dataSuccess.fillna('')
#
#                                 header_ls = ['buyerGSTIN', 'typeofinvoice', 'invoiceSubType', 'invoiceDate', 'invoiceNo',
#                                              'invoiceValue', 'sellerGSTIN', 'pos', 'reverseCharge', 'Reference_id',
#                                              'invoiceFinancialPeriod', 'buyerID', 'financialPeriod', 'gstnStatus',
#                                              'invoiceStatus']
#
#                                 for i in header_ls:
#                                     if i not in set(dataSuccess.columns.tolist()):
#                                         dataSuccess[i] = ''
#
#                                 dataSuccess['sellerGSTIN'] = dataSuccess['sellerGSTIN'].astype(str).str.replace('nan', '')
#
#                                 dataUnique = dataSuccess[header_ls].drop_duplicates(subset=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'], keep='first')
#                                 dataReplace = dataUnique.loc[((dataUnique['sellerGSTIN'].isin(dataPurchaseHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataPurchaseHeader['invoiceNo'])) & (dataUnique['typeofinvoice'].isin(dataPurchaseHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataPurchaseHeader['buyerGSTIN'])) & (dataUnique['invoiceFinancialPeriod'].isin(dataPurchaseHeader['invoiceFinancialPeriod'])))]
#                                 print(len(dataReplace), 'data to replace')
#                                 dataReplace.to_csv(r"C:\Users\Admin\Desktop\Data to replace in sales invoice header.csv")
#
#                                 dataInsert = dataUnique[['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'invoiceSubType', 'gstnStatus', 'buyerID','invoiceStatus', 'financialPeriod', 'buyerGSTIN', 'invoiceDate', 'reverseCharge', 'pos','invoiceFinancialPeriod']].loc[~((dataUnique['sellerGSTIN'].isin(dataPurchaseHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataPurchaseHeader['invoiceNo'])) & (dataUnique['typeofinvoice'].isin(dataPurchaseHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataPurchaseHeader['buyerGSTIN'])))]
#                                 print(len(dataInsert), 'data to insert')
#                                 dataInsert.to_csv(r"C:\Users\Admin\Desktop\Data to insert in sales invoice header.csv")
#
#                                 if (len(dataInsert) != 0):
#                                     dataInsert.to_sql('purchase_invoice_header', mydb, if_exists='append', index=False,chunksize=100)
#
#                                 if (len(dataReplace) != 0):
#                                     print("INSIDE UPDATE")
#                                     count = 0
#                                     for index, replace in dataReplace.iterrows():
#                                         count = count + 1
#                                         updatedAt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                                         query = """update purchase_invoice_header a
#                                         set
#                                         a.invoiceSubType='""" + str(replace['invoiceSubType']) + """',
#                                         a.invoiceDate = '""" + str(replace['invoiceDate']) + """',
#                                         a.reverseCharge = '""" + str(replace['reverseCharge']) + """',
#                                         a.invoiceFinancialPeriod ='""" + str(replace['invoiceFinancialPeriod']) + """',
#                                         a.pos= '""" + str(replace['pos']) + """',
#                                         a.updatedAt= '""" + updatedAt + """',
#                                         a.financialPeriod= '""" + str(replace['financialPeriod']) + """',
#                                         a.invoiceStatus= '""" + str(replace['invoiceStatus']) + """'
#                                         where
#                                         a.sellerGSTIN= '""" + str(replace['sellerGSTIN']) + """'
#                                         and a.buyerGSTIN='""" + str(replace['buyerGSTIN']) + """'
#                                         and a.invoiceNo='""" + str(replace['invoiceNo']) + """'
#                                         and a.typeofinvoice='""" + str(replace['typeofinvoice']) + """'
#                                         and a.invoiceFinancialPeriod='""" + str(replace['invoiceFinancialPeriod']) + """'
#                                         and a.buyerGSTIN like '%%""" + pan_num + """%%' """
#                                         with mydb.begin() as conn:
#                                             conn.execute(query)
#                                     print("number of replace count ", count)
#
#                                 dataPurchaseItem = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
#                                 print("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'")
#                                 dataPurchaseItem = dataPurchaseItem.applymap(lambda x: x.strip() if type(x) == str else x)
#                                 dataPurchaseItem = dataPurchaseItem.fillna('')
#                                 print(len(dataPurchaseItem), 'data from invoice header for item table df creation')
#                                 dataPurchaseItem.to_csv(r"C:\Users\Admin\Desktop\Data from sales invoice header for item df.csv")
#                                 dataSuccess.to_csv(r"C:\Users\Admin\Desktop\DataSuccess.csv")
#
#                                 finalDataPurchaseItem = pd.merge(left=dataSuccess, right=dataPurchaseItem,on=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'],how='left')
#                                 ls1 = tuple(finalDataPurchaseItem['invoiceHeaderID'].dropna().unique())
#                                 print(len(finalDataPurchaseItem), 'data after merging for item table')
#                                 print(len(ls1), 'number of invoice header ID to be deleted from purchase invoice items table')
#                                 finalDataPurchaseItem.to_csv(r"C:\Users\Admin\Desktop\Final Data for item.csv")
#
#                                 if (len(ls1) > 1):
#                                     mydb.execute("Delete from purchase_invoice_items where invoiceHeaderID in " + str(ls1) + "")
#                                 elif (len(ls1) == 1):
#                                     mydb.execute("Delete from purchase_invoice_items where invoiceHeaderID = " + str(ls1[0]) + "")
#                                 item_ls = ['quantity', 'unit', 'taxableValue', 'igstRate', 'cgstRate', 'sgstOrUgstRate', 'igstAmt',
#                                            'cgstAmt', 'sgstOrUgstAmt', 'cessAmt', 'HSNorSAC', 'invoiceHeaderID']
#
#                                 for i in item_ls:
#                                     if i not in set(finalDataPurchaseItem.columns.tolist()):
#                                         finalDataPurchaseItem[i] = np.nan
#
#                                 finalDataPurchaseItem = finalDataPurchaseItem[item_ls]
#                                 finalDataPurchaseItem['srNo'] = ''
#                                 finalDataPurchaseItem['srNo'] = finalDataPurchaseItem.groupby('invoiceHeaderID').cumcount() + 1
#                                 print(finalDataPurchaseItem, 'records to insert into purchase invoice item table')
#
#                                 finalDataPurchaseItem.to_sql(name='purchase_invoice_items', con=mydb, if_exists='append', index=False,chunksize=50)
#
#                             return HttpResponse(json.dumps({"reason": "Purchase Upload successfully", "status": "success"}),status=status.HTTP_201_CREATED)
#             else:
#                 return HttpResponse(json.dumps({"reason": "Condition not satisfied for sales or purchase.", "status": "FAIL"}))
