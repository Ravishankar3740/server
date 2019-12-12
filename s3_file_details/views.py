############################################################  DJANGO FRAMEWORK IMPORTS  ###################################################################################################
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser, FileUploadParser
from rest_framework import status
from rest_framework.response import Response
from django.shortcuts import HttpResponse
############################################################  CUSTOM SCRIPT IMPORTS  ####################################################################################################
from .serializers import *
from .models import *
# from .s3
from .Reports import *
from .Dynamic_Rule_Script import *
from .filereading import Filere
from .Updated_Sales_Validation import Sales
from .Updated_Purchase_Validation import purchase
############################################################  LIBRARY IMPORTS  #########################################################################################################
import pymysql, json, os, re, pandas as pd, boto3, io, ast, mysql.connector, glob, xlrd, time, numpy as np
from itertools import zip_longest
from datetime import datetime
from time import strftime
from pathlib import Path
from boto3 import client
from io import StringIO
from pyxlsb import open_workbook
from pyexcelerate import Workbook
from django.conf import settings
############################################################  GLOBAL VARIABLES  #########################################################################################################
global connection
global BASE_DIR
import ast
from .uploadEngine import mydb
from collections import ChainMap
from .import_summary_report import *
from funcy import join
#########################################################################################################################################################################################
############################################################  StartView #########################################################################################################
class StartView(APIView):
    def post(self, request):
        try:
            Company_Id = request.data['Company_Id']
            type = request.data['File_Type']
            month = request.data['month2']
            financial_year = request.data['Financial_Year']
            print(financial_year)
            status2 = ['Deleted', 'Uploaded']
            sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=type, month2=month,financial_year=financial_year).exclude(Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
            print(sql_query)
            print(Company_Id,type,month,financial_year)
            print("data for deeps")
            return Response(({"status": sql_query}), status=status.HTTP_201_CREATED)
        except Exception as e:
            print(e)
            return Response(({"status": "Something Went Wrong,Please Try Again"}), status=status.HTTP_201_CREATED)
############################################################  Deleteview  #########################################################################################################
class DeleteView(APIView):
    def post(self, request):
        Company_Id = request.data['Company_Id']
        type = request.data['File_Type']
        month = request.data['month']
        financial_year = request.data['Financial_Year']
        delete = 'Deleted'
        sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=type, month2=month, financial_year=financial_year,status="Deleted").order_by('-id').values())
        return Response(({"status": sql_query}), status=status.HTTP_201_CREATED)
############################################################  processed view  #########################################################################################################
class Processed(APIView):
    def post(self, request):
        Company_Id = request.data['Company_Id']
        type = request.data['File_Type']
        month = request.data['month']
        financial_year = request.data['Financial_Year']
        uploaded = 'Uploaded'
        sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=type, month2=month, financial_year=financial_year,Action_Status=uploaded).order_by('-id').values())
        # print(sql_query,"processsed")
        return Response(({"status": sql_query, "statuss": "success"}), status=status.HTTP_201_CREATED)
############################################################  filter  #########################################################################################################
class Filter(APIView):
    def post(self, request):
        Company_Id = request.data['Company_Id']
        print(Company_Id)
        type = request.data['File_Type']
        print(type)
        month2 = request.data['month2']
        print(month2)
        financial_year = request.data['Financial_Year']
        print(financial_year)
        if type == "GSTR1-Sales" or type == "GSTR2-Purchase" or type == "General":
            sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=type, month2=month2,financial_year=financial_year).exclude(Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
            # print(sql_query,"filter")
            return Response(({"status": sql_query}), status=status.HTTP_201_CREATED)
############################################################  upload file  #########################################################################################################
class UploadView2(APIView):
    parser_classes = (MultiPartParser, FormParser, FileUploadParser)
    def post(self, request):
        datuu = request.data
        print(datuu, "datuu")
        comp_id = datuu['Company_Id']
        print(comp_id, "comp_id")
        type = datuu['type']
        print(type, "type")
        mon = datuu['month']
        # Company_Id = comp_id, type = datuu['type'], month2 = datuu['month2'],
        if filerecord.objects.filter(filename=datuu['filename']).exists():
            return Response(({"status2": "failed", "reason": "File Already Exist,Please Rename Your File"}),status=status.HTTP_201_CREATED)
        upload_data = UploadSerializer(data=request.data)
        if upload_data.is_valid():
            upload_data.save()
            uppu = upload_data.data
            print(uppu, "uppuuppu")
            print(upload_data.data['type'], "typeeeeeee")
            time = upload_data.data['Timestamp']
            id = upload_data.data['id']
            Company_Id = upload_data.data['Company_Id']
            month = str(upload_data.data['month2'])
            Action_Status = upload_data.data['Action_Status']
            filetype = upload_data.data['type']
            file_name1 = os.path.basename(upload_data.data['file'])
            fileExtension = file_name1.split(os.extsep)[-1]
            filename2 = os.path.splitext(file_name1)[0]
            if (filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'xlsx') or (filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'xls') or (filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'XLSX') or (filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'xlsb'):
                datta = time.replace("T", "_")
                s3_file_path = filename2 + "_" + datta + '.' + fileExtension
                print(s3_file_path, "s3_file_path")
                see2 = filerecord.objects.filter(id=id, filename_Timestamp='file')
                for i in see2:
                    i.raw_file_path = file_name1
                    i.RawFileRef_Id = id
                    i.filename_Timestamp = s3_file_path
                    i.save()
                BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                print(BASE_DIR, 'BASE_DIR')
                file_size = os.path.getsize(BASE_DIR + '/media/' + file_name1)
                file_size2 = str(file_size) + " kb"
                see3 = filerecord.objects.filter(file_size='new').update(file_size=file_size2)
                sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=filetype, month2=month).exclude(Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
                return Response(({"status": sql_query, "status2": "Success", "reason": "File Successfully Uploaded"}),status=status.HTTP_201_CREATED)
            elif (filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'csv'):
                print("m here for csv")
                datta = time.replace("T", "_")
                s3_file_path = filename2 + "_" + datta + '.' + fileExtension
                print(s3_file_path, "s3_file_path")
                see2 = filerecord.objects.filter(id=id, filename_Timestamp='file')
                for i in see2:
                    i.raw_file_path = file_name1
                    i.RawFileRef_Id = id
                    i.filename_Timestamp = s3_file_path
                    i.save()
                BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                print(BASE_DIR, 'BASE_DIR')
                file_size = os.path.getsize(BASE_DIR + '/media/' + file_name1)
                file_size2 = str(file_size) + " kb"
                see3 = filerecord.objects.filter(file_size='new').update(file_size=file_size2)
                sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=filetype, month2=month).exclude(Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
                return Response(({"status": sql_query, "status2": "Success", "reason": "File Successfully Uploaded"}),
                                status=status.HTTP_201_CREATED)
            elif filetype == 'General':
                datta = time.replace("T", "_")
                s3_file_path = filename2 + "_" + datta + '.' + fileExtension
                print(s3_file_path, "s3_file_path")
                see2 = filerecord.objects.filter(id=id, filename_Timestamp='file')
                for i in see2:
                    i.raw_file_path = file_name1
                    i.RawFileRef_Id = id
                    i.filename_Timestamp = s3_file_path
                    i.save()
                BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                print(BASE_DIR, 'BASE_DIR')
                file_size = os.path.getsize(BASE_DIR + '/media/' + file_name1)
                file_size2 = str(file_size) + " kb"
                see3 = filerecord.objects.filter(file_size='new').update(file_size=file_size2)
                sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=filetype, month2=month).exclude(Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
                return Response(({"status": sql_query, "status2": "Success", "reason": "File Successfully Uploaded"}),status=status.HTTP_201_CREATED)
            else:
                if fileExtension != 'xlsx' or fileExtension != 'csv' or fileExtension != 'xlsb':
                    deleting = filerecord.objects.filter(id=id).delete()
                    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    print(BASE_DIR, 'BASE_DIR')
                    os.remove(BASE_DIR + '/media/' + file_name1)
                    return HttpResponse(json.dumps({"status2": "failed", "reason": "Only xlsx,csv Files Allowed"}),status=status.HTTP_201_CREATED)
############################################################  delete file  #########################################################################################################
class Delete_files(APIView):
    def post(self, request):
        del_data = request.data['list']
        print(del_data)
        Company_Id = request.data['Company_Id']
        print(Company_Id)
        purpose = request.data['File_Type']
        print(purpose)
        time = str(datetime.now())
        ls = []
        for i in del_data:
            ls.append(i.get("id"))
        data_del = tuple(ls)
        print(data_del)
        for i in ls:
            deleting = filerecord.objects.filter(id=i).update(status='Deleted')
            see3 = filerecord.objects.filter(id=i).update(Deleted_at=time)
            see3 = filerecord.objects.filter(id=i).update(Action_Status='Deleted')
            see4 = filerecord.objects.filter(id=i).update(Deleted_by=Company_Id)
        return HttpResponse(json.dumps({"status": "success", "response": "Files Deleted"}),status=status.HTTP_201_CREATED)
############################################################  merge file  #########################################################################################################

class hard_query(APIView):
    def post(self, request):
        sheet_row_merge = request.data
        print(sheet_row_merge, "merge data")
        merge2 = []
        statuss = []
        for i in sheet_row_merge:
            merge2.append(i['actionStatus'])
        res = all(ele == merge2[0] for ele in merge2)
        for i in range(0, len(merge2)):
            statuss.append('New')
        print(merge2, "merge2")
        if merge2 != statuss:
            return HttpResponse(json.dumps({"status": "failed", "response": "You can Merge New Status Files Only"}),status=status.HTTP_201_CREATED)
        financial_year = sheet_row_merge[0]
        financial_year2 = financial_year['financial_year']
        type = sheet_row_merge[0]
        type2 = type['type']
        Company_Id = sheet_row_merge[0]
        Company_Id2 = Company_Id['companyId']
        statuss = sheet_row_merge[0]
        actionStatus = statuss['actionStatus']
        statuss2 = sheet_row_merge[1]
        actionStatus2 = statuss['actionStatus']
        month = sheet_row_merge[0]
        month2 = month['month']
        month3 = sheet_row_merge[0]
        month4 = month['month2']
        file = sheet_row_merge[0]
        file2 = file['fileName']
        print(file2, "file22222222222")
        ls = len(sheet_row_merge)
        file_id = []
        header = []
        sheet = []
        for i in range(ls):
            a = sheet_row_merge[i]['id']
            b = sheet_row_merge[i]['Sheet']
            c = sheet_row_merge[i]['Header']
            file_id.append(a)
            sheet.append(b)
            header.append(c)
        for i in range(0, len(file_id)):
            see3 = filerecord.objects.filter(id=file_id[i]).update(sheet_name=sheet[i], row_no=header[i])
        combined_df = pd.DataFrame()
        for i in file_id:
            print(i, "iiiiiiiiiiiii")
            file = list(filerecord.objects.filter(id=i).values('raw_file_path'))
            print(file, "fileeeeeeeeeeeeeeee")
            filenamee = file[0]
            fileExtension = filenamee['raw_file_path'].split(os.extsep)[-1]
            if fileExtension == 'xlsx' or fileExtension == 'csv' or fileExtension == 'xlsb' or fileExtension == "xls" or 'XLSX':
                sql_query = list(filerecord.objects.filter(id=i).values('filename', 'sheet_name', 'row_no', ))
                print(sql_query, "adsssddsd")
                file_name = sql_query[0]['filename']
                fileExtension = file_name.split(os.extsep)[-1]
                file_sheet = sql_query[0]['sheet_name']
                file_row = int(sql_query[0]['row_no'])
                d = Filere()
                df_data = d.fileread(fileExtension, file_sheet, file_name, file_row)
                print(df_data, "df_data")
                s = d.newfunc()
                print(s, "sssssssssssssss")
                df45 = df_data
                if (isinstance(df45, str)):
                    return HttpResponse(json.dumps({"status": "failed", "response": df45}),status=status.HTTP_201_CREATED)
                else:
                    combined_df = combined_df.append(df45).reset_index(drop=True)
        print(combined_df, "here is output")
        output = combined_df.columns
        print(len(output))
        dataaaa = set(s) & set(output)
        print(len(dataaaa))
        print("heehheehehe")
        if len(dataaaa) != len(output):
            return HttpResponse(json.dumps({"status": "failed", "response": "File Headers Should Be Same"}),status=status.HTTP_201_CREATED)
        batch_id = ''
        for j in file_id:
            batch_id += str(j)
        time = str(datetime.now())
        time2 = time.replace(':', '_')
        name = 'Merge_' + batch_id + '_' + time
        nn = name.replace(':', '_')
        nn2 = nn.replace('.', '_')
        nn3 = nn2 + '.xlsx'
        typee = sheet_row_merge[0]['type']
        print(typee, "typee")
        merg_name = "Merge_File_" + typee + '_' + time2 + ".csv"
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(BASE_DIR, 'BASE_DIR')
        combined_df.to_csv(BASE_DIR + "/Merge/" + merg_name, index=False)
        file_size = os.path.getsize(BASE_DIR + "/Merge/" + merg_name)
        file_size2 = str(file_size) + " kb"
        batch_ref = 'Batch_id_' + batch_id
        creating = filerecord.objects.create(file=file2, filename=merg_name, filetype='.xlsx',filename_Timestamp=merg_name, RawFileRef_Id=sheet_row_merge[0]['id'], Action_Status='Merge',row_no='', sheet_name='', HQId='', sub_state='', stock_id='',Branch_id='', Merge_by='', Merge_Batch_Id='', Deleted_by='',Archived_by='', processed_by='', raw_file_path=merg_name,processed_file_path='', error_file_path='', control_summary_path='',Merge_file_path='', Company_Id=Company_Id2, type=type2,financial_year=financial_year2, month=month2, file_size=file_size2,Merge_Reference=batch_ref, month2=month4)
        see3 = filerecord.objects.filter(filename=merg_name)
        for i in see3:
            i.Uploadedtimestamp = time
            i.Uploaded_by = time
            i.Timestamp = time
            i.row_no = '1'
            i.sheet_name = nn2
            i.save()
        return HttpResponse(json.dumps({"status": "success", "response": "Your Files Successfully Mergerd"}),status=status.HTTP_201_CREATED)
############################################################  Sheetname  #########################################################################################################
class Mapping_file_sheets(APIView):
    def post(self, request):
        Company_Id = request.data['Company_Id']
        merge_data = request.data['list']
        print(merge_data)
        type = merge_data.get("type")
        filename = merge_data.get("file")
        filename2 = merge_data.get("filename")
        print(filename, "this is filename")
        file = [filename2]
        print(file, "filename in list")
        actionStatus = merge_data.get("Action_Status")
        fileExtension = filename2.split(os.extsep)[-1]
        print(fileExtension, "fileExtension")
        print("111")
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(BASE_DIR, 'BASE_DIR')
        if actionStatus == 'New' and (type == 'GSTR1-Sales' or type == 'GSTR2-Purchase') or actionStatus == 'Compute' and (type == 'GSTR1-Sales' or type == 'GSTR2-Purchase') or actionStatus == 'Merge' and (type == 'GSTR1-Sales' or type == 'GSTR2-Purchase'):
            print("something")
            print(fileExtension)
            try:
                if actionStatus == 'New' and (fileExtension == 'xlsx' or fileExtension == 'xls' or fileExtension == 'XLSX'):
                    xl = pd.ExcelFile(BASE_DIR + '/media/' + filename)
                    print("in media")
                    sheetnames = xl.sheet_names
                    print("Sheet Name media:== ", sheetnames)
                    return HttpResponse(json.dumps({"status": "success", "sheet_names": sheetnames}),status=status.HTTP_201_CREATED)

                if actionStatus == 'Compute' and (fileExtension == 'xlsx' or fileExtension == 'xls' or fileExtension == 'XLSX'):
                    print("in compute")
                    xl = pd.ExcelFile(BASE_DIR + '/client_compute/' + file[0])
                    sheetnames = xl.sheet_names
                    print("Sheet Name Compute:== ", sheetnames)
                    return HttpResponse(json.dumps({"status": "success", "sheet_names": sheetnames}),status=status.HTTP_201_CREATED)

                elif actionStatus == 'New' and fileExtension == 'xlsb':
                    wb = open_workbook(BASE_DIR + '/media/' + filename)
                    sheetnames = wb.sheets
                    return HttpResponse(json.dumps({"status": "success", "sheet_names": sheetnames}),status=status.HTTP_201_CREATED)

                elif fileExtension == 'csv':
                    print("csvvvv")
                    print("222")
                    return HttpResponse(json.dumps({"status": "success", "sheet_names": file}),status=status.HTTP_201_CREATED)

            except Exception as e:
                print(e, "eeeeee")
                print(e)
                return HttpResponse(json.dumps({"status": "failed", "response": "Unsupported format, or corrupt file"}),status=status.HTTP_201_CREATED)
############################################################  col return and teplate validation and row validation  #########################################################################################################

class Create_Rule(APIView):
    def post(self, request):
        data_col = request.data['data']
        print("Create_Rule check:--- ", data_col)
        global temp_data
        temp_data = request.data['templateDetails']
        print(temp_data, "temp_data")
        file2 = data_col['file']
        fileExtension = data_col['filename'].split(os.extsep)[-1]
        Type = data_col['type']
        print(Type, "type")
        sheetname = temp_data['sheet_name']
        rowno = temp_data['row_no']
        Temp_Name = temp_data['Template_Name']
        print(Temp_Name, "temp name")
        comp = data_col['Company_Id']
        print(comp, "comp name")
        print(file2, "file2222222222")
        print("hahahahah")
        if Mapping.objects.filter(Company_Id=comp, Type=Type, Template_Name=Temp_Name).exists():
            return HttpResponse(json.dumps({"status": "failed", "response": "Template Name already exists"}))
        if (data_col['Action_Status'] == 'New' or data_col['Action_Status'] == 'Compute' or data_col['Action_Status'] == 'Merge') and (Type == 'GSTR1-Sales' or Type == 'GSTR2-Purchase'):
            print("in if")
            d = Filere()
            df_data = d.fileread(fileExtension, sheetname, data_col['filename'], rowno)
            print(df_data,"df_data")
            if (isinstance(df_data, str)):
                return HttpResponse(json.dumps({"status": "failed", "response": df_data}), status=status.HTTP_201_CREATED)
            else:
                df2 = df_data.rename(columns=lambda x: x.strip() if (x != None) else x)
                df3 = df2.applymap(lambda x: x.strip() if type(x) == str else x)
                global col
                col = list(df3.columns)
                col2 = ["Invoice No", "Invoice Value", "Taxable Value", "Igst Amount", "Cgst Amount", "Sgst Amount","GSTIN"]
                sql_query = list(Mapping.objects.filter(Company_Id=comp, Type=Type, Template_Name=Temp_Name).values())
                print(sql_query, "sql_query")
                # sys_col = sql_query[0]['id']
                # return HttpResponse(json.dumps({"status":"success","Custumer_Col":col, "System_Col":col2, "tempid":str(sys_col)}),status=status.HTTP_201_CREATED)
                return HttpResponse(json.dumps({"status": "success", "Custumer_Col": col, "System_Col": col2}),status=status.HTTP_201_CREATED)
############################################################  col return  #########################################################################################################
from sqlalchemy import create_engine
class file_details_sheet_row(APIView):
    def post(self, request):
        global data3
        data3 = request.data
        print(data3, "data3")
        Template_Name = data3['Template_Name']
        comp_id = data3['Company_Id']
        Type = data3['Type']
        temp_id = list(Mapping.objects.filter(Template_Name=Template_Name).values('id'))
        return HttpResponse(json.dumps({"columns": col, "status": "success"}), status=status.HTTP_201_CREATED)

############################################################  system columns return  #########################################################################################################

class system_col(APIView):
    def post(self, request):
        try:
            base = request.data['invoiceType']
            type1 = request.data['type']
            print("ggggg")
            connection = pymysql.connect(host='15.206.93.178', user="taxgenie", password="taxgenie*#8102*$",database="taxgenie_efilling")
            if len(base) == 1:
                invoice_type = 'where invoiceType = "' + base[0] + '" and type = "' + type1 + '"'
                print(invoice_type, "invoice_type1")
            else:
                invoice_type = 'where invoiceType in ' + str(tuple(base)) + ' and type = "' + type1 + '"'
            sysCol = pd.read_sql("SELECT DISTINCT `field`, `required` FROM `system col_s3` " + invoice_type, connection)
            print(sysCol, "sysCol")
            sys_col = sysCol.to_json(orient='records')
            connection.close()
            return HttpResponse(json.dumps({"status": "success", "response": sys_col}), status=status.HTTP_201_CREATED)
        except Exception as e:
            return HttpResponse(json.dumps({"status": "failed", "response": "No Invoice Type Selected"}),status=status.HTTP_201_CREATED)
############################################################  template activation #########################################################################################################

class Col_header(APIView):
    def post(self, request):
        data3=request.data
        print(data3, "col_data")
        map_data=request.data
        print(map_data,"map_data")
        query = pd.read_sql("select company_parent_id from company_master where companyID=" + str(map_data['Company_Id']) + "",mydb)
        print(query, "queryqueryquery")
        par_comp_id = str(query['company_parent_id'].iloc[0])
        mydb.dispose()
        see3 = Mapping.objects.create(filename=map_data['filename'], filename_Timestamp=map_data['filename_Timestamp'],Type=map_data['Type'], Company_Id=map_data['Company_Id'], sheet_name=map_data['sheet_name'],row_no=map_data['row_no'], Template_Name=map_data['Template_Name'],Action_Status=map_data['Action_Status'],column_header=map_data['column_header'],Template_Type = map_data['Temp_Identify'],Company_Parent_id=par_comp_id)
        temp_id = list(Mapping.objects.filter(Template_Name=data3['Template_Name']).values('id'))
        print(temp_id, "temp_id")
        tempid = temp_id[0]['id']
        return HttpResponse(json.dumps({"status": "success", "tempid": str(tempid)}), status=status.HTTP_201_CREATED)
############################################################  column header update #########################################################################################################

class col_submit(APIView):
    def post(self, request):
        col_data =request.data
        print(col_data,"col_data")
        mapping_header = (Mapping.objects.filter(Template_Name=col_data['Template_Name']).update(column_header=col_data['column_header']))
        return HttpResponse(json.dumps({"status": "success", "tempid": "sasdasd"}), status=status.HTTP_201_CREATED)
############################################################ choose template #########################################################################################################

class choose_template(APIView):
    def post(self, request):
        selection = request.data['number']
        print(selection)
        Company_Id = request.data['Company_Id']
        print(Company_Id, "COMPPP")
        Action_Status = request.data['Action_Status']
        print(Action_Status)
        type = request.data['Type']
        print(type)
        query = pd.read_sql("select company_parent_id from company_master where companyID=" + str(Company_Id) + "", mydb)
        print(query, "queryqueryquery")
        par_comp_id = str(query['company_parent_id'].iloc[0])
        mydb.dispose()
        if (selection == 'Tg') and (Action_Status == "New" or Action_Status == "Merge" or Action_Status == "Compute"):
            sql_query = list(Mapping.objects.filter(Company_Parent_id=par_comp_id, Type=type,Template_Type='222').order_by('-id').values('id', 'Template_Name'))
            print(sql_query, "datatatataa")
            return HttpResponse(json.dumps({"status": "success", "reason": sql_query}), status=status.HTTP_201_CREATED)
        elif (selection == 'Comp') and (Action_Status == "New" or Action_Status == "Merge"):
            print(Company_Id,type,"queryyy")
            sql_query = list(Mapping.objects.filter(Company_Parent_id=par_comp_id, Type=type,Template_Type='111').order_by('-id').values('id', 'Template_Name'))
            print(sql_query, "template listttt")#
            return HttpResponse(json.dumps({"status": "success", "reason": sql_query}), status=status.HTTP_201_CREATED)
        else:
            return HttpResponse(json.dumps({"status": "warning", "reason": "You Have Already Processed File for this Process"}),status=status.HTTP_201_CREATED)
############################################################ tg processing #########################################################################################################

class Tg_File_Processing(APIView):
    def post(self, request):
        # try:
        Action_Status = request.data['actionstatus']
        print(Action_Status, "actionstatus")
        temp_name = request.data['Template_Name']
        temp_name2 = temp_name['Template_Name']
        raw_path = request.data['raw_file_path']
        month2 = request.data['month2']
        month = request.data['month']
        year = request.data['financial_year']
        tg = request.data['Tg_Data']
        print(tg, "tggggggggggg")
        Type = request.data['Type']
        print(Type, "TypeType")
        Company_Id = request.data['Company_Id']
        file = request.data['filename']
        print(file, "processing file")
        fileExtension = file.split(os.extsep)[-1]
        print(fileExtension, "fileExtension is here")
        gstin = request.data['GSTIN']
        pan_num =gstin[2:-3]
        raw_id2 = list(filerecord.objects.filter(filename=file).values('RawFileRef_Id'))
        print(raw_id2,"raw_id2")
        raw_id=raw_id2[0]['RawFileRef_Id']
        print("This is pan +++++++++++++++++++++++++++++++++++++++++++++++", pan_num)
        if Action_Status == 'TG-FILE':
            return HttpResponse(json.dumps({"status": "warning", "reason": "You Have Already Computed File"}),status=status.HTTP_201_CREATED)
        elif Action_Status == 'New' and (Type == 'GSTR1-Sales' or Type == 'GSTR2-Purchase') or Action_Status == 'Compute' and (Type == 'GSTR1-Sales' or Type == 'GSTR2-Purchase') or Action_Status == 'Merge' and (Type == 'GSTR1-Sales' or Type == 'GSTR2-Purchase'):
            sql_query = list(Mapping.objects.filter(Company_Id=Company_Id, Type=Type, Template_Name=temp_name2).values())
            row = sql_query[0]['row_no']
            sheet = sql_query[0]['sheet_name']
            map1 = list(Mapping.objects.filter(Company_Id=Company_Id, Type=Type, id=temp_name['id']).values('id','column_header'))
            print(map1,"mappii")
            column_header = map1[0]['column_header']
            temp_id = map1[0]['id']
            print(column_header, "column_header")
            res = json.loads(column_header)
            dt =join(res)
            print(dt,"dttt")
            print(type(dt))
            d = Filere()
            df_data = d.fileread(fileExtension, sheet, file, row)
            if (isinstance(df_data, str)):
                print("in str")
                if str(df_data).startswith('Invalid'):
                    exception = 'SheetName Should Be Same as Per Selected Template Details'
                else:
                    exception = 'Row Numeber Should Be Same as Per Selected Template Details'
                return HttpResponse(json.dumps({"status": "failed", "reason": exception}),
                                    status=status.HTTP_201_CREATED)
            else:
                df2 = df_data.rename(columns=lambda x: x.strip())
                df2 = df2.applymap(lambda x: x.strip() if type(x) == str else x)
                ls2 = df2.columns.tolist()
                df31 = df2.select_dtypes(exclude=['object'])
                df41 = df2.select_dtypes(include=['object']).apply(pd.to_numeric, errors='ignore')
                df3 = pd.concat([df31, df41], axis=1)
                df3 = df3[ls2]

                statuss = 'TG-FILE'
                print("m in tg")
                # df3 = df_data.rename(columns=lambda x: x.strip() if (x != None) else x)
                # df3 = df3.applymap(lambda x: x.strip() if type(x) == str else x)
                df3['M_id'] = df3.index + 1
                stage_tb = pd.DataFrame()
                # dt=dt
                print(dt,"dtyaaa")
                for user, sys in dt.items():
                    stage_tb[sys] = df3[user]
                stage_tb['M_id'] = df3['M_id']
                stage_tb['M_id']
                print(stage_tb.columns, "stage_tb")
                print(Type, "stage_tbstage_tb")

                if Type == 'GSTR1-Sales' and (Action_Status == 'New' or Action_Status == 'Compute'):
                    print("GSTR1-SalesGSTR1-Sales")
                    timeStamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # TIMESTAMP FOR REFERENCE ID
                    output = Sales(stage_tb, pan_num, request.data['Type'], timeStamp, request.data['month'],raw_id)
                    print(output.columns, "for upload")
                    output = pd.merge(left=df3, right=output[['M_id', 'Status', 'reason', 'Reference_id', 'invoiceFinancialPeriod', 'sellerID','financialPeriod', 'gstnStatus', 'RawFileRef_Id', 'invoiceStatus']], on=['M_id'],how='left')
                    # output.to_csv(r"C:\Users\Admin\Desktop\output inside if.csv")
                elif Type == 'GSTR2-Purchase' and (Action_Status == 'New' or Action_Status == 'Compute'):
                    print("GSTR2-PurchaseGSTR2-Purchase")
                    timeStamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # TIMESTAMP FOR REFERENCE ID
                    print(stage_tb.columns,"stagetbb")
                    output = purchase(stage_tb, pan_num, request.data['Type'], timeStamp, request.data['month'],raw_id)
                    output = pd.merge(left=df3, right=output[['M_id', 'Status', 'reason', 'Reference_id', 'invoiceFinancialPeriod', 'buyerID','financialPeriod', 'gstnStatus', 'RawFileRef_Id', 'invoiceStatus']], on=['M_id'],how='left')
            time = str(datetime.now())
            filename_wout_ext = Path(raw_path).stem
            name = str(filename_wout_ext) + '_' + time
            nn = name.replace(':', '_')
            nn2 = nn.replace('.', '_')
            nn3 = nn2 + '.xlsx'
            # success = output.loc[output['reason']=='']
            BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            print(BASE_DIR, 'BASE_DIR')
            for i in list(output.select_dtypes(include=['datetime64'])):
                output[i] = output[i].dt.strftime("%d-%m-%Y")
            data = [output.columns.tolist(), ] + output.fillna('').values.tolist()
            wb = Workbook()
            wb.new_sheet(sheet_name='Sheet1', data=data)
            wb.save(BASE_DIR + "/TG-FILE/" + nn3)
            # success.to_csv(BASE_DIR+"/TG-FILE/"+nn3,index=False)
            Fail = output.loc[output['reason'] != '']
            for i in list(Fail.select_dtypes(include=['datetime64'])):
                Fail[i] = Fail[i].dt.strftime("%d-%m-%Y")
            data = [Fail.columns.tolist(), ] + Fail.fillna('').values.tolist()
            wb = Workbook()
            wb.new_sheet(sheet_name='Sheet1', data=data)
            wb.save(BASE_DIR + "/Error_files/" + nn3)
            file_size = os.path.getsize(BASE_DIR + "/TG-FILE/" + nn3)
            file_size2 = str(file_size) + "kb"
            Tg_report = TG_GOV_summary_reports(output, dt,Type)
            print(Tg_report, "Tg_report++++++++++++++++")
            CREATING = filerecord.objects.create(file=nn3, filename=nn3, filetype='.xlsx', filename_Timestamp=nn3,RawFileRef_Id=raw_id, Action_Status='TG-FILE', row_no='',sheet_name='', HQId='', sub_state='', stock_id='',Branch_id='', Merge_by='', Merge_Batch_Id='',Deleted_by='', Archived_by='', processed_by='',raw_file_path=raw_path, processed_file_path=nn3,error_file_path=nn3, control_summary_path='',Merge_file_path='', Company_Id=Company_Id, type=Type,financial_year=year, month=month, file_size=file_size2,Merge_Reference='-', GSTIN=gstin, template_id=temp_id,status=statuss, month2=month2)
            com_id = list(filerecord.objects.filter(filename=nn3).values('id'))
            print(com_id)
            see3 = filerecord.objects.filter(id=com_id[0]['id'])
            for i in see3:
                i.Row_count = Tg_report['Row_count']
                i.Columns_count = Tg_report['Columns_count']
                i.GSTIN_count = Tg_report['GSTIN_count']
                i.Unique_Invoice_Count = Tg_report['Unique_Invoice_Count']
                i.Invoice_Value = Tg_report['Invoice_Value']
                i.Taxable_Value = Tg_report['Taxable_Value']
                i.IGST = Tg_report['IGST']
                i.CGST = Tg_report['CGST']
                i.SGST_UTGST = Tg_report['SGST_UTGST']
                i.CESS = Tg_report['CESS']
                i.Report_Type = Tg_report['Report_Type']
                i.save()
            see3 = filerecord.objects.filter(filename=nn3).update(Uploadedtimestamp=time)
            see3 = filerecord.objects.filter(filename=nn3).update(Uploaded_by=time)
            see3 = filerecord.objects.filter(filename=nn3).update(Timestamp=time)
            return HttpResponse(json.dumps({"status": "success", "reason": "Your File Successfully Processed"}),status=status.HTTP_201_CREATED)
        # except KeyError as e:
        #     print(e,"eeeeeuu")
        # return HttpResponse(json.dumps({"status": "failed", "reason": str(e) + " Columns  not present in File columns"}),status=status.HTTP_201_CREATED)
############################################################ client processing #########################################################################################################

class client_file_processing(APIView):
    def post(self, request):
        temp_name = request.data['Template_Name']
        print(temp_name)
        print(temp_name,"temp_name5555")
        client = request.data['Client_Data']
        client2 = client[0]
        temp_name2 = temp_name['Template_Name']
        raw_path = request.data['raw_file_path']
        year = request.data['financial_year']
        month = request.data['month']
        month2 = request.data['month2']
        GSTIN = request.data['GSTIN']
        Type = request.data['Type']
        Company_Id = request.data['Company_Id']
        file = request.data['file']
        actionstatus = request.data['actionstatus']
        fileExtension = client2['filename'].split(os.extsep)[-1]
        raw_id2 = list(filerecord.objects.filter(file=file).values('RawFileRef_Id'))
        print(raw_id2, "raw_id2")
        raw_id = raw_id2[0]['RawFileRef_Id']
        # ++++++++++++++++++++++++++++++++++Raw Report+++++++++++++++++++++++++++++++++++++++++
        if actionstatus == 'New' and (Type == 'GSTR1-Sales' or Type == 'GSTR2-Purchase') or actionstatus == 'Merge' and (Type == 'GSTR1-Sales' or Type == 'GSTR2-Purchase'):
            print("m in if")
            sql_query = list(Mapping.objects.filter(Company_Id=Company_Id, Type=Type, id=temp_name['id']).exclude(Type='').values())
            print(sql_query, "sql_query")
            row = sql_query[0]['row_no']
            sheet = sql_query[0]['sheet_name']
            statuss = 'Compute'
            d = Filere()
            df_data = d.fileread(fileExtension, sheet, client2['filename'], row)
            if (isinstance(df_data, str)):
                print("in str")
                if str(df_data).startswith('Invalid'):
                    exception='SheetName Should Be Same as Per Selected Template Details'
                else:
                    exception='Row Numeber Should Be Same as Per Selected Template Details'
                return HttpResponse(json.dumps({"status": "failed", "reason": exception}), status=status.HTTP_201_CREATED)
            # ++++++++++++++++Raw report++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            else:
                try:
                    df2 = df_data.rename(columns=lambda x: x.strip())
                    df2 = df2.applymap(lambda x: x.strip() if type(x) == str else x)
                    ls2 = df2.columns.tolist()
                    df31 = df2.select_dtypes(exclude=['object'])
                    df41 = df2.select_dtypes(include=['object']).apply(pd.to_numeric, errors='ignore')
                    df2 = pd.concat([df31, df41], axis=1)
                    df2 = df2[ls2]
                    map1 = list(Mapping.objects.filter(Company_Id=Company_Id, Type=Type, Template_Name=temp_name2).values())
                    print(map1, "map1")
                    print(map1[0]['column_header'], "hhhh")
                    map2 = map1[0]['column_header']
                    print(type(map2))
                    print(map2,"maaaap2")
                    print("shareit")
                    raw_dt = {}
                    if (map2 is not None):
                        dao = eval(map2)
                        for i in dao:
                            raw_dt.update(i)
                        print(raw_dt, "lsssss")
                        raw_report = Raw_summary_reports(df2, raw_dt)
                        print(raw_report, "Raw reports")
                        see3 = filerecord.objects.filter(id=raw_id)
                        for i in see3:
                            i.Row_count = raw_report['Row_count']
                            i.Columns_count = raw_report['Columns_count']
                            i.GSTIN_count = raw_report['GSTIN_count']
                            i.Unique_Invoice_Count = raw_report['Unique_Invoice_Count']
                            i.Invoice_Value = raw_report['Invoice_Value']
                            i.Taxable_Value = raw_report['Taxable_Value']
                            i.IGST = raw_report['IGST']
                            i.CGST = raw_report['CGST']
                            i.SGST_UTGST = raw_report['SGST_UTGST']
                            i.CESS = raw_report['CESS']
                            i.Report_Type = raw_report['Report_Type']
                            i.save()
                except Exception as e:
                    print(e)
                see3 = TgComparisionandarithmeticandfuction2.objects.filter(template_name=temp_name2).update(companyid=Company_Id)
                try:
                    print("m in else")
                    A = TgComparisionandarithmeticandfuction2.objects.filter(companyid=Company_Id,template_name=temp_name2).values()
                    print(A, "AAAAA")
                    rule = pd.DataFrame(A)
                    print(rule, "rulerulerulerule")
                    print(rule.columns, "ruleeee")
                    # ----------------------ORIGINAL DATA WITH OUTPUT
                    df2['M_id'] = (df2.index + 1)
                    df_raw = df2.copy()
                    df = purchase_engine(df2, rule)
                    size_raw = len(df_raw.columns)
                    size_raw = (size_raw - 1)
                    df = df.iloc[:, size_raw:]
                    output = pd.merge(left=df_raw, right=df, on=['M_id'], how='left').reset_index(drop=True)
                    # output = "\t" + output.fillna('').astype(str)
                    del output['M_id']
                    time = str(datetime.now())
                    filename_wout_ext = Path(file).stem
                    name = filename_wout_ext + '_' + time
                    nn = name.replace(':', '_')
                    nn2 = nn.replace('.', '_')
                    time = str(datetime.now())
                    nn3 = nn2 + '.xlsx'
                    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    print(BASE_DIR, 'BASE_DIR')
                    for i in list(output.select_dtypes(include=['datetime64'])):
                        output[i] = output[i].dt.strftime("%d-%m-%Y")
                    data = [output.columns.tolist(), ] + output.fillna('').values.tolist()
                    wb = Workbook()
                    wb.new_sheet(sheet_name='Sheet1', data=data)
                    wb.save(BASE_DIR + "/client_compute/" + nn3)
                    # output.to_excel(BASE_DIR+"/client_compute/"+nn3,index=False)
                    file_size = os.path.getsize(BASE_DIR + "/client_compute/" + nn3)
                    file_size2 = str(file_size) + " kb"
                    CREATING = filerecord.objects.create(file=file, filename=nn3, filetype='.xlsx',
                                                         filename_Timestamp=nn3, RawFileRef_Id=raw_id,
                                                         Action_Status='Compute', row_no='', sheet_name='',
                                                         HQId='', sub_state='', stock_id='', Branch_id='',
                                                         Merge_by='', Merge_Batch_Id='', Deleted_by='',
                                                         Archived_by='', processed_by='', raw_file_path=raw_path,
                                                         processed_file_path=nn3, error_file_path='',
                                                         control_summary_path='', Merge_file_path='',
                                                         Company_Id=Company_Id, type=Type, financial_year=year,
                                                         month=month, file_size=file_size2, Merge_Reference='',
                                                         GSTIN=GSTIN, status=statuss, month2=month2)
                    see3 = filerecord.objects.filter(filename=nn3).update(Uploadedtimestamp=time)
                    see3 = filerecord.objects.filter(filename=nn3).update(Uploaded_by=time)
                    see3 = filerecord.objects.filter(filename=nn3).update(Timestamp=time)

                    report_write = filerecord.objects.filter(RawFileRef_Id=client2['id']).values('Report_Type',
                                                                                                 'Row_count',
                                                                                                 'Columns_count',
                                                                                                 'GSTIN_count',
                                                                                                 'Unique_Invoice_Count',
                                                                                                 'Invoice_Value',
                                                                                                 'Taxable_Value',
                                                                                                 'IGST', 'CGST',
                                                                                                 'SGST_UTGST', 'CESS')
                    df9 = pd.DataFrame(report_write,
                                       columns=['Report_Type', 'Row_count', 'Columns_count', 'GSTIN_count',
                                                'Unique_Invoice_Count', 'Invoice_Value', 'Taxable_Value', 'IGST',
                                                'CGST', 'SGST_UTGST', 'CESS'])
                    try:
                        if map2 is not None:
                            compute_report = Compute_summary_reports(output, raw_dt)
                            print(compute_report, "compute_report++++++++++++++++")
                            com_id = list(filerecord.objects.filter(filename=nn3).values('id'))
                            print(com_id)
                            # ++++++++++++++Compute update+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                            see3 = filerecord.objects.filter(id=com_id[0]['id'])
                            for i in see3:
                                i.Row_count = compute_report['Row_count']
                                i.Columns_count = compute_report['Columns_count']
                                i.GSTIN_count = compute_report['GSTIN_count']
                                i.Unique_Invoice_Count = compute_report['Unique_Invoice_Count']
                                i.Invoice_Value = compute_report['Invoice_Value']
                                i.Taxable_Value = compute_report['Taxable_Value']
                                i.IGST = compute_report['IGST']
                                i.CGST = compute_report['CGST']
                                i.SGST_UTGST = compute_report['SGST_UTGST']
                                i.CESS = compute_report['CESS']
                                i.Report_Type = compute_report['Report_Type']
                                i.save()
                    except Exception as e:
                        print(e,"eee in client")
                    # df9.to_excel("C:/Users/Admin/Desktop/report.xlsx")
                    return HttpResponse(json.dumps({"status": "success", "reason": "Your File Successfully Processed"}),status=status.HTTP_201_CREATED)
                except KeyError as e:
                    return HttpResponse(json.dumps({"status": "failed", "reason": str(e) + " Column is not present in '" + temp_name2 + "'template"}), status=status.HTTP_201_CREATED)
############################################################ mapping view #########################################################################################################

class Mappingview(APIView):
    def post(self, request):
        temp_name = request.data['Template_Name']
        print(temp_name)
        comp_id = request.data['Company_Id']
        print(comp_id)
        type = request.data['type']
        print(type)
        see = list(Mapping.objects.filter(Template_Name=temp_name).values())
        print(see,"ssssss")
        dtaat = see[0]['column_header']
        # a = dtaat['column_header']
        print(dtaat)
        if dtaat is None:
            return HttpResponse(json.dumps({"status": "failed", "reason": "You have not mapped"}))
        else:
            d=ast.literal_eval(dtaat)
            return HttpResponse(json.dumps(d))
############################################################ deleting template #########################################################################################################

class Delete_temp(APIView):
    def post(self, request):
        process = request.data['process']
        print(process)
        temp_name = request.data['Template_Name']
        print(temp_name)
        comp_id = request.data['Company_Id']
        print(comp_id)
        id_data = request.data['id']
        print(id_data)
        type = request.data['type']
        print(type)
        if process == "Compute" or process == "TG-File":
            deleteing = Mapping.objects.filter(id=id_data).update(Template_Type='000')
            return HttpResponse(json.dumps({"status": "success", "reason": "Your Template Deleted"}))
        else:
            return HttpResponse(json.dumps({"status": "failed", "reason": "Something Went Wrong"}))
############################################################ masterfile upload #########################################################################################################

class Master_table(APIView):
    def post(self, request):
        data_read = request.data
        print(data_read, "data_read")
        filename = str(data_read['file'])
        sheet = str(data_read['sheet_name'])
        Row = data_read['row_no']
        fileExtension = filename.split(os.extsep)[-1]
        print(filename, sheet, Row, fileExtension)
        print("print")
        if fileExtension == 'xlsb':
            try:
                df_data = []
                xls_file = open_workbook(request.data['file'])
                print("ASFDasdfghjdsdfghjFFSDFSDF")
                with xls_file.get_sheet(sheet) as sheet:
                    for row in sheet.rows():
                        df_data.append([item.v for item in row])
                    df_data = pd.DataFrame(df_data[int(Row):], columns=df_data[int(Row) - 1])
                    print(df_data, "df_data")
                columns = list(df_data.columns)
                print(columns, "col")
                colu = all(isinstance(n, str) for n in columns)
                print(colu, "colucolucolu")
                if (colu == True):
                    master_data = Master_table_serializer(data=request.data)
                    if master_data.is_valid():
                        master_data.save()
                        see3 = Mastertable.objects.filter(id=master_data.data['id']).update(MasterFileColumns=columns)
                        return HttpResponse(json.dumps({"status": "Success", "reason": "Master File Uploaded"}))
                else:
                    df_data = 'Incorrect Row Number'
                return HttpResponse(json.dumps({"status": "failed", "response": df_data}))
            except Exception as e:
                print(e, "EEEEEEEEEEE")
                if str(e).endswith('is not in list'):
                    e = 'Invalid Sheetname'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).endswith('index out of range'):
                    e = 'At Given Row Number Data Not Present'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).startswith('File is not a zip'):
                    e = 'File is corrupted/Unable to read'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).startswith('Passed'):
                    e = 'At Given Row Number Data Not Present'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                else:
                    return HttpResponse(json.dumps({"status": "failed", "response": str(e)}))
        elif (fileExtension == 'xlsx' or fileExtension == 'XLSX' or fileExtension == 'xls'):
            try:
                print("m in xlsx")
                df = pd.read_excel(request.data['file'], sheet_name=str(sheet), header=int(Row) - 1)
                print(df, "xlsx")
                columns = list(df.columns)
                print(columns, "col")
                colu = all(isinstance(n, str) for n in columns)
                print(colu, "colucolucolu")
                if (colu):
                    master_data = Master_table_serializer(data=request.data)
                    if master_data.is_valid():
                        master_data.save()
                        see3 = Mastertable.objects.filter(id=master_data.data['id']).update(MasterFileColumns=columns)
                        return HttpResponse(json.dumps({"status": "Success", "reason": "Master File Uploaded"}))
                else:
                    df_data = 'Incorrect Row Number'
                return HttpResponse(json.dumps({"status": "failed", "response": df_data}))
            except Exception as e:
                print(e)
                if str(e).startswith('No'):
                    e = 'Invalid Sheetname'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).startswith('Unsupported'):
                    e = 'File is corrupted/Unable to read'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).startswith('Passed'):
                    e = 'At Given Row Number Data Not Present'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                else:
                    return HttpResponse(json.dumps({"status": "failed", "response": str(e)}))
        elif fileExtension == 'csv':
            df = pd.read_csv(request.data['file'], encoding="ISO-8859-1")
            print(df, "csv")
            columns=df.columns
            master_data = Master_table_serializer(data=request.data)
            if master_data.is_valid():
                master_data.save()
                see3 = Mastertable.objects.filter(id=master_data.data['id']).update(MasterFileColumns=columns)
                return HttpResponse(json.dumps({"status": "Success", "reason": "Master File Uploaded"}))
############################################################ getmasterfile list  #########################################################################################################
class Get_MaaterFile_list(APIView):
    def post(self, request):
        comp_id = request.data['Company_Id']
        type = request.data['Type']
        # Company_Id2 = comp_id.replace('"', '')
        dataaa = list(Mastertable.objects.filter(Company_Id=comp_id, Type=type).values('id', 'file'))
        print(dataaa, "dataaa")
        file_list = []
        for i in dataaa:
            filename = i['file']
            id = i['id']
            base = os.path.basename(filename)
            file_list.append({"id": id, "file": base})
        print(file_list, "file_list")
        return HttpResponse(json.dumps({"status": "Success", "response": file_list}))

        #
############################################################  master file columns  #########################################################################################################

class Masterfile_columns(APIView):
    def post(self, request):
        read_data = request.data
        print(read_data, "read data")
        map1 = list(Mastertable.objects.filter(id=read_data['id']).values('MasterFileColumns'))
        print(map1, "map")
        columns = map1[0]['MasterFileColumns']
        columns2 = ast.literal_eval(columns)
        return HttpResponse(json.dumps({"status": "Success", "response": columns2}), status=status.HTTP_201_CREATED)

############################################################  download file  #########################################################################################################

class download_file(APIView):
    def post(self, request):
        downoad_data = request.data['data']
        print(downoad_data, "downoad_data")
        filename = downoad_data['filename']
        print(filename, "filename")
        file = downoad_data['raw_file_path']
        print(file, "file")
        folder = downoad_data['Action_Status']
        print(folder, "folder")
        select = request.data['number']
        print(select, "select")
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(BASE_DIR, 'BASE_DIR')
        try:
            if folder == 'TG-FILE' and select == '1':
                genCSVPath = (BASE_DIR + "/TG-FILE/" + filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
            elif folder == 'TG-FILE' and select == '2':
                genCSVPath = (BASE_DIR + "/Error_files/" + filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
            elif folder == 'TG-FILE' and select == '3':
                genCSVPath = (BASE_DIR + "/Control_Summary/" + filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            return e
        try:
            if folder == 'New' and select == '4':
                genCSVPath = (BASE_DIR + "/media/" + file)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer,content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                # filen = filename
                response['Content-Disposition'] = b'attachment; filename=%s' % file.encode(encoding="utf-8")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            print(e, "eeee")
            return HttpResponse(
                json.dumps({"status": "failed", "response": "file is not processed/uploaded on server"}),
                status=status.HTTP_201_CREATED)
        try:
            if folder == 'TG-FILE' and select == '4':
                # print("m in else")
                print("m here for raw")
                print(downoad_data['raw_file_path'], "filefilefile")
                if len(glob.glob(BASE_DIR + "/media/" + downoad_data['raw_file_path'])) != 0:
                    genCSVPath = (BASE_DIR + "/media/" + downoad_data['raw_file_path'])
                else:
                    genCSVPath = (BASE_DIR + "/Merge/" + downoad_data['raw_file_path'])
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer,
                                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                response['Content-Disposition'] = b'attachment; filename=%s' % downoad_data['raw_file_path'].encode(
                    encoding="utf-8")
                response['Cache-Control'] = 'no-cache'
                return response

            elif folder == "Compute" and select == '4':
                arr = os.listdir(BASE_DIR + '/Merge/')
                if len(glob.glob(BASE_DIR + "/media/" + file)) != 0:
                    genCSVPath = (BASE_DIR + "/media/" + file)
                else:
                    genCSVPath = (BASE_DIR + "/Merge/" + file)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer,
                                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                response['Content-Disposition'] = b'attachment; filename=%s' % file.encode(encoding="utf-8")
                response['Cache-Control'] = 'no-cache'
                return response

        except Exception as e:
            return e
        try:
            if folder == 'Merge' and select == '4':
                genCSVPath = (BASE_DIR + "/Merge/" + filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                response['Content-Disposition'] = b'attachment; filename=%s' % filename.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            return e
        try:
            if folder == 'Compute' and select == '1':
                genCSVPath = (BASE_DIR + "/client_compute/" + filename)
                print(genCSVPath, "genCSVPathgenCSVPath")
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer,
                                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                # filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filename.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
            else:
                pass
        except Exception as e:
            return e
        # ----------------------------------------------Done-----------------------------------------#
        # ----------------------------------------------Done-----------------------------------------#
        try:
            if folder == 'New' and select == '4':
                genCSVPath = (BASE_DIR + "/Merge/" + filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer,content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                filen = filename + '.xlsx'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            return e

class testt(APIView):
    def post(self, request):
        # rule = pd.read_sql("SELECT * from `tg_comparisionandarithmeticandfuction` WHERE `companyId`='"+str(Company_Id)+"'and Template_Name='"+str(temp_name2)+"'",connection)
        datatatata = list(TgComparisionandarithmeticandfuction2.objects.filter(companyid=2408, template_name='abhil comp').values())
        # (Mastertable.objects.filter(id=read_data['id']).values())
        print(datatatata, "dataaa")
        return HttpResponse(json.dumps({"status": "failed", "response": "df_data"}), status=status.HTTP_201_CREATED)
############################################################  sheet row view  #########################################################################################################

class Sheet_ViewRule(APIView):
    def post(self, request):
        comp = request.data['view_rule_data']
        print(comp,"comppp")
        map1 = list(Mapping.objects.filter(Company_Id=comp['Company_Id'],Type=comp['type'],Template_Name=comp['Template_Name']).values('row_no', 'sheet_name'))
        print(map1,"mappp")
        return HttpResponse(json.dumps({"status": "sucesss", "response": map1}), status=status.HTTP_201_CREATED)
############################################################  Download file  #########################################################################################################
class Download_RuleBuilder_Rule(APIView):
    def post(self,request):
        print("download")
        download = request.data['download_rule_data']
        print("Download rule", download)
        comp=download['Company_Id']
        print(comp)
        dowwnload_Data = list(TgComparisionandarithmeticandfuction2.objects.filter(companyid=comp, template_name=str(download["Template_Name"])).values())
        return Response({"status": dowwnload_Data},status=status.HTTP_201_CREATED)
############################################################  Summary_report   #########################################################################################################
class Summary_report(APIView):
    def post(self, request):
        summart_data = request.data
        print(summart_data,"summmm")
        if summart_data['RawFileRef_Id']:
            report_write = list(filerecord.objects.filter(RawFileRef_Id=summart_data['RawFileRef_Id']).values())
            print(report_write, "report_write")
            df9 = pd.DataFrame(report_write)
            print(df9)
            print(df9['id'])
            df9 = df9.drop_duplicates(subset='Action_Status', keep='last')
            df9 = df9[['Report_Type', 'Row_count', 'Columns_count', 'GSTIN_count', 'Unique_Invoice_Count', 'Invoice_Value','Taxable_Value', 'IGST', 'CGST', 'SGST_UTGST', 'CESS']]

            if 'CS1' in list(df9['Report_Type']):
                a = (df9[['Row_count', 'Columns_count', 'GSTIN_count', 'Unique_Invoice_Count', 'Invoice_Value','Taxable_Value', 'IGST', 'CGST', 'SGST_UTGST', 'CESS']].loc[(df9['Report_Type'] == 'CS1')]).apply(pd.to_numeric, errors='coerce').values.flatten().tolist()

            if 'CS2' in list(df9['Report_Type']):
                b = (df9[['Row_count', 'Columns_count', 'GSTIN_count', 'Unique_Invoice_Count', 'Invoice_Value','Taxable_Value', 'IGST', 'CGST', 'SGST_UTGST', 'CESS']].loc[(df9['Report_Type'] == 'CS2')]).apply(pd.to_numeric, errors='coerce').values.flatten().tolist()

            if 'CS3' in list(df9['Report_Type']):
                c = (df9[['Row_count', 'Columns_count', 'GSTIN_count', 'Unique_Invoice_Count', 'Invoice_Value','Taxable_Value', 'IGST', 'CGST', 'SGST_UTGST', 'CESS']].loc[(df9['Report_Type'] == 'CS3')]).apply(pd.to_numeric, errors='coerce').values.flatten().tolist()

            if ('CS1' in list(df9['Report_Type'])) and ('CS2' in list(df9['Report_Type'])):
                df9.loc[len(df9)] = ['CS1-CS2'] + list(list(np.array(a) - np.array(b)))

            if ('CS1' in list(df9['Report_Type'])) and ('CS3' in list(df9['Report_Type'])):
                df9.loc[len(df9)] = ['CS1-CS3'] + list(list(np.array(a) - np.array(c)))

            df9.to_csv(r"C:\Users\Admin\Desktop\Before Action Status.csv")
            df9.loc[df9['Report_Type']=='CS1','Action_Status']='Raw Summary report'
            df9.loc[df9['Report_Type'] =='CS2','Action_Status'] ='Compute Summary report'
            df9.loc[df9['Report_Type'] =='CS3','Action_Status'] ='TG Summary report'

            report_write2 = df9.to_json(orient='records')
            print(report_write2, "report_write2")

            # df9 = pd.DataFrame(report_write,
            return Response({"status": "success", "response": report_write2},status=status.HTTP_201_CREATED)
        # else:
        #     return Response({"status": "success", "response": "something went wrong"}, status=status.HTTP_201_CREATED)
############################################################  Validation_Data   #########################################################################################################

class Validation_Data(APIView):
    def post(self,request):
        raw_id=request.data['FOR_RAW_ID']
        print(raw_id,"raw_id")
        rep_d = request.data['Report_Type']
        print(rep_d, "raw_id")
        connection = pymysql.connect(host='15.206.93.178', user="taxgenie", password="taxgenie*#8102*$", database="taxgenie_efilling")
        a = 'upload_sales_result_table' if raw_id['type']=='GSTR1-Sales' else 'upload_purchase_result_table'
        query = pd.read_sql("select DISTINCT  `RawFileRef_Id` from "+a+" where RawFileRef_Id='" + str(raw_id['RawFileRef_Id']) + "'", connection)
        if (len(query)==0) :
            return Response({"status": "failed", "response": "Push File In TG-Upload"}, status=status.HTTP_201_CREATED)
        else:
            query = pd.read_sql("select * from "+a+" where RawFileRef_Id='"+str(raw_id['RawFileRef_Id'])+"'",connection)
            if rep_d=='All':
                print("all")
                query = pd.read_sql("select * from "+a+" where RawFileRef_Id='"+str(raw_id['RawFileRef_Id'])+"'",connection)
            elif rep_d == 'Pass':
                print("pass")
                query = pd.read_sql("select * from "+a+" where RawFileRef_Id='" + str(raw_id['RawFileRef_Id']) + "' and Status='" + str('Success') + "'",connection)
            elif rep_d == 'Fail':
                print("fail")
                query = pd.read_sql("select * from "+a+" where RawFileRef_Id='" + str(raw_id['RawFileRef_Id']) + "' and Status='" + str('Fail') + "'",connection)
            connection.close()
            print(query['Status'], "queryqueryquery")
            print(query.columns,"columns")

            BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            print(BASE_DIR, 'BASE_DIR')
            time = str(datetime.now())
            FILE_N="Import_File_"+ time
            nn = FILE_N.replace(':', '_')
            nn2 = nn.replace('.', '_')
            path=BASE_DIR +"//Validation_Data//"+nn2+".xlsx"
            query.to_excel(path)
            FilePointer = open(path, "rb")
            response = HttpResponse(FilePointer,content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            filen = nn2+".xlsx"
            response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="utf-8")
            response['Cache-Control'] = 'no-cache'
            return response

############################################################   summary excel report and download   #########################################################################################################

class File_Id_Check(APIView):
    def post(self, request):
        raw_id=request.data['raw_id']
        print(raw_id,"raw_id")
        report_type = request.data['Report_Type']
        connection = pymysql.connect(host='15.206.93.178', user="taxgenie", password="taxgenie*#8102*$",database="taxgenie_efilling")
        a = 'upload_sales_result_table' if raw_id['type'] == 'GSTR1-Sales' else 'upload_purchase_result_table'
        query = pd.read_sql("select DISTINCT `RawFileRef_Id` from "+a+" where RawFileRef_Id='" + str(raw_id['RawFileRef_Id']) + "'",connection)
        if (len(query)==0) :
            return Response({"status": "failed", "response": "Push File In TG-Upload"}, status=status.HTTP_201_CREATED)
        else:
            query = pd.read_sql("select * from "+a+" where RawFileRef_Id='" + str(raw_id['RawFileRef_Id']) + "'",connection)
            print(query['Raw_Id_Timestamp'],"query")
            recent_date = query['Raw_Id_Timestamp'].max()
            query=query[query['Raw_Id_Timestamp'] == recent_date]
            print(query['Raw_Id_Timestamp'],"Raw_Id_Timestamp")
            connection.close()
            print(query,"database")
            print(raw_id['Report_Type'],"raw_id['Report_Type']")
            report=import_History_Summary(query,report_type)
            print(report,"report")
            imp_data=report.to_json(orient='records')
            print(imp_data)
            return Response({"status": "success", "response":imp_data}, status=status.HTTP_201_CREATED)


