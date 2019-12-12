from .views import *
import pandas as pd
def Raw_summary_reports(df,dt):
    Column_Count = len(df.columns)
    df1 = pd.DataFrame()
    for user, sys in dt.items():
        print(user)
        print(sys)
        df1[sys] = df[user]

    ls_key = ['Row_Count', 'Column_Count', 'GSTIN_Count', 'Invoice_Number_Count', 'Total_Invoice_Value',
              'Total_Taxable_Value', 'Total_IGST_Value', 'Total_CGST_Value', 'Total_SGST_UTGST_Value', 'Total_CESS_Value']
    ls_value = []
    summary = dict(zip(ls_key, ls_value))

    Row_Count = len(df1)
    summary['Row_count'] = Row_Count

    summary['Columns_count'] = Column_Count

    summary['Report_Type'] ='CS1'
    if ('GSTIN' in list(df1.columns)):
        GSTIN_Count = df1['GSTIN'].nunique()
        summary['GSTIN_count'] = GSTIN_Count
    else:
        summary['GSTIN_count'] =''
    if ('Invoice No' in list(df1.columns)):
        Invoice_Number_Count = df1['Invoice No'].nunique()
        summary['Unique_Invoice_Count'] = Invoice_Number_Count
    else:
        summary['Unique_Invoice_Count'] =''
    if ('Invoice Value' in list(df1.columns)):
        Total_Invoice_Value =round(df1['Invoice Value'].astype(float).sum(),4)
        summary['Invoice_Value'] = Total_Invoice_Value
    else:
        summary['Invoice_Value'] =''
    if ('Taxable Value' in list(df1.columns)):
        Total_Taxable_Value =round(df1['Taxable Value'].astype(float).sum(),4)
        summary['Taxable_Value'] = Total_Taxable_Value
    else:
        summary['Taxable_Value'] =''

    if ('Igst Amount' in list(df1.columns)):
        Total_IGST_Value =round(df1['Igst Amount'].astype(float).sum(),4)
        summary['IGST'] = Total_IGST_Value
    else:
        summary['IGST'] =''
    if ('Cgst Amount' in list(df1.columns)):
        Total_CGST_Value =round(df1['Cgst Amount'].astype(float).sum(),4)
        summary['CGST'] = Total_CGST_Value
    else:
        summary['CGST'] =''
    if ('Sgst Amount' in list(df1.columns)):
        Total_SGST_UTGST_Value =round(df1['Sgst Amount'].astype(float).sum(),4)
        summary['SGST_UTGST'] = Total_SGST_UTGST_Value
    else:
        summary['SGST_UTGST'] = ''
    if ('Cess Amount' in list(df1.columns)):
        Total_CESS_Value =round(df1['Cess Amount'].astype(float).sum(),4)
        summary['CESS'] = Total_CESS_Value
    else:
        summary['CESS'] =''
    return  summary


def Compute_summary_reports(df,dt):
    df1 = pd.DataFrame()
    Column_Count = len(df.columns)
    for user, sys in dt.items():
        print(user)
        print(sys)
        df1[sys] = df[user]

    ls_key = ['Row_Count', 'Column_Count', 'GSTIN_Count', 'Invoice_Number_Count', 'Total_Invoice_Value',
              'Total_Taxable_Value', 'Total_IGST_Value', 'Total_CGST_Value', 'Total_SGST_UTGST_Value', 'Total_CESS_Value']
    ls_value = []
    summary = dict(zip(ls_key, ls_value))

    Row_Count = len(df1)
    summary['Row_count'] = Row_Count
    summary['Columns_count'] = Column_Count

    summary['Report_Type'] ='CS2'
    if ('GSTIN' in list(df1.columns)):
        GSTIN_Count = df1['GSTIN'].nunique()
        summary['GSTIN_count'] = GSTIN_Count
    else:
        summary['GSTIN_count'] =''
    if ('Invoice No' in list(df1.columns)):
        Invoice_Number_Count = df1['Invoice No'].nunique()
        summary['Unique_Invoice_Count'] = Invoice_Number_Count
    else:
        summary['Unique_Invoice_Count'] =''
    if ('Invoice Value' in list(df1.columns)):
        Total_Invoice_Value =round(df1['Invoice Value'].astype(float).sum(),4)
        summary['Invoice_Value'] = Total_Invoice_Value
    else:
        summary['Invoice_Value'] =''
    if ('Taxable Value' in list(df1.columns)):
        Total_Taxable_Value =round(df1['Taxable Value'].astype(float).sum(),4)
        summary['Taxable_Value'] = Total_Taxable_Value
    else:
        summary['Taxable_Value'] =''

    if ('Igst Amount' in list(df1.columns)):
        Total_IGST_Value =round(df1['Igst Amount'].astype(float).sum(),4)
        summary['IGST'] = Total_IGST_Value
    else:
        summary['IGST'] =''
    if ('Cgst Amount' in list(df1.columns)):
        Total_CGST_Value =round(df1['Cgst Amount'].astype(float).sum(),4)
        summary['CGST'] = Total_CGST_Value
    else:
        summary['CGST'] =''
    if ('Sgst Amount' in list(df1.columns)):
        Total_SGST_UTGST_Value =round(df1['Sgst Amount'].astype(float).sum(),4)
        summary['SGST_UTGST'] = Total_SGST_UTGST_Value
    else:
        summary['SGST_UTGST'] = ''
    if ('Cess Amount' in list(df1.columns)):
        Total_CESS_Value =round(df1['Cess Amount'].astype(float).sum(),4)
        summary['CESS'] = Total_CESS_Value
    else:
        summary['CESS'] =''
    return  summary

def TG_GOV_summary_reports(df,dt,Type):
    Column_Count = len(df.columns)
    df1=pd.DataFrame()
    for user, sys in dt.items():
        print(user)
        print(sys)
        df1[sys] = df[user]

    ls_key = ['Row_Count', 'Column_Count', 'GSTIN_Count', 'Invoice_Number_Count', 'Total_Invoice_Value',
              'Total_Taxable_Value', 'Total_IGST_Value', 'Total_CGST_Value', 'Total_SGST_UTGST_Value', 'Total_CESS_Value']
    ls_value = []
    summary = dict(zip(ls_key, ls_value))

    Row_Count = len(df1)
    summary['Row_count'] = Row_Count

    summary['Columns_count'] = Column_Count

    summary['Report_Type'] ='CS3'
    if( Type == 'GSTR1-Sales'):
        if ('GSTIN' in list(df1.columns)):
            GSTIN_Count = df1['sellerGSTIN'].nunique()
            summary['GSTIN_count'] =GSTIN_Count
        else:
            summary['GSTIN_count'] =''
    elif(Type == 'GSTR2-Purchase'):
        if ('GSTIN' in list(df1.columns)):
            GSTIN_Count = df1['buyerGSTIN'].nunique()
            summary['GSTIN_count'] =GSTIN_Count
        else:
            summary['GSTIN_count'] =''

    if ('Igst Amount' in list(df1.columns)):
        Total_IGST_Value = round(df1['Igst Amount'].astype(str).str.replace(",", '').astype(float).sum(), 4)
        summary['IGST'] = Total_IGST_Value
    else:
        summary['IGST'] = ''
    if ('Cgst Amount' in list(df1.columns)):
        Total_CGST_Value = round(df1['Cgst Amount'].astype(str).str.replace(",", '').astype(float).sum(), 4)
        summary['CGST'] = Total_CGST_Value
    else:
        summary['CGST'] = ''
    if ('Sgst Amount' in list(df1.columns)):
        Total_SGST_UTGST_Value = round(df1['Sgst Amount'].astype(str).str.replace(",", '').astype(float).sum(), 4)
        summary['SGST_UTGST'] = Total_SGST_UTGST_Value
    else:
        summary['SGST_UTGST'] = ''
    if ('Cess Amount' in list(df1.columns)):
        Total_CESS_Value = round(df1['Cess Amount'].astype(float).sum(), 4)
        summary['CESS'] = Total_CESS_Value
    else:
        summary['CESS'] = ''
    return summary
