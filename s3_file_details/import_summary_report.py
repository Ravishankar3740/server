import pandas as pd
import numpy as np
from .views import  *

def import_History_Summary(df, Type):
    if (Type == 'All'):
        Pass = df.loc[df['Status'] == 'Success']
        Fail = df.loc[df['Status'] == 'Fail']

        ls_key_All = ['Row_Count', 'Column_Count', 'GSTIN_Count', 'Invoice_Number_Count', 'Total_Invoice_Value',
                      'Total_Taxable_Value', 'Total_IGST_Value', 'Total_CGST_Value', 'Total_SGST_UTGST_Value',
                      'Total_CESS_Value', 'Status']
        ls_value_Pass = []
        Pass_summary = dict(zip(ls_key_All, ls_value_Pass))
        Row_Count = str(len(Pass))
        Pass_summary['Row_Count'] = Row_Count
        Column_Count = len(Pass.columns)
        Pass_summary['Column_Count'] = Column_Count
        if ('Buyer Gstin' in list(Pass.columns)):
            GSTIN_Count = Pass['Buyer Gstin'].nunique()
            Pass_summary['GSTIN_Count'] = GSTIN_Count
        if ('Invoice No' in list(Pass.columns)):
            Invoice_Number_Count = Pass['Invoice No'].nunique()
            Pass_summary['Invoice_Number_Count'] = Invoice_Number_Count

        if ('Invoice Value' in list(Pass.columns)):
            Total_Invoice_Value =round(Pass['Invoice Value'].astype(float).sum(),4)
            Pass_summary['Total_Invoice_Value'] = Total_Invoice_Value
        if ('Taxable Value' in list(Pass.columns)):
            Total_Taxable_Value =round(Pass['Taxable Value'].astype(float).sum(),4)
            Pass_summary['Total_Taxable_Value'] = Total_Taxable_Value
        if ('Igst Amount' in list(Pass.columns)):
            Total_IGST_Value =round(Pass['Igst Amount'].astype(float).sum(),4)
            Pass_summary['Total_IGST_Value'] = Total_IGST_Value
        if ('Cgst Amount' in list(Pass.columns)):
            Total_CGST_Value =round(Pass['Cgst Amount'].astype(float).sum(),4)
            Pass_summary['Total_CGST_Value'] = Total_CGST_Value
        if ('Sgst Amount' in list(Pass.columns)):
            Total_SGST_UTGST_Value =round(Pass['Sgst Amount'].astype(float).sum(),4)
            Pass_summary['Total_SGST_UTGST_Value'] = Total_SGST_UTGST_Value
        if ('Cess Amount' in list(Pass.columns)):
            Total_CESS_Value =round(Pass['Cess Amount'].astype(float).sum(),4)
            Pass_summary['Total_CESS_Value'] = Total_CESS_Value
        if ('Status' in list(Pass.columns)):
            #     Status = Pass['Status'].unique()[0]
            Pass_summary['Status'] = "Pass"
        remaining = list(set(ls_key_All) - set(list(Pass_summary.keys())))
        Pass_summary.update(dict.fromkeys(remaining))
        ls_value_fail = []
        Fail_summary = dict(zip(ls_key_All, ls_value_fail))
        Row_Count = str(len(Fail))
        Fail_summary['Row_Count'] = Row_Count
        Column_Count = len(Fail.columns)
        Fail_summary['Column_Count'] = Column_Count
        if ('Buyer Gstin' in list(Fail.columns)):
            GSTIN_Count = Fail['Buyer Gstin'].nunique()
            Fail_summary['GSTIN_Count'] = GSTIN_Count
        if ('Invoice No' in list(Fail.columns)):
            Invoice_Number_Count = Fail['Invoice No'].nunique()
            Fail_summary['Invoice_Number_Count'] = Invoice_Number_Count
        if ('Invoice Value' in list(Fail.columns)):
            Total_Invoice_Value =round(Fail['Invoice Value'].astype(float).sum(),4)
            Fail_summary['Total_Invoice_Value'] = Total_Invoice_Value
        if ('Taxable Value' in list(Fail.columns)):
            Total_Taxable_Value =round(Fail['Taxable Value'].astype(float).sum(),4)
            Fail_summary['Total_Taxable_Value'] = Total_Taxable_Value
        if ('Igst Amount' in list(Fail.columns)):
            Total_IGST_Value =round(Fail['Igst Amount'].astype(str).str.replace(",",'').astype(float).sum(),4)
            Fail_summary['Total_IGST_Value'] = Total_IGST_Value
        if ('Cgst Amount' in list(Fail.columns)):
            Total_CGST_Value =round(Fail['Cgst Amount'].astype(str).str.replace(",",'').astype(float).sum(),4)
            Fail_summary['Total_CGST_Value'] = Total_CGST_Value
        if ('Sgst Amount' in list(Fail.columns)):
            Total_SGST_UTGST_Value =round(Fail['Sgst Amount'].astype(str).str.replace(",",'').astype(float).sum(),4)
            Fail_summary['Total_SGST_UTGST_Value'] = Total_SGST_UTGST_Value
        if ('Cess Amount' in list(Fail.columns)):
            Total_CESS_Value =round(Fail['Cess Amount'].astype(float).sum(),4)
            Fail_summary['Total_CESS_Value'] = Total_CESS_Value
        if ('Status' in list(Fail.columns)):
            #     Status = Fail['Status'].unique()[0]
            Fail_summary['Status'] = "Fail"
        remaining = list(set(ls_key_All) - set(list(Fail_summary.keys())))
        Fail_summary.update(dict.fromkeys(remaining))
        df1 = pd.DataFrame([Pass_summary, Fail_summary])
        df1 = df1.append(df1[['GSTIN_Count', 'Invoice_Number_Count', 'Row_Count', 'Total_CESS_Value',
                              'Total_CGST_Value', 'Total_IGST_Value', 'Total_Invoice_Value', 'Total_SGST_UTGST_Value',
                              'Total_Taxable_Value']].astype(float).sum().rename('Total')).assign(
            Total=lambda d: d.sum(1)).reset_index(drop=True)
        df1.loc[df1['Status'].isna(), 'Status'] = 'Total'
        df1.loc[df1['Column_Count'].isna(), 'Column_Count'] = Column_Count
        return df1

    elif (Type == "Pass"):
        Pass = df.loc[df['Status'] == 'Success']
        ls_key_All = ['Row_Count', 'Column_Count', 'GSTIN_Count', 'Invoice_Number_Count', 'Total_Invoice_Value',
                      'Total_Taxable_Value', 'Total_IGST_Value', 'Total_CGST_Value', 'Total_SGST_UTGST_Value',
                      'Total_CESS_Value', 'Status']
        ls_value_Pass = []
        Pass_summary = dict(zip(ls_key_All, ls_value_Pass))
        Row_Count = str(len(Pass))
        Pass_summary['Row_Count'] = Row_Count
        Column_Count = len(Pass.columns)
        Pass_summary['Column_Count'] = Column_Count
        if ('Buyer Gstin' in list(Pass.columns)):
            GSTIN_Count = Pass['Buyer Gstin'].nunique()
            Pass_summary['GSTIN_Count'] = GSTIN_Count
        if ('Invoice No' in list(Pass.columns)):
            Invoice_Number_Count = Pass['Invoice No'].nunique()
            Pass_summary['Invoice_Number_Count'] = Invoice_Number_Count
        if ('Invoice Value' in list(Pass.columns)):
            Total_Invoice_Value =round(Pass['Invoice Value'].astype(float).sum(),4)
            Pass_summary['Total_Invoice_Value'] = Total_Invoice_Value
        if ('Taxable Value' in list(Pass.columns)):
            Total_Taxable_Value =round(Pass['Taxable Value'].astype(float).sum(),4)
            Pass_summary['Total_Taxable_Value'] = Total_Taxable_Value
        if ('Igst Amount' in list(Pass.columns)):
            Total_IGST_Value =round(Pass['Igst Amount'].astype(float).sum(),4)
            Pass_summary['Total_IGST_Value'] = Total_IGST_Value
        if ('Cgst Amount' in list(Pass.columns)):
            Total_CGST_Value =round(Pass['Cgst Amount'].astype(float).sum(),4)
            Pass_summary['Total_CGST_Value'] = Total_CGST_Value
        if ('Sgst Amount' in list(Pass.columns)):
            Total_SGST_UTGST_Value =round(Pass['Sgst Amount'].astype(float).sum(),4)
            Pass_summary['Total_SGST_UTGST_Value'] = Total_SGST_UTGST_Value
        if ('Cess Amount' in list(Pass.columns)):
            Total_CESS_Value =round(Pass['Cess Amount'].astype(float).sum(),4)
            Pass_summary['Total_CESS_Value'] = Total_CESS_Value
        if ('Status' in list(Pass.columns)):
            #     Status = Pass['Status'].unique()[0]
            Pass_summary['Status'] = "Pass"
        remaining = list(set(ls_key_All) - set(list(Pass_summary.keys())))
        Pass_summary.update(dict.fromkeys(remaining))
        df1 = pd.DataFrame([Pass_summary])
        return df1

    elif (Type == 'Fail'):
        Fail = df.loc[df['Status'] == 'Fail']
        ls_key_All = ['Row_Count', 'Column_Count', 'GSTIN_Count', 'Invoice_Number_Count', 'Total_Invoice_Value',
                      'Total_Taxable_Value', 'Total_IGST_Value', 'Total_CGST_Value', 'Total_SGST_UTGST_Value',
                      'Total_CESS_Value', 'Status']
        ls_value_fail = []
        Fail_summary = dict(zip(ls_key_All, ls_value_fail))
        Row_Count = str(len(Fail))
        Fail_summary['Row_Count'] = Row_Count
        Column_Count = len(Fail.columns)
        Fail_summary['Column_Count'] = Column_Count
        if ('Buyer Gstin' in list(Fail.columns)):
            GSTIN_Count = Fail['Buyer Gstin'].nunique()
            Fail_summary['GSTIN_Count'] = GSTIN_Count
        if ('Invoice No' in list(Fail.columns)):
            Invoice_Number_Count = Fail['Invoice No'].nunique()
            Fail_summary['Invoice_Number_Count'] = Invoice_Number_Count
        if ('Invoice Value' in list(Fail.columns)):
            Total_Invoice_Value =round(Fail['Invoice Value'].astype(float).sum(),4)
            Fail_summary['Total_Invoice_Value'] = Total_Invoice_Value
        if ('Taxable Value' in list(Fail.columns)):
            Total_Taxable_Value =round(Fail['Taxable Value'].astype(float).sum(),4)
            Fail_summary['Total_Taxable_Value'] = Total_Taxable_Value
        if ('Igst Amount' in list(Fail.columns)):
            Total_IGST_Value =round(Fail['Igst Amount'].astype(float).sum(),4)
            Fail_summary['Total_IGST_Value'] = Total_IGST_Value
        if ('Cgst Amount' in list(Fail.columns)):
            Total_CGST_Value =round(Fail['Cgst Amount'].astype(float).sum(),4)
            Fail_summary['Total_CGST_Value'] = Total_CGST_Value
        if ('Sgst Amount' in list(Fail.columns)):
            Total_SGST_UTGST_Value = round(Fail['Sgst Amount'].astype(float).sum(),4)
            Fail_summary['Total_SGST_UTGST_Value'] = Total_SGST_UTGST_Value
        if ('Cess Amount' in list(Fail.columns)):
            Total_CESS_Value =round(Fail['Cess Amount'].astype(float).sum(),4)
            Fail_summary['Total_CESS_Value'] = Total_CESS_Value
        if ('Status' in list(Fail.columns)):
            #     Status = Fail['Status'].unique()[0]
            Fail_summary['Status'] = "Fail"
        remaining = list(set(ls_key_All) - set(list(Fail_summary.keys())))
        Fail_summary.update(dict.fromkeys(remaining))
        df1 = pd.DataFrame([Fail_summary])
        return df1