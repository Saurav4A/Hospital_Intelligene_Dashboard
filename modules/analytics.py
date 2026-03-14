import pandas as pd

def calc_growth(current, previous):
    if previous == 0:
        return 0
    return round(((current - previous) / previous) * 100, 2)

def analyze_revenue(df):
    summary = df.groupby(['Unit', 'Department']).agg({
        'Amount': 'sum',
        'PatientCount': 'sum'
    }).reset_index()
    summary['ADR'] = summary['Amount'] / summary['PatientCount']
    return summary
