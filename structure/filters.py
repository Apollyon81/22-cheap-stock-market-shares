import pandas as pd
import numpy as np

def clean_numeric(value):
    """Converte valores BR/EN para float sem alterar texto original."""
    if pd.isna(value):
        return np.nan

    s = str(value).strip().replace('%', '')

    if s in ['', '-', 'N/A']:
        return np.nan

    # Formato BR 1.234,56
    if '.' in s and ',' in s:
        s = s.replace('.', '').replace(',', '.')
    # Formato BR 4,50
    elif ',' in s:
        s = s.replace(',', '.')

    try:
        return float(s)
    except:
        return np.nan


def apply_filters(df_raw):
    df = df_raw.copy()

    # Criar colunas auxiliares numéricas
    df['Liq_num'] = df['Liq.2meses'].apply(clean_numeric)
    df['MrgEbit_num'] = df['Mrg Ebit'].apply(clean_numeric)
    df['EVEBIT_num'] = df['EV/EBIT'].apply(clean_numeric)
    df['PL_num'] = df['P/L'].apply(clean_numeric)

    # ================= FILTROS =================

    df = df[df['Liq_num'] >= 1_000_000]     # liquidez mínima
    df = df[df['MrgEbit_num'] > 0]          # margem > 0
    df = df[df['EVEBIT_num'] > 0]           # EV/EBIT > 0
    df = df[df['PL_num'] > 0]               # P/L > 0

    # ================ ORDENAÇÃO =================
    df = df.sort_values(by='EVEBIT_num', ascending=True)

    # Lista final
    result = df["Papel"].tolist()

    # DEBUG
    print("QTD:", len(result))
    print(result)

    # LIMITA A 22 (se quiser)
    return result[:22]
