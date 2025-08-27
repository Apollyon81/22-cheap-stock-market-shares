import pandas as pd
import numpy as np

def clean_numeric(value):
    """Limpa e converte valores numéricos"""
    if pd.isna(value) or value in ['-', 'N/A']:
        return np.nan
    if isinstance(value, str):
        # Remove % e converte pontos e vírgulas
        value = value.replace('%', '').replace('.', '').replace(',', '.')
    return pd.to_numeric(value, errors='coerce')

def apply_filters(df):
    """
    Aplica filtros de qualidade nas ações seguindo critérios específicos:
    1. Liquidez mínima de R$ 1 milhão
    2. Margem EBIT positiva
    3. EV/EBIT positivo e razoável
    4. EBIT e LPA positivos
    5. Verifica consistência dos dados
    """
    # Cria cópia para não modificar o original
    df = df.copy()
    
    # Limpa e converte valores numéricos
    numeric_columns = ['Liq.2meses', 'Mrg Ebit', 'EV/EBIT', 'P/L', 'ROE', 'ROIC']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_numeric)
    
    # 1. Filtro de Liquidez (Volume médio diário > R$ 1 milhão)
    df = df[df['Liq.2meses'] >= 1000000]
    
    # 2. Filtro de Margem EBIT (positiva e realista)
    df = df[df['Mrg Ebit'] > 0]
    df = df[df['Mrg Ebit'] <= 100]  # Remove margens irrealistas
    
    # 3. Filtro de EV/EBIT (positivo e razoável)
    df = df[df['EV/EBIT'] > 0]
    df = df[df['EV/EBIT'] <= 50]  # Remove múltiplos muito altos
    
    # 4. Filtro de P/L (positivo e razoável)
    df = df[df['P/L'] > 0]
    df = df[df['P/L'] <= 50]  # Remove P/L muito altos
    
    # 5. Filtros adicionais de qualidade
    if 'ROE' in df.columns:
        df = df[df['ROE'] > 0]  # ROE positivo
    if 'ROIC' in df.columns:
        df = df[df['ROIC'] > 0]  # ROIC positivo
    
    # Remove duplicatas mantendo a primeira ocorrência
    df = df.drop_duplicates(subset='Papel', keep='first')
    
    # Ordena por EV/EBIT (crescente) e Margem EBIT (decrescente)
    df = df.sort_values(by=['EV/EBIT', 'Mrg Ebit'], ascending=[True, False])
    
    # Seleciona as top 22 ações
    df_filtered = df.head(22)
    
    # Seleciona apenas as colunas relevantes para exibição
    colunas_exibicao = ['Papel', 'Liq.2meses', 'Mrg Ebit', 'EV/EBIT', 'P/L']
    df_filtered = df_filtered[colunas_exibicao]
    
    return df_filtered

def format_output(df):
    """Formata os valores para exibição"""
    df = df.copy()
    
    # Formata valores monetários
    df['Liq.2meses'] = df['Liq.2meses'].apply(lambda x: f'R$ {x:,.2f}'.replace(',', '_').replace('.', ',').replace('_', '.'))
    
    # Formata percentuais
    df['Mrg Ebit'] = df['Mrg Ebit'].apply(lambda x: f'{x:.2f}%'.replace('.', ','))
    
    # Formata múltiplos
    df['EV/EBIT'] = df['EV/EBIT'].apply(lambda x: f'{x:.2f}'.replace('.', ','))
    df['P/L'] = df['P/L'].apply(lambda x: f'{x:.2f}'.replace('.', ','))
    
    return df
