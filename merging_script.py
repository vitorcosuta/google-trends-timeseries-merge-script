from argparse import ArgumentParser
from pathlib import Path
import pandas as pd
import re

PADRAO = r"^(.*?)\s((?:[A-ZÀ-ÖØ-öø-ÿ]{2,}|\b[A-ZÀ-ÖØ-öø-ÿ][a-zà-öø-ÿ]*(?:\s[A-ZÀ-ÖØ-öø-ÿ][a-zà-öø-ÿ]*)*)?)(?:\s\+\s(.*?)\s((?:[A-ZÀ-ÖØ-öø-ÿ]{2,}|\b[A-ZÀ-ÖØ-öø-ÿ][a-zà-öø-ÿ]*(?:\s[A-ZÀ-ÖØ-öø-ÿ][a-zà-öø-ÿ]*)*)?))?$"

def fill_empty_columns(df: pd.DataFrame) -> None:
    """Preenche as colunas vazias de um dataframe com valores pré-estipulados"""

    years = list(range(2004, 2025))
    months = list(range(1, 13))

    dates = [f"{year}-{month:02}" for year in years for month in months]

    date_col = df.columns[0]
    gti_col = df.columns[1]

    df[date_col] = dates
    df[gti_col] = [0] * len(df)

def generate_proper_format(file: str) -> pd.DataFrame:
    """Gera o dataframe do arquivo no formato adequado, removendo linhas de categoria e sufixos das colunas"""

    df = pd.read_csv(file, sep=',', skiprows=1)
    remove_cols_suffixes(df)
    return df

def remove_cols_suffixes(df: pd.DataFrame) -> None:
    """Remove o sufixo indesejado das colunas do dataframe"""

    df.columns = [col.removesuffix(': (Brasil)') for col in df.columns]

def merge_files(p: Path) -> pd.DataFrame:
    """Une os arquivos de uma pasta por meio de uma junção natural"""

    files = [file.as_posix() for file in p.iterdir() if (file.is_file() and file.suffix == ".csv")]

    # Lendo o primeiro arquivo
    merged_data = generate_proper_format(files[0])
    
    if merged_data[merged_data.columns[0]].empty:
        fill_empty_columns(merged_data)

    for file in files[1:]:

        df = generate_proper_format(file)

        if df[df.columns[0]].empty:
            fill_empty_columns(df)

        merged_data = merged_data.merge(df, how='inner', on='Mês')

    return merged_data

def add_country_col(df: pd.DataFrame) -> None:
    """Adiciona uma coluna correspondente ao nome do país"""

    pais = ""

    # Aplicando regex para extrair o país da primeira query
    try:
        pais = re.search(PADRAO, df.columns[1], re.UNICODE).group(2)
    except AttributeError as e:
        print(f"Erro ao pesquisar a coluna {df.columns[1]}: {e}")

    # Adicionando a coluna do país (bilateralização dos dados)
    df['País'] = [pais] * len(df)

def rename_cols(df: pd.DataFrame, country_on: bool) -> None:
    """Altera o nome das colunas de queries para somente o termo correspondente na regex"""
    
    new_cols = []

    if(country_on):

        for col in df.columns[1:-1]:

            try:
                new_col = re.search(PADRAO, col, re.UNICODE).group(1)
                new_cols.append(new_col)
            except AttributeError as e:
                print(f"Erro ao pesquisar a coluna {col}: {e}")
    
        df.columns = ['Mês'] + new_cols + ['País']
    else:
        for col in df.columns[1:]:

            try:
                new_col = re.search(PADRAO, col, re.UNICODE).group(1)
                new_cols.append(new_col)
            except AttributeError as e:
                print(f"Erro ao pesquisar a coluna {col}: {e}")

        print(new_cols)
    
        #df.columns = ['Mês'] + new_cols

def main():
    parser = ArgumentParser()

    parser.add_argument("src_dir", help="Diretório com os arquivos a serem juntados")
    parser.add_argument("filename", help="Nome do arquivo final a ser gerado")

    args = parser.parse_args()

    src_dir = args.src_dir
    filename = args.filename

    src_path = Path(src_dir)

    subfolders = [folder for folder in src_path.iterdir() if folder.is_dir()]

    # Procurando por subpastas
    if not subfolders:
        data = merge_files(src_path)
        rename_cols(data, False)
        data.to_csv(filename, index=False)
    else:

        dfs = []

        for folder in subfolders:
            data = merge_files(folder)
            add_country_col(data)
            rename_cols(data, True)
            dfs.append(data)

        result = pd.concat(dfs, ignore_index=True)
        result.to_csv(filename, index=False)

if __name__ == "__main__":
    main()