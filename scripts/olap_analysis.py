import pandas as pd
import mysql.connector
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import configurations
from config import DW_CONFIG

class OLAPAnalyzer:
    def __init__(self):
        self.conn = mysql.connector.connect(**DW_CONFIG)
        self.cursor = self.conn.cursor(dictionary=True)
        
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()

    def get_sales_data(self):
        """Carrega dados de vendas com todas as dimensões necessárias"""
        query = """
        SELECT 
            t.ano,
            t.mes,
            t.trimestre,
            l.cidade,
            l.estado,
            c.nome_categoria,
            p.nome_produto,
            SUM(f.quantidade) as quantidade_total,
            SUM(f.valor_total) as valor_total_vendas,
            SUM(f.desconto_total) as desconto_total
        FROM fato_vendas f
        JOIN dim_tempo t ON f.id_tempo = t.id_tempo
        JOIN dim_loja l ON f.id_loja = l.id_loja
        JOIN dim_produto p ON f.id_produto = p.id_produto
        JOIN dim_categoria c ON p.categoria = c.nome_categoria
        GROUP BY t.ano, t.mes, t.trimestre, l.cidade, l.estado, c.nome_categoria, p.nome_produto
        """
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall())
        # Força as colunas numéricas para float
        for col in ['quantidade_total', 'valor_total_vendas', 'desconto_total']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def analyze_sales_by_time_and_category(self, df):
        """Análise de vendas por tempo e categoria (melhorada)"""
        # Criar coluna ano-trimestre
        df['ano_trimestre'] = df['ano'].astype(str) + '-T' + df['trimestre'].astype(str)
        # Agrupa por ano-trimestre e categoria
        time_category_analysis = df.groupby(['ano_trimestre', 'nome_categoria']).agg({
            'valor_total_vendas': 'sum',
            'quantidade_total': 'sum'
        }).reset_index()
        print("Resumo vendas por ano-trimestre e categoria:")
        print(time_category_analysis.head(10))
        # Gráfico de barras empilhadas
        fig = px.bar(
            time_category_analysis,
            x='ano_trimestre',
            y='valor_total_vendas',
            color='nome_categoria',
            title='Vendas por Trimestre e Categoria',
            labels={
                'ano_trimestre': 'Ano-Trimestre',
                'valor_total_vendas': 'Valor Total de Vendas',
                'nome_categoria': 'Categoria'
            },
            barmode='stack'
        )
        fig.update_xaxes(type='category')
        return fig

    def analyze_sales_by_location_and_category(self, df):
        """Análise de vendas por localização e categoria"""
        # Agrupa por estado e categoria
        location_category_analysis = df.groupby(['estado', 'nome_categoria']).agg({
            'valor_total_vendas': 'sum',
            'quantidade_total': 'sum'
        }).reset_index()
        
        # Cria visualização
        fig = px.treemap(
            location_category_analysis,
            path=['estado', 'nome_categoria'],
            values='valor_total_vendas',
            title='Vendas por Estado e Categoria',
            color='valor_total_vendas',
            color_continuous_scale='Viridis'
        )
        return fig

    def analyze_sales_trends(self, df):
        """Análise de tendências de vendas ao longo do tempo (melhorada)"""
        # Criar coluna ano-mês
        df['ano_mes'] = df['ano'].astype(str) + '-' + df['mes'].astype(str).str.zfill(2)
        # Agrupa por ano-mês
        monthly_trends = df.groupby(['ano_mes']).agg({
            'valor_total_vendas': 'sum',
            'quantidade_total': 'sum'
        }).reset_index()
        print("Resumo vendas por ano-mês:")
        print(monthly_trends.head(10))
        # Gráfico de linha com pontos
        fig = px.line(
            monthly_trends,
            x='ano_mes',
            y='valor_total_vendas',
            markers=True,
            title='Tendência de Vendas Mensais',
            labels={
                'ano_mes': 'Ano-Mês',
                'valor_total_vendas': 'Valor Total de Vendas'
            }
        )
        fig.update_xaxes(type='category')
        return fig

    def generate_summary_report(self, df):
        """Gera um relatório resumido com métricas principais"""
        summary = {
            'total_vendas': df['valor_total_vendas'].sum(),
            'total_quantidade': df['quantidade_total'].sum(),
            'media_vendas_por_categoria': df.groupby('nome_categoria')['valor_total_vendas'].mean(),
            'top_categorias': df.groupby('nome_categoria')['valor_total_vendas'].sum().nlargest(5),
            'top_estados': df.groupby('estado')['valor_total_vendas'].sum().nlargest(5)
        }
        return summary

def main():
    analyzer = OLAPAnalyzer()
    
    # Carrega dados
    print("Carregando dados...")
    sales_data = analyzer.get_sales_data()
    
    # Gera análises
    print("\nGerando análises...")
    
    # Análise por tempo e categoria
    time_category_fig = analyzer.analyze_sales_by_time_and_category(sales_data)
    time_category_fig.write_html("analise_tempo_categoria.html")
    
    # Análise por localização e categoria
    location_category_fig = analyzer.analyze_sales_by_location_and_category(sales_data)
    location_category_fig.write_html("analise_localizacao_categoria.html")
    
    # Análise de tendências
    trends_fig = analyzer.analyze_sales_trends(sales_data)
    trends_fig.write_html("analise_tendencias.html")
    
    # Gera relatório resumido
    summary = analyzer.generate_summary_report(sales_data)
    
    print("\nRelatório Resumido:")
    print(f"Total de Vendas: R$ {summary['total_vendas']:,.2f}")
    print(f"Total de Quantidade Vendida: {summary['total_quantidade']:,.0f}")
    print("\nTop 5 Categorias por Vendas:")
    print(summary['top_categorias'])
    print("\nTop 5 Estados por Vendas:")
    print(summary['top_estados'])
    
    print("\nAnálises salvas em arquivos HTML:")
    print("- analise_tempo_categoria.html")
    print("- analise_localizacao_categoria.html")
    print("- analise_tendencias.html")

if __name__ == '__main__':
    main() 