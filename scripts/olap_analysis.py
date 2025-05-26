import pandas as pd
import mysql.connector
import plotly.graph_objects as go
from datetime import datetime
import os

class OLAPAnalyzer:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host="mysql_db",
            user=os.getenv('MYSQL_USER', 'user'),
            password=os.getenv('MYSQL_PASSWORD', 'userpassword'),
            database="DW_Varejo"
        )
        self.cursor = self.conn.cursor(dictionary=True)

    def __del__(self):
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

    def analyze_by_time_and_category(self):
        """Análise de vendas por tempo e categoria"""
        query = """
        SELECT 
            t.ano,
            t.mes,
            t.trimestre,
            c.nome_categoria,
            SUM(f.valor_total) as total_valor
        FROM fato_vendas f
        JOIN dim_tempo t ON f.id_tempo = t.id_tempo
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        GROUP BY t.ano, t.mes, t.trimestre, c.nome_categoria
        ORDER BY t.ano, t.mes, total_valor DESC
        """
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall())
        
        if len(df) > 0:
            # Criar coluna ano-trimestre
            df['periodo'] = df['ano'].astype(str) + '-T' + df['trimestre'].astype(str)
            
            # Criar gráfico de barras
            fig = go.Figure()
            
            for categoria in df['nome_categoria'].unique():
                df_cat = df[df['nome_categoria'] == categoria]
                fig.add_trace(go.Bar(
                    name=categoria,
                    x=df_cat['periodo'],
                    y=df_cat['total_valor']
                ))
            
            fig.update_layout(
                title='Vendas por Período e Categoria',
                xaxis_title='Ano-Trimestre',
                yaxis_title='Valor Total de Vendas (R$)',
                barmode='stack'
            )
            
            return fig
        return None

    def analyze_trends(self):
        """Análise de tendências de vendas"""
        query = """
        SELECT 
            t.ano,
            t.mes,
            SUM(CAST(f.valor_total AS DECIMAL(10,2))) as total_valor,
            COUNT(*) as num_vendas
        FROM fato_vendas f
        JOIN dim_tempo t ON f.id_tempo = t.id_tempo
        GROUP BY t.ano, t.mes
        ORDER BY t.ano, t.mes
        """
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall())
        
        if len(df) > 0:
            # Criar datas manualmente
            df['data'] = df.apply(lambda x: datetime(year=int(x['ano']), month=int(x['mes']), day=1), axis=1)
            
            # Converter para tipos numéricos
            df['total_valor'] = pd.to_numeric(df['total_valor'], errors='coerce')
            df['num_vendas'] = pd.to_numeric(df['num_vendas'], errors='coerce')
            
            # Criar gráfico de linha
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df['data'],
                y=df['total_valor'],
                name='Valor Total',
                mode='lines+markers'
            ))
            
            fig.add_trace(go.Scatter(
                x=df['data'],
                y=df['num_vendas'],
                name='Número de Vendas',
                mode='lines+markers',
                yaxis='y2'
            ))
            
            fig.update_layout(
                title='Tendências de Vendas',
                xaxis_title='Período',
                yaxis_title='Valor Total (R$)',
                yaxis2=dict(
                    title='Número de Vendas',
                    overlaying='y',
                    side='right'
                ),
                height=600,
                showlegend=True
            )
            
            return fig
        return None

    def analyze_by_category(self):
        query = """
        SELECT 
            c.nome_categoria,
            COUNT(*) as num_vendas,
            SUM(f.quantidade) as total_quantidade,
            SUM(f.valor_total) as total_valor,
            SUM(f.desconto_total) as total_desconto,
            AVG(f.valor_total) as media_valor,
            COUNT(DISTINCT f.id_cliente) as num_clientes
        FROM fato_vendas f
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        GROUP BY c.nome_categoria
        ORDER BY total_valor DESC
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def analyze_by_location(self):
        """Análise de vendas por localização"""
        # Análise por estado
        query_estado = """
        SELECT 
            l.estado,
            COUNT(*) as num_vendas,
            COUNT(DISTINCT f.id_cliente) as num_clientes,
            COUNT(DISTINCT l.id_loja) as num_lojas,
            SUM(CAST(f.quantidade AS DECIMAL(10,2))) as total_quantidade,
            SUM(CAST(f.valor_total AS DECIMAL(10,2))) as total_valor,
            AVG(CAST(f.valor_total AS DECIMAL(10,2))) as ticket_medio
        FROM fato_vendas f
        JOIN dim_loja l ON f.id_loja = l.id_loja
        GROUP BY l.estado
        ORDER BY total_valor DESC
        """
        self.cursor.execute(query_estado)
        resultados_estado = self.cursor.fetchall()

        # Análise por cidade
        query_cidade = """
        SELECT 
            l.cidade,
            l.estado,
            COUNT(*) as num_vendas,
            COUNT(DISTINCT f.id_cliente) as num_clientes,
            SUM(CAST(f.valor_total AS DECIMAL(10,2))) as total_valor
        FROM fato_vendas f
        JOIN dim_loja l ON f.id_loja = l.id_loja
        GROUP BY l.cidade, l.estado
        ORDER BY total_valor DESC
        LIMIT 10
        """
        self.cursor.execute(query_cidade)
        resultados_cidade = self.cursor.fetchall()

        # Criar gráfico de barras por estado
        df_estado = pd.DataFrame(resultados_estado)
        if len(df_estado) > 0:
            # Converter para tipos numéricos
            numeric_columns = ['total_valor', 'num_vendas', 'num_clientes', 'num_lojas', 'total_quantidade', 'ticket_medio']
            for col in numeric_columns:
                df_estado[col] = pd.to_numeric(df_estado[col], errors='coerce')
            
            # Criar gráfico de barras horizontal
            fig = go.Figure(data=[
                go.Bar(
                    y=df_estado['estado'],
                    x=df_estado['total_valor'],
                    orientation='h',
                    text=df_estado['total_valor'].apply(lambda x: f'R$ {x:,.2f}'),
                    textposition='auto',
                )
            ])

            fig.update_layout(
                title='Distribuição de Vendas por Estado',
                xaxis_title='Valor Total de Vendas (R$)',
                yaxis_title='Estado',
                height=max(400, len(df_estado) * 50),  # Ajusta altura baseado no número de estados
                showlegend=False
            )

            # Criar gráfico de pizza para distribuição percentual
            fig_pie = go.Figure(data=[
                go.Pie(
                    labels=df_estado['estado'],
                    values=df_estado['total_valor'],
                    hole=.3,
                    textinfo='label+percent',
                    textposition='inside'
                )
            ])

            fig_pie.update_layout(
                title='Distribuição Percentual de Vendas por Estado',
                height=600,
                showlegend=True
            )

            return {
                'estados': resultados_estado,
                'cidades': resultados_cidade,
                'grafico_barras': fig,
                'grafico_pizza': fig_pie
            }
        return None

    def analyze_price_trends(self):
        """Análise histórica de preços"""
        query = """
        SELECT 
            t.data,
            c.nome_categoria,
            p.nome_produto,
            CAST(fp.preco_normal AS DECIMAL(10,2)) as preco_normal,
            CAST(fp.preco_promocional AS DECIMAL(10,2)) as preco_promocional,
            CAST(fp.preco_compra AS DECIMAL(10,2)) as preco_compra,
            CAST(fp.margem_lucro AS DECIMAL(10,2)) as margem_lucro,
            fp.em_promocao
        FROM fato_precos fp
        JOIN dim_tempo t ON fp.id_tempo = t.id_tempo
        JOIN dim_produto p ON fp.id_produto = p.id_produto
        JOIN dim_categoria c ON fp.id_categoria = c.id_categoria
        ORDER BY t.data, c.nome_categoria, p.nome_produto
        """
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall())
        
        if len(df) > 0:
            # Converter colunas para tipos numéricos
            numeric_columns = ['preco_normal', 'preco_promocional', 'preco_compra', 'margem_lucro']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Criar gráfico de linha para evolução de preços por produto
            fig_produtos = go.Figure()
            
            # Selecionar os 10 produtos mais caros para melhor visualização
            top_produtos = df.groupby('nome_produto')['preco_normal'].mean().nlargest(10).index
            df_top = df[df['nome_produto'].isin(top_produtos)]
            
            for produto in df_top['nome_produto'].unique():
                df_prod = df_top[df_top['nome_produto'] == produto]
                
                fig_produtos.add_trace(go.Scatter(
                    x=df_prod['data'],
                    y=df_prod['preco_normal'],
                    name=produto,
                    mode='lines+markers'
                ))
            
            fig_produtos.update_layout(
                title='Evolução Histórica de Preços por Produto (Top 10)',
                xaxis_title='Data',
                yaxis_title='Preço (R$)',
                height=600,
                showlegend=True
            )
            
            # Criar gráfico de linha para evolução de preços por categoria
            fig_categorias = go.Figure()
            
            df_cat = df.groupby(['data', 'nome_categoria']).agg({
                'preco_normal': 'mean',
                'preco_promocional': 'mean',
                'margem_lucro': 'mean'
            }).reset_index()
            
            for categoria in df_cat['nome_categoria'].unique():
                df_cat_filtered = df_cat[df_cat['nome_categoria'] == categoria]
                
                fig_categorias.add_trace(go.Scatter(
                    x=df_cat_filtered['data'],
                    y=df_cat_filtered['preco_normal'],
                    name=f'{categoria}',
                    mode='lines+markers'
                ))
            
            fig_categorias.update_layout(
                title='Evolução Histórica de Preços Médios por Categoria',
                xaxis_title='Data',
                yaxis_title='Preço Médio (R$)',
                height=600,
                showlegend=True
            )
            
            # Criar gráfico de linha para evolução da margem de lucro
            fig_margem = go.Figure()
            
            for categoria in df_cat['nome_categoria'].unique():
                df_cat_filtered = df_cat[df_cat['nome_categoria'] == categoria]
                
                fig_margem.add_trace(go.Scatter(
                    x=df_cat_filtered['data'],
                    y=df_cat_filtered['margem_lucro'],
                    name=categoria,
                    mode='lines+markers'
                ))
            
            fig_margem.update_layout(
                title='Evolução Histórica da Margem de Lucro por Categoria',
                xaxis_title='Data',
                yaxis_title='Margem de Lucro (%)',
                height=600,
                showlegend=True
            )
            
            # Salvar gráficos
            fig_produtos.write_html('analise_historica_precos_produtos.html')
            fig_categorias.write_html('analise_historica_precos_categorias.html')
            fig_margem.write_html('analise_historica_margem_lucro.html')
            
            # Calcular variação de preços
            df_variacao = df.groupby('nome_produto').agg({
                'preco_normal': ['first', 'last', 'mean', 'min', 'max'],
                'data': ['first', 'last']
            })
            
            df_variacao['variacao_percentual'] = ((df_variacao[('preco_normal', 'last')] - 
                                                 df_variacao[('preco_normal', 'first')]) / 
                                                df_variacao[('preco_normal', 'first')] * 100).round(2)
            
            return df_variacao
        return None

    def analyze_inventory_trends(self):
        """Análise histórica de estoque"""
        query = """
        SELECT 
            t.data,
            c.nome_categoria,
            p.nome_produto,
            l.nome_loja,
            CAST(fe.quantidade_atual AS DECIMAL(10,2)) as quantidade_atual,
            CAST(fe.quantidade_minima AS DECIMAL(10,2)) as quantidade_minima,
            CAST(fe.quantidade_maxima AS DECIMAL(10,2)) as quantidade_maxima,
            CAST(fe.dias_estoque AS DECIMAL(10,2)) as dias_estoque,
            fe.status_estoque
        FROM fato_estoque fe
        JOIN dim_tempo t ON fe.id_tempo = t.id_tempo
        JOIN dim_produto p ON fe.id_produto = p.id_produto
        JOIN dim_categoria c ON p.id_categoria = c.id_categoria
        JOIN dim_loja l ON fe.id_loja = l.id_loja
        ORDER BY t.data, c.nome_categoria, p.nome_produto
        """
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall())
        
        if len(df) > 0:
            # Converter colunas para tipos numéricos
            numeric_columns = ['quantidade_atual', 'quantidade_minima', 'quantidade_maxima', 'dias_estoque']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Criar gráfico de linha para evolução do estoque por produto
            fig_produtos = go.Figure()
            
            # Calcular variação de estoque
            df['variacao_estoque'] = df.groupby('nome_produto')['quantidade_atual'].transform(lambda x: x.max() - x.min())
            top_produtos = df.groupby('nome_produto')['variacao_estoque'].mean().nlargest(10).index
            df_top = df[df['nome_produto'].isin(top_produtos)]
            
            for produto in df_top['nome_produto'].unique():
                df_prod = df_top[df_top['nome_produto'] == produto]
                
                fig_produtos.add_trace(go.Scatter(
                    x=df_prod['data'],
                    y=df_prod['quantidade_atual'],
                    name=produto,
                    mode='lines+markers'
                ))
            
            fig_produtos.update_layout(
                title='Evolução Histórica do Estoque por Produto (Top 10 em Variação)',
                xaxis_title='Data',
                yaxis_title='Quantidade em Estoque',
                height=600,
                showlegend=True
            )
            
            # Criar gráfico de linha para evolução do estoque por categoria
            fig_categorias = go.Figure()
            
            df_cat = df.groupby(['data', 'nome_categoria']).agg({
                'quantidade_atual': 'sum',
                'quantidade_minima': 'sum',
                'quantidade_maxima': 'sum',
                'dias_estoque': 'mean'
            }).reset_index()
            
            for categoria in df_cat['nome_categoria'].unique():
                df_cat_filtered = df_cat[df_cat['nome_categoria'] == categoria]
                
                fig_categorias.add_trace(go.Scatter(
                    x=df_cat_filtered['data'],
                    y=df_cat_filtered['quantidade_atual'],
                    name=f'{categoria}',
                    mode='lines+markers'
                ))
            
            fig_categorias.update_layout(
                title='Evolução Histórica do Estoque Total por Categoria',
                xaxis_title='Data',
                yaxis_title='Quantidade Total em Estoque',
                height=600,
                showlegend=True
            )
            
            # Criar gráfico de linha para evolução dos dias de estoque
            fig_dias = go.Figure()
            
            for categoria in df_cat['nome_categoria'].unique():
                df_cat_filtered = df_cat[df_cat['nome_categoria'] == categoria]
                
                fig_dias.add_trace(go.Scatter(
                    x=df_cat_filtered['data'],
                    y=df_cat_filtered['dias_estoque'],
                    name=categoria,
                    mode='lines+markers'
                ))
            
            fig_dias.update_layout(
                title='Evolução Histórica dos Dias de Estoque por Categoria',
                xaxis_title='Data',
                yaxis_title='Dias de Estoque',
                height=600,
                showlegend=True
            )
            
            # Criar gráfico de área para evolução do status de estoque
            df_status = df.groupby(['data', 'status_estoque']).size().unstack(fill_value=0)
            
            fig_status = go.Figure()
            
            for status in df_status.columns:
                fig_status.add_trace(go.Scatter(
                    x=df_status.index,
                    y=df_status[status],
                    name=status,
                    mode='lines',
                    stackgroup='one'
                ))
            
            fig_status.update_layout(
                title='Evolução Histórica do Status de Estoque',
                xaxis_title='Data',
                yaxis_title='Quantidade de Produtos',
                height=600,
                showlegend=True
            )
            
            # Salvar gráficos
            fig_produtos.write_html('analise_historica_estoque_produtos.html')
            fig_categorias.write_html('analise_historica_estoque_categorias.html')
            fig_dias.write_html('analise_historica_dias_estoque.html')
            fig_status.write_html('analise_historica_status_estoque.html')
            
            # Calcular variação de estoque
            df_variacao = df.groupby('nome_produto').agg({
                'quantidade_atual': ['first', 'last', 'mean', 'min', 'max'],
                'dias_estoque': ['mean', 'min', 'max'],
                'data': ['first', 'last']
            })
            
            df_variacao['variacao_quantidade'] = (df_variacao[('quantidade_atual', 'last')] - 
                                                df_variacao[('quantidade_atual', 'first')])
            
            df_variacao['variacao_percentual'] = ((df_variacao[('quantidade_atual', 'last')] - 
                                                 df_variacao[('quantidade_atual', 'first')]) / 
                                                df_variacao[('quantidade_atual', 'first')] * 100).round(2)
            
            # Análise de status atual
            df_status_atual = df.groupby('nome_produto').agg({
                'status_estoque': 'last'
            })
            
            return df_variacao, df_status_atual
        return None, None

def main():
    try:
        print("Gerando análises e gráficos...")
        analyzer = OLAPAnalyzer()
        
        # Análise de tendências gerais
        fig = analyzer.analyze_trends()
        if fig:
            fig.write_html("analise_tendencias.html")
            print("- Gerado: analise_tendencias.html")
        
        # Análise de localização
        resultados_estado = analyzer.analyze_by_location()
        if resultados_estado:
            resultados_estado['grafico_barras'].write_html("analise_localizacao_barras.html")
            resultados_estado['grafico_pizza'].write_html("analise_localizacao_pizza.html")
            print("- Gerado: analise_localizacao_barras.html")
            print("- Gerado: analise_localizacao_pizza.html")
            
            print("\n=== Análise de Vendas por Estado ===\n")
            for estado in resultados_estado['estados']:
                print(f"Estado: {estado['estado']}")
                print(f"Número de Vendas: {estado['num_vendas']}")
                print(f"Número de Clientes: {estado['num_clientes']}")
                print(f"Número de Lojas: {estado['num_lojas']}")
                print(f"Quantidade Total: {estado['total_quantidade']}")
                print(f"Valor Total: R$ {estado['total_valor']:.2f}")
                print(f"Ticket Médio: R$ {estado['ticket_medio']:.2f}\n")
            
            print("\n=== Top 10 Cidades em Vendas ===\n")
            for cidade in resultados_estado['cidades']:
                print(f"Cidade: {cidade['cidade']} - {cidade['estado']}")
                print(f"Número de Vendas: {cidade['num_vendas']}")
                print(f"Número de Clientes: {cidade['num_clientes']}")
                print(f"Valor Total: R$ {cidade['total_valor']:.2f}\n")
        
        # Análise histórica de preços
        resultados_preco = analyzer.analyze_price_trends()
        if resultados_preco is not None:
            print("- Gerado: analise_historica_precos_produtos.html")
            print("- Gerado: analise_historica_precos_categorias.html")
            print("- Gerado: analise_historica_margem_lucro.html")
            
            print("\n=== Análise Histórica de Preços ===\n")
            for produto in resultados_preco.index:
                print(f"Produto: {produto}")
                print(f"Preço Inicial: R$ {resultados_preco.loc[produto, ('preco_normal', 'first')]:.2f}")
                print(f"Preço Final: R$ {resultados_preco.loc[produto, ('preco_normal', 'last')]:.2f}")
                print(f"Preço Médio: R$ {resultados_preco.loc[produto, ('preco_normal', 'mean')]:.2f}")
                print(f"Preço Mínimo: R$ {resultados_preco.loc[produto, ('preco_normal', 'min')]:.2f}")
                print(f"Preço Máximo: R$ {resultados_preco.loc[produto, ('preco_normal', 'max')]:.2f}")
                print(f"Variação de Preço: {resultados_preco.loc[produto, 'variacao_percentual']:.1f}%")
                print(f"Período: {resultados_preco.loc[produto, ('data', 'first')].strftime('%d/%m/%Y')} a {resultados_preco.loc[produto, ('data', 'last')].strftime('%d/%m/%Y')}\n")
        
        # Análise histórica de estoque
        resultados_estoque, status_atual = analyzer.analyze_inventory_trends()
        if resultados_estoque is not None:
            print("- Gerado: analise_historica_estoque_produtos.html")
            print("- Gerado: analise_historica_estoque_categorias.html")
            print("- Gerado: analise_historica_dias_estoque.html")
            print("- Gerado: analise_historica_status_estoque.html")
            
            print("\n=== Análise Histórica de Estoque ===\n")
            for produto in resultados_estoque.index:
                print(f"Produto: {produto}")
                print(f"Quantidade Inicial: {resultados_estoque.loc[produto, ('quantidade_atual', 'first')]:.0f}")
                print(f"Quantidade Final: {resultados_estoque.loc[produto, ('quantidade_atual', 'last')]:.0f}")
                print(f"Quantidade Média: {resultados_estoque.loc[produto, ('quantidade_atual', 'mean')]:.1f}")
                print(f"Quantidade Mínima: {resultados_estoque.loc[produto, ('quantidade_atual', 'min')]:.0f}")
                print(f"Quantidade Máxima: {resultados_estoque.loc[produto, ('quantidade_atual', 'max')]:.0f}")
                print(f"Variação de Quantidade: {resultados_estoque.loc[produto, 'variacao_quantidade']:.0f} unidades ({resultados_estoque.loc[produto, 'variacao_percentual']:.1f}%)")
                print(f"Dias de Estoque (Média/Min/Max): {resultados_estoque.loc[produto, ('dias_estoque', 'mean')]:.1f} / {resultados_estoque.loc[produto, ('dias_estoque', 'min')]:.1f} / {resultados_estoque.loc[produto, ('dias_estoque', 'max')]:.1f}")
                print(f"Status Atual: {status_atual.loc[produto, 'status_estoque']}")
                print(f"Período: {resultados_estoque.loc[produto, ('data', 'first')].strftime('%d/%m/%Y')} a {resultados_estoque.loc[produto, ('data', 'last')].strftime('%d/%m/%Y')}\n")

    except Exception as e:
        print(f"Erro durante a análise OLAP: {str(e)}")
    finally:
        if 'analyzer' in locals():
            del analyzer

if __name__ == "__main__":
    main() 