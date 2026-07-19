{{ config(
    materialized = 'table',
    partition_by = {'field': 'sk_data', 'data_type': 'date', 'granularity': 'month'},
    cluster_by = ['sk_municipio', 'sk_cliente']
) }}
    
select
    -- chaves estrangeiras
    data_contratacao                          as sk_data,
    coalesce(cnpj_cliente, '-1')              as sk_cliente,
    coalesce(id_municipio, '-1')              as sk_municipio,

    -- dimensões degeneradas
    id_contrato,
    descricao_projeto,

    -- medidas
    valor_contratado,
    valor_desembolsado,
    valor_contratado - valor_desembolsado     as valor_a_desembolsar,
    safe_divide(valor_desembolsado, valor_contratado) as taxa_desembolso

from {{ ref('stg_bndes__operacoes') }}
