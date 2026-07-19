{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'sk_data', 'data_type': 'date', 'granularity': 'month'},
    cluster_by = ['sk_municipio', 'sk_cliente'],
    on_schema_change = 'append_new_columns'
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

{% if is_incremental() %}
where data_contratacao >= (
    select date_sub(max(sk_data), interval 3 month) from {{ this }}
)
{% endif %}