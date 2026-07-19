with fonte as (
    select * from {{ source('bndes', 'operacoes_nao_automaticas') }}
),

limpo as (
    select
        cast(id_contrato as string)                     as id_contrato,
        cast(cnpj_cliente as string)                    as cnpj_cliente,
        trim(razao_social_cliente)                      as razao_social_cliente,
        cast(data_contratacao as date)                  as data_contratacao,
        cast(id_municipio as string)                    as id_municipio,
        trim(nome_municipio)                            as nome_municipio,
        upper(trim(sigla_uf))                           as sigla_uf,
        trim(descricao_projeto)                         as descricao_projeto,
        cast(valor_contratado  as numeric)              as valor_contratado,
        cast(valor_desembolsado as numeric)             as valor_desembolsado
    from fonte
    where data_contratacao is not null
)

select * from limpo