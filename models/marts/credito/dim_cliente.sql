with base as (

    select
        cnpj_cliente,
        max(razao_social_cliente) as razao_social_cliente,
        min(data_contratacao)     as primeira_operacao,
        max(data_contratacao)     as ultima_operacao,
        count(*)                  as qtd_operacoes,
        sum(valor_contratado)     as total_contratado
    from {{ ref('stg_bndes__operacoes') }}
    where cnpj_cliente is not null
    group by cnpj_cliente

),

clientes as (

    select
        cnpj_cliente                    as sk_cliente,
        cnpj_cliente,
        substr(cnpj_cliente, 1, 8)      as raiz_cnpj,
        razao_social_cliente,
        primeira_operacao,
        ultima_operacao,
        qtd_operacoes,
        total_contratado
    from base

),

nao_informado as (

    select
        '-1'                     as sk_cliente,
        cast(null as string)     as cnpj_cliente,
        cast(null as string)     as raiz_cnpj,
        'Não informado'          as razao_social_cliente,
        cast(null as date)       as primeira_operacao,
        cast(null as date)       as ultima_operacao,
        0                        as qtd_operacoes,
        cast(0 as numeric)       as total_contratado

)

select * from clientes
union all
select * from nao_informado