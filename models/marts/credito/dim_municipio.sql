with base as (
    select distinct
        id_municipio,
        nome_municipio,
        sigla_uf
    from {{ ref('stg_bndes__operacoes') }}
    where id_municipio is not null
)

select
    id_municipio    as sk_municipio,
    nome_municipio,
    sigla_uf,
    case
        when sigla_uf in ('AC','AP','AM','PA','RO','RR','TO') then 'Norte'
        when sigla_uf in ('AL','BA','CE','MA','PB','PE','PI','RN','SE') then 'Nordeste'
        when sigla_uf in ('DF','GO','MT','MS') then 'Centro-Oeste'
        when sigla_uf in ('ES','MG','RJ','SP') then 'Sudeste'
        when sigla_uf in ('PR','RS','SC') then 'Sul'
    end             as regiao
from base

union all

select '-1', 'Não informado', 'XX', 'Não informado'