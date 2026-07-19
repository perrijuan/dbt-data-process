with dias as (
    select dia
    from unnest(generate_date_array('2001-01-01', '2025-12-31')) as dia
)

select
    dia                                        as sk_data,
    extract(year    from dia)                  as ano,
    extract(month   from dia)                  as mes,
    extract(quarter from dia)                  as trimestre,
    format_date('%Y-%m', dia)                  as ano_mes,
    format_date('%B', dia)                     as nome_mes,
    extract(dayofweek from dia) in (1, 7)      as eh_fim_de_semana
from dias