select id_contrato, count(*) as n
from {{ ref('fct_operacoes_contratadas') }}
group by 1
having count(*) > 1