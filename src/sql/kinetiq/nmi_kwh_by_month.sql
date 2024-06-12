SELECT /*+ PARALLEL(12) */ trunc(end_date,'MM') as the_month, installation_ref as nmi, sum(volume) as kwh
from pv2admin.invoice_detail
where end_date between date '{start_date_str}' and date '{end_date_str}'
and category_id='100'
and units='KWH'
and reversed_trans_id is NULL
group by trunc(end_date,'MM'), installation_ref
order by 1,2