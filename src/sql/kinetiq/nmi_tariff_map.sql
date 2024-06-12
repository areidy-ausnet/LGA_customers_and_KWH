SELECT distinct installation_ref as nmi,
                first_value(REGEXP_SUBSTR(tariff_time_block_id, '^[a-zA-Z0-9]+')) OVER (partition by installation_ref order by end_date desc) as tariff
from  pv2admin.invoice_detail
WHERE end_date BETWEEN date '{start_date_str}' AND DATE '{end_date_str}'
AND  category_id = '100'
AND units = 'KWH'
AND reversed_trans_id IS NULL
order by 1,2