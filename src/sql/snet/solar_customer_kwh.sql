WITH solar_nmi_customers AS (SELECT /*+ INDEX( AVH_N_VT_VF_IDX) PARALLEL(64) */ DISTINCT nmi
                             FROM sn_app.alzfnds_vw_hist
                             WHERE                        -- is_nmi_sub_po = '1'      --no idea?
                               --AND
                                 supplyconnected = 'TRUE' --energised
                               AND solar_installed = 'Yes'
                               AND valid_to >= DATE '{start_date_str}'
                               AND valid_from <= DATE '{end_date_str}')
SELECT /*+ PARALLEL(60) */ TRUNC(interval_time_local, 'MM') AS the_month,
                           nmi,
                           SUM(to_grid_wh) / 1000           AS solar_kwh_export
FROM network_prd_drv.interval_derived
WHERE interval_time_local BETWEEN TIMESTAMP '{start_date_str} 00:00:00' AND TIMESTAMP '{end_date_str} 23:59:00'
  AND nmi IN (SELECT nmi FROM solar_nmi_customers)
  AND to_grid_wh > 0 --only get nmis exporting
GROUP BY TRUNC(interval_time_local, 'MM'), nmi
ORDER BY 1, 2