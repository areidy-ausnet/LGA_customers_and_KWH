/* SDME is the internal electricity GIS application. This table seems to have an export of it.
   Gets the location of each NMI
 */
SELECT DISTINCT nmi, location_latitude as lat, location_longitude as long
       from imprd001_dimlayer.dim_sdme_substation
where row_expiration_dtm='9999-12-31'
and current_record_flag='Y'