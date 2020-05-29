This folder contains the `csv` files that are used for usage analysis. I've used the following SQL command to generate the CSVs:

```sql
SELECT c_table, c_facet FROM public.derivarequests
    WHERE fromhost LIKE 'www.rebuildingakidney.org'
    AND c_cid ~ 'recordset|record|recordedit|viewer|navbar'
    AND c_table IS NOT null
    AND (r_status BETWEEN 200 AND 299 OR r_status = 304)
    AND devicereportedtime >= '2018-01-01' AND devicereportedtime <= '2019-07-15’;
```

- The `c_cid` filter was changed for generating app specific csv files.
- The `fromhost` filter was changed for generating data for other deployments.
