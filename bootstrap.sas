%macro bootstrap(
/****************************************************************************/
/*  Create a bootstrap sample set in Netezza                                */
/****************************************************************************/
  data_in     = , /*  Initial dataset to be bootstrapped                    */
  connection  = , /*  Connection to Netezza                                 */
  n_samples   = , /*  Total number of samples                               */
  samp_size   = , /*  Sample Size per sample                                */
  data_out    =   /*  Output dataset                                        */
/****************************************************************************/
);

  /* Generate unique names for objects in Netezza */  
  select nm
  into :bootstrap
  from connection to &connection (
    SELECT 
      'TBL'||ID||'_'||ABS(HASH4(USER||NOW())) AS NM
    FROM 
      _V_SESSION 
    WHERE
      USERNAME = USER 
      AND STATUS = 'active'
    ORDER BY CONNTIME DESC
    LIMIT 1);
  
  /* Copy input data with arbitrarily ranked Row IDs */
  execute by &connection (
    CREATE TEMP TABLE INIT_&bootstrap AS
    SELECT 
      ROW_NUMBER() OVER(ORDER BY RANDOM()) AS ROW_ID,
      *
    FROM 
      &data_in
  );
  
  /* Find max Row ID (also, no. of rows) */
  select n 
  into :n
  from connection to &connection (
    SELECT
      MAX(ROW_ID) AS N
    FROM 
      INIT_&bootstrap
  );

  /* Create a table of resamples with replacement */
  execute by &connection (
    CREATE TEMP TABLE &bootstrap AS 
    SELECT 
      0::INT AS ROW_ID,
      0::INT AS BOOT_ID
    LIMIT 0      
  );

  %do i = 1 %to &n_samples;
    execute by &connection (
      INSERT INTO &bootstrap
      SELECT 
        (&n * RANDOM() + 0.5)::INT AS ROW_ID,
        &i AS BOOT_ID
      FROM
        _V_DUAL_DSLICE D1
      CROSS JOIN
        _V_DUAL_DSLICE D2
      CROSS JOIN 
        _V_DUAL_DSLICE D3
      LIMIT &samp_size
    );
  %end;

  /* Merge with initial dataset */
  execute by &connection (
    CREATE TEMP TABLE &data_out AS
    SELECT A.*, B.BOOT_ID
    FROM
      INIT_&bootstrap A
    JOIN
      &bootstrap B
    ON
      A.ROW_ID = B.ROW_ID
  );

%mend bootstrap;
