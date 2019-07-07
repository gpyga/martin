%macro score_forest (
/****************************************************************************/
/* Score Random Forests in Netezza                                          */
/****************************************************************************/
  connection  = , /* Connection to Netezza                                  */
  data_in     = , /* Dataset to be scored                                   */
  forest_in   = , /* Netezza table with random forest set                   */

  data_out    =   /* Output Netzza table with scores                        */
/****************************************************************************/
);

  select attname into :key_id
  from connection to &connection (
    SELECT
      ATTNAME
    FROM
      _V_RELATION_COLUMN
    WHERE
      NAME = %str(%')&data_in.%str(%')
      AND ATTNUM = 1
  );

  execute by &connection (
    CREATE TEMP TABLE CUST_LIST AS
    SELECT DISTINCT
      &key_id
    FROM 
      &data_in
  );

  execute by &connection (
    CREATE TEMP TABLE TREE_LIST AS
    SELECT 
      MIN(PARENT_NODE) AS NODE,
      TREE
    FROM 
      &forest_in
    WHERE 
      DEPTH = 1
    GROUP BY TREE
  );

  execute by &connection (
    CREATE TABLE &data_out AS
    SELECT DISTINCT
      &key_id,
      TREE,
      NODE,
      0::FLOAT AS SCORE
    FROM 
      CUST_LIST
    CROSS JOIN
      TREE_LIST
  );

  select n into :n from connection to &connection (
    SELECT MAX(DEPTH) AS N FROM &forest_in
  );

  %do i = 1 %to &n;

    execute by &connection (
      CALL UT_DROP_TABLE_IF_EXISTS(%str(%')N_&data_out.%str(%'))
    );

    execute by &connection (
      CREATE TEMP TABLE N_&data_out AS
      SELECT DISTINCT
        A.%left(&key_id),
        B.TREE,
        B.NODE,
        B.SCORE
      FROM
        &data_out A
      JOIN (
        SELECT DISTINCT 
          PARENT_NODE,
          TREE,
          NODE,
          VAR,
          VAL,
          SCORE
        FROM &forest_in
        WHERE 
          COND = 1 
          AND DEPTH = &i) B
      ON
        A.NODE = B.PARENT_NODE
      LEFT JOIN 
        &data_in C
      ON
        C.%left(&key_id) = A.%left(&key_id)
        AND B.VAR = C.VAR
      WHERE
        NVL(C.VAL,0) <= B.VAL
    );

    execute by &connection (
      INSERT INTO N_&data_out
      SELECT DISTINCT
        A.&key_id,
        B.TREE,
        B.NODE,
        B.SCORE
      FROM
        &data_out A
      JOIN (
        SELECT DISTINCT
          PARENT_NODE,
          TREE,
          NODE,
          VAR,
          VAL,
          SCORE
        FROM &forest_in
        WHERE
          COND = 0
          AND DEPTH = &i) B
      ON
        A.NODE = B.PARENT_NODE
      LEFT JOIN
        &data_in C
      ON
        C.&key_id = A.&key_id
        AND B.VAR = C.VAR
      WHERE
        NVL(C.VAL, 0) > B.VAL
    );

    execute by &connection (
      CREATE TABLE T_&data_out AS
      SELECT DISTINCT
        A.&key_id,
        A.TREE,
        NVL(B.NODE, A.NODE) AS NODE,
        NVL(B.SCORE, A.SCORE) AS SCORE
      FROM
        &data_out A
      LEFT JOIN
        N_&data_out B
      ON
        A.&key_id = B.&key_id
        AND A.TREE = B.TREE
    );

    execute by &connection (
      DROP TABLE &data_out
    );

    execute by &connection (
      ALTER TABLE T_&data_out RENAME TO &data_out
    );
  %end;

%mend score_forest;
