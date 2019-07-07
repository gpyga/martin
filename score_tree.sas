%macro score_tree (
/****************************************************************************/
/* Score CART in Netezza                                                    */
/****************************************************************************/
  connection  = , /* Connection to Netezza                                  */
  data_in     = , /* Dataset to be scored                                   */
  tree_in     = , /* Netezza table with CART rules                          */

  data_out    =   /* Output Netezza table with scores                       */
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
    CREATE TABLE &data_out AS
    SELECT DISTINCT
      &key_id,
      0::BIGINT AS NODE,
      0::FLOAT AS SCORE
    FROM 
      &data_in
  );

  select n into :n from connection to &connection (
    SELECT MAX(DEPTH) AS N FROM &tree_in
  );

  %do i = 1 %to &n;

    execute by &connection (
      CALL UT_DROP_TABLE_IF_EXISTS(%str(%')T_&data_out.%str(%'))
    );

    execute by &connection (
      CREATE TEMP TABLE T_&data_out AS
      SELECT 
        A.%left(&key_id),
        B.NODE,
        B.SCORE
      FROM
        &data_out A
      JOIN (
        SELECT DISTINCT 
          PARENT_NODE,
          NODE,
          VAR,
          VAL,
          SCORE
        FROM &tree_in
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
      INSERT INTO T_&data_out
      SELECT
        A.&key_id,
        B.NODE,
        B.SCORE
      FROM
        &data_out A
      JOIN (
        SELECT DISTINCT
          PARENT_NODE,
          NODE,
          VAR,
          VAL,
          SCORE
        FROM &tree_in
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
        NVL(C.VAL,0) > B.VAL
    );

    execute by &connection (
      DELETE FROM &data_out
      WHERE &key_id IN (
        SELECT &key_id FROM T_&data_out
      )
    );
    execute by &connection (
      INSERT INTO &data_out
      SELECT * FROM T_&data_out
    );

  %end;

%mend score_tree;
