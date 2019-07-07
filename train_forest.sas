%macro train_forest (
/****************************************************************************/
/* Train Random Forest in Netezza                                           */
/****************************************************************************/
  connection  = , /* Connection to Netezza                                  */
  data_in     = , /* Dataset to be split                                    */

  method      = , /* ftest, lift, -gini-, -entropy-                         */
  n_trees     = , /* Number of trees in the forest                          */
  height      = , /* Height of the tree                                     */
  min_search  = , /* Minimum node size to split                             */
  leaf_size   = , /* Minimum leaf size after split                          */

  forest_out  =   /* Output SAS dataset with rules                          */
/****************************************************************************/
);

  select nm
  into :seq_nm
  from connection to &connection (
    SELECT
      'SEQ'||ID||'_'||ABS(HASH4(USER||NOW())) AS NM
    FROM
      _V_SESSION
    WHERE
      USERNAME = USER
      AND STATUS = 'active'
    ORDER BY CONNTIME DESC
    LIMIT 1);

  execute by &connection (
    CREATE SEQUENCE &seq_nm AS BIGINT
    START WITH %eval(&n_trees +1) 
    INCREMENT BY 1
  );

  select attname into :key_id
  from connection to &connection (
    SELECT
      ATTNAME
    FROM
      _V_RELATION_COLUMN
    WHERE
      UPPER(NAME) = UPPER(%str(%')&data_in.%str(%'))
      AND ATTNUM = 1
  );

  execute by &connection (
    CREATE TEMP TABLE SAMP_&data_in AS
    SELECT DISTINCT
      &key_id
    FROM
      &data_in
  );

  select n into :samp_size
  from connection to &connection (
    SELECT (COUNT(1) * 0.8)::INT AS N FROM SAMP_&data_in
  );

  execute by &connection (
    CREATE TABLE &forest_out AS
    SELECT
      0::BIGINT AS PARENT_NODE,
      0::INT AS DEPTH,
      ''::VARCHAR(200) AS VAR,
      0::FLOAT AS VAL,
      0::INT AS COND,
      0::BIGINT AS NODE,
      0::BIGINT AS TREE,
      0::FLOAT AS SCORE,
      0::INT AS POP
    LIMIT 0
  );

  %bootstrap (
    data_in     = SAMP_&data_in,
    connection  = &connection,
    n_samples   = &n_trees,
    samp_size   = &samp_size,
    data_out    = BOOT_&data_in
  );

  execute by &connection (
    CREATE TEMP TABLE BS_&data_in AS
    SELECT 
      &key_id,
      BOOT_ID AS NODE,
      BOOT_ID AS TREE
    FROM
      BOOT_&data_in
  );

  execute by &connection (
    CREATE TEMP TABLE VAR_LIST AS
    SELECT DISTINCT VAR FROM &data_in
  );

  select n into :n_vars 
  from connection to &connection (
    SELECT SQRT(COUNT(1))::INT AS N FROM VAR_LIST
  );

  %let i = 1;
  %do %until (&i > &height);

    execute by &connection (
      CALL UT_DROP_TABLE_IF_EXISTS('NODE_LIST')
    );
    
    execute by &connection (
      CREATE TEMP TABLE NODE_LIST AS
      SELECT DISTINCT NODE, TREE FROM BS_&data_in
    );

    execute by &connection (
      CALL UT_DROP_TABLE_IF_EXISTS('RAND_SUBSPACE')
    );

    /* Select Random Subspaces */
    execute by &connection (
      CREATE TEMP TABLE RAND_SUBSPACE AS
      SELECT *
      FROM 
        NODE_LIST
      CROSS JOIN 
        VAR_LIST
      WHERE 
        RANDOM() <= &n_vars**-1
    );

    execute by &connection (
      CALL UT_DROP_TABLE_IF_EXISTS('SUBSPACE_DATA')
    );

    execute by &connection (
      CREATE TEMP TABLE SUBSPACE_DATA AS
      SELECT 
        A.*, B.NODE, B.TREE
      FROM
        &data_in A
      JOIN 
        RAND_SUBSPACE B
      ON
        A.VAR = B.VAR
      JOIN
        BS_&data_in C
      ON
        C.%left(&key_id) = A.%left(&key_id)
        AND C.NODE = B.NODE
    );

    %node_split (
      connection  = &connection,
      data_in     = SUBSPACE_DATA,
      sequence    = &seq_nm,
      method      = &method,
      min_search  = &min_search,
      leaf_size   = &leaf_size,
      rules_out   = depth_&i
    );

    select n into :n
    from connection to &connection (
      SELECT COUNT(1) AS N FROM depth_&i
    );

    %if &n = 0 %then %do;
      /* Force quit if no more rules */
      %let i = %eval(&height + 1);
    %end;
    %else %do;

      execute by &connection (
        DELETE FROM depth_&i
        WHERE NODE NOT IN (
          SELECT MIN(NODE)
          FROM DEPTH_&i
          GROUP BY PARENT_NODE, COND)
      );

      execute by &connection (
        INSERT INTO &forest_out
        SELECT
          A.PARENT_NODE,
          &i AS DEPTH,
          A.VAR,
          A.VAL,
          A.COND,
          A.NODE,
          B.TREE,
          A.SCORE,
          A.POP
        FROM
          depth_&i A
        JOIN
          NODE_LIST B
        ON
          A.PARENT_NODE = B.NODE
      );

      execute by &connection (DROP TABLE BS_&data_in);
      execute by &connection (
        CREATE TEMP TABLE BS_&data_in AS
        SELECT DISTINCT
          &key_id,
          B.NODE,
          B.TREE
        FROM
          SUBSPACE_DATA A
        JOIN
          (SELECT * FROM &forest_out WHERE COND = 1 AND DEPTH = &i) B
        ON
          A.NODE = B.PARENT_NODE
          AND A.VAR = B.VAR
          AND A.VAL <= B.VAL
      );

      execute by &connection (
        INSERT INTO BS_&data_in
        SELECT DISTINCT
          &key_id,
          B.NODE,
          B.TREE
        FROM
          SUBSPACE_DATA A
        JOIN
          (SELECT * FROM &forest_out WHERE COND = 0 AND DEPTH = &i) B
        ON
          A.NODE = B.PARENT_NODE
          AND A.VAR = B.VAR
          AND A.VAL > B.VAL
      );
      
      %let i = %eval(&i + 1);
      
    %end;
  %end;

  execute by &connection (DROP SEQUENCE &seq_nm);

%mend train_forest;


