%macro train_tree (
/****************************************************************************/
/* Train CART in Netezza                                                    */
/****************************************************************************/
  connection  = , /* Connection to Netezza                                  */
  data_in     = , /* Dataset to be split                                    */

  method      = , /* ftest, lift, -gini-, -entropy-                         */
  height      = , /* Height of the tree                                     */
  min_search  = , /* Minimum node size to split                             */
  leaf_size   = , /* Minimum leaf size after split                          */

  tree_out    =   /* Output Netezza dataset with ruleset                    */
/****************************************************************************/
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
    START WITH 1 INCREMENT BY 1
  );

  execute by &connection (
    DROP TABLE M_&data_in IF EXISTS
  );

  execute by &connection (
    CREATE TEMP TABLE M_&data_in AS 
    SELECT
      *,
      0 AS NODE
    FROM 
      &data_in
  );
  
  execute by &connection (
    CREATE TABLE &tree_out AS
    SELECT 
      0::BIGINT AS PARENT_NODE,
      0::INT AS DEPTH,
      ''::VARCHAR(200) AS VAR,
      0::FLOAT AS VAL,
      0::INT AS COND,
      0::BIGINT AS NODE,
      0::FLOAT AS SCORE,
      0::INT AS POP
    LIMIT 0
  );

  %let i = 1;
  %do %until (&i > &height);

    %node_split (
      connection  = &connection, 
      data_in     = M_&data_in,
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
        INSERT INTO &TREE_OUT
        SELECT 
          PARENT_NODE,
          &i AS DEPTH,
          VAR,
          VAL,
          COND,
          NODE,
          SCORE,
          POP 
        FROM
          depth_&i
      );

      execute by &connection (CALL UT_DROP_TABLE_IF_EXISTS('T_NODES'));

      execute by &connection (
        CREATE TEMP TABLE T_NODES AS
        SELECT DISTINCT
          &key_id,
          B.PARENT_NODE,
          B.NODE
        FROM 
          M_&data_in A
        JOIN 
          (SELECT * FROM depth_&i WHERE COND = 1) B
        ON 
          A.NODE = B.PARENT_NODE 
          AND A.VAR = B.VAR
          AND A.VAL <= B.VAL
      );

      execute by &connection (
        INSERT INTO T_NODES
        SELECT DISTINCT
          &key_id,
          B.PARENT_NODE,
          B.NODE
        FROM
          M_&data_in A
        JOIN
          (SELECT * FROM depth_&i WHERE COND = 0) B
        ON
          A.NODE = B.PARENT_NODE
          AND A.VAR = B.VAR
          AND A.VAL > B.VAL
      );

      %if method eq lift %then %do;
        execute by &connection (
          CREATE TEMP TABLE T_&data_in AS
          SELECT
            A.%left(&key_id),
            B.NODE,
            A.VAR,
            A.VAL,
            A.TREAT,
            A.TRGT
          FROM
            M_&data_in A
          JOIN
            T_NODES B
          ON
            A.%left(&key_id) = B.%left(&key_id)
            AND A.NODE = B.PARENT_NODE
        );
      %end;
      %else %do;
        execute by &connection (
          CREATE TEMP TABLE T_&data_in AS
          SELECT 
            A.%left(&key_id),
            B.NODE,
            A.VAR,
            A.VAL,
            A.TRGT
          FROM 
            M_&data_in A 
          JOIN 
            T_NODES B 
          ON 
            A.%left(&key_id) = B.%left(&key_id) 
            AND A.NODE = B.PARENT_NODE
        );
      %end;

      execute by &connection (DROP TABLE M_&data_in IF EXISTS);
      execute by &connection (ALTER TABLE T_&data_in RENAME TO M_&data_in);
      
      %let i = %eval(&i + 1);
    %end;
  %end;

  execute by &connection (DROP SEQUENCE &seq_nm);

%mend train_tree;
