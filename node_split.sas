%macro node_split (
/****************************************************************************/
/*  Split nodes based on optimal criteria                                   */
/****************************************************************************/
  connection  = , /* Connection to Netezza                                  */
  data_in     = , /* Dataset to be split                                    */
  sequence    = , /* Temporary sequence to ID nodes                         */

  method      = , /* ftest, lift, -gini-, -entropy-                         */
  min_search  = , /* Minimum node size to split                             */
  leaf_size   = , /* Minimum leaf size                                      */

  rules_out   =   /* Output Netezza table with rules                        */
/****************************************************************************/
);

  %if &method = ftest %then %do;

    execute by &connection (CALL UT_DROP_TABLE_IF_EXISTS('SUMMARY'));
    execute by &connection (
      CREATE TEMP TABLE SUMMARY AS
      SELECT
        VAR, VAL, NODE, 
        SUM(TRGT) AS SUM_Y,
        SUM(TRGT ** 2) AS SUMSQ_Y,
        COUNT(TRGT) AS POP
      FROM
        &data_in
      GROUP BY 1, 2, 3
    );
  
    execute by &connection (CALL UT_DROP_TABLE_IF_EXISTS('CUMSUM'));
    execute by &connection (
      CREATE TEMP TABLE CUMSUM AS
      SELECT
        NODE,
        VAR,
        VAL,

        SUM(SUM_Y) OVER(PARTITION BY NODE, VAR) AS TTL_SUM,
        SUM(SUMSQ_Y) OVER(PARTITION BY NODE, VAR) AS TTL_SUMSQ,
        SUM(POP) OVER(PARTITION BY NODE, VAR) AS TTL_POP,
        TTL_SUMSQ + (TTL_SUM**2 / TTL_POP) AS TTL_SSE,

        SUM(SUM_Y) OVER(
          PARTITION BY NODE, VAR
          ORDER BY VAL
          ROWS UNBOUNDED PRECEDING
        ) AS LEFT_SUM,
        SUM(SUMSQ_Y) OVER(
          PARTITION BY NODE, VAR
          ORDER BY VAL
          ROWS UNBOUNDED PRECEDING
        ) AS LEFT_SUMSQ,
        SUM(POP) OVER(
          PARTITION BY NODE, VAR
          ORDER BY VAL
          ROWS UNBOUNDED PRECEDING
        ) AS LEFT_POP,
        LEFT_SUMSQ + (LEFT_SUM**2 / LEFT_POP) AS LEFT_SSE,
  
        TTL_SUM - LEFT_SUM AS RIGHT_SUM,
        TTL_SUMSQ - LEFT_SUMSQ AS RIGHT_SUMSQ,
        TTL_POP - LEFT_POP AS RIGHT_POP,
        CASE
          WHEN RIGHT_POP > 0
          THEN RIGHT_SUMSQ + (RIGHT_SUM**2 / RIGHT_POP)
        END AS RIGHT_SSE,
      
        (LEFT_POP + RIGHT_POP - 2) *
        (LEFT_SUM/LEFT_POP - RIGHT_SUM/RIGHT_POP)**2 / 
        ((LEFT_POP**-1 + RIGHT_POP**-1) * (LEFT_SSE + RIGHT_SSE)) AS TEST,

        LEFT_SUM / LEFT_POP AS LEFT_SCORE,
        CASE
          WHEN RIGHT_POP > 0
          THEN RIGHT_SUM / RIGHT_POP
        END AS RIGHT_SCORE
      FROM
        SUMMARY
    );
    
    execute by &connection (CALL UT_DROP_TABLE_IF_EXISTS('BEST'));
    execute by &connection (
      CREATE TEMP TABLE BEST AS
      SELECT
        NODE,
        MAX(TEST) AS TEST
      FROM CUMSUM
      WHERE
        LEAST(RIGHT_POP, LEFT_POP) >= GREATEST(&leaf_size, 1)
        AND TTL_POP >= &min_search
        AND LEFT_SSE > 0 AND RIGHT_SSE > 0
      GROUP BY 1
    );
  
  %end;
  %else %if &method = lift %then %do;
 
    execute by &connection (CALL UT_DROP_TABLE_IF_EXISTS('SUMMARY'));
    execute by &connection (
      CREATE TEMP TABLE SUMMARY AS
      SELECT
        VAR, VAL, NODE,

        SUM(NVL(DECODE(TREAT, 1, TRGT), 0)) AS SUM_Y1, 
        SUM(NVL(DECODE(TREAT, 1, TRGT**2), 0)) AS SUMSQ_Y1,
        COUNT(DECODE(TREAT, 1, TRGT)) AS POP1,

        SUM(NVL(DECODE(TREAT, 0, TRGT), 0)) AS SUM_Y0,
        SUM(NVL(DECODE(TREAT, 0, TRGT**2), 0)) AS SUMSQ_Y0,
        COUNT(DECODE(TREAT, 0, TRGT)) AS POP0

      FROM
        &data_in
      GROUP BY 1, 2, 3
    );
 
    execute by &connection (CALL UT_DROP_TABLE_IF_EXISTS('CUMSUM'));
    execute by &connection (
      CREATE TEMP TABLE CUMSUM AS
      SELECT
        NODE,
        VAR,
        VAL,
  
        SUM(SUM_Y1) OVER(PARTITION BY NODE, VAR) AS TTL_SUM1,
        SUM(SUMSQ_Y1) OVER(PARTITION BY NODE, VAR) AS TTL_SUMSQ1,
        SUM(POP1) OVER(PARTITION BY NODE, VAR) AS TTL_POP1,
        TTL_SUMSQ1 + (TTL_SUM1**2 / TTL_POP1) AS TTL_SSE1,
  
        SUM(SUM_Y1) OVER(
          PARTITION BY NODE, VAR
          ORDER BY VAL
          ROWS UNBOUNDED PRECEDING
        ) AS LEFT_SUM1,
        SUM(SUMSQ_Y1) OVER(
          PARTITION BY NODE, VAR
          ORDER BY VAL
          ROWS UNBOUNDED PRECEDING
        ) AS LEFT_SUMSQ1,
        SUM(POP1) OVER(
          PARTITION BY NODE, VAR
          ORDER BY VAL
          ROWS UNBOUNDED PRECEDING
        ) AS LEFT_POP1,
        CASE
          WHEN LEFT_POP1 > 0
          THEN LEFT_SUMSQ1 + (LEFT_SUM1**2 / LEFT_POP1)
        END AS LEFT_SSE1,
  
        TTL_SUM1 - LEFT_SUM1 AS RIGHT_SUM1,
        TTL_SUMSQ1 - LEFT_SUMSQ1 AS RIGHT_SUMSQ1,
        TTL_POP1 - LEFT_POP1 AS RIGHT_POP1,
        CASE
          WHEN RIGHT_POP1 > 0
          THEN RIGHT_SUMSQ1 + (RIGHT_SUM1**2 / RIGHT_POP1)
        END AS RIGHT_SSE1,
        
        SUM(SUM_Y0) OVER(PARTITION BY NODE, VAR) AS TTL_SUM0,
        SUM(SUMSQ_Y0) OVER(PARTITION BY NODE, VAR) AS TTL_SUMSQ0,
        SUM(POP0) OVER(PARTITION BY NODE, VAR) AS TTL_POP0,
        TTL_SUMSQ0 + (TTL_SUM0**2 / TTL_POP0) AS TTL_SSE0,
  
        SUM(SUM_Y0) OVER(
          PARTITION BY NODE, VAR
          ORDER BY VAL
          ROWS UNBOUNDED PRECEDING
        ) AS LEFT_SUM0,
        SUM(SUMSQ_Y0) OVER(
          PARTITION BY NODE, VAR
          ORDER BY VAL
          ROWS UNBOUNDED PRECEDING
        ) AS LEFT_SUMSQ0,
        SUM(POP0) OVER(
          PARTITION BY NODE, VAR
          ORDER BY VAL
          ROWS UNBOUNDED PRECEDING
        ) AS LEFT_POP0,
        CASE
          WHEN LEFT_POP0 > 0
          THEN LEFT_SUMSQ0 + (LEFT_SUM0**2 / LEFT_POP0)
        END AS LEFT_SSE0,
        
        TTL_SUM0 - LEFT_SUM0 AS RIGHT_SUM0,
        TTL_SUMSQ0 - LEFT_SUMSQ0 AS RIGHT_SUMSQ0,
        TTL_POP0 - LEFT_POP0 AS RIGHT_POP0,
        CASE
          WHEN RIGHT_POP0 > 0
          THEN RIGHT_SUMSQ0 + (RIGHT_SUM0**2 / RIGHT_POP0)
        END AS RIGHT_SSE0,

        CASE 
          WHEN 
            LEAST(LEFT_POP1, LEFT_POP0, RIGHT_POP1, RIGHT_POP0) > 0 
            AND LEFT_SSE1 + RIGHT_SSE1 + LEFT_SSE0 + RIGHT_SSE0 > 0
          THEN 
            (LEFT_POP1 + RIGHT_POP1 + LEFT_POP0 + RIGHT_POP0 - 4) *
            ((RIGHT_SUM1/RIGHT_POP1 - RIGHT_SUM0/RIGHT_POP0) -
            (LEFT_SUM1/LEFT_POP1 - LEFT_SUM0/LEFT_POP0))**2 /
            ((LEFT_POP1**-1 + RIGHT_POP1**-1 + LEFT_POP0**-1 + RIGHT_POP0**-1)*
            (LEFT_SSE1 + RIGHT_SSE1 + LEFT_SSE0 + RIGHT_SSE0)) 
         END AS TEST,

        CASE 
          WHEN LEAST(LEFT_POP1, LEFT_POP0) > 0 
          THEN LEFT_SUM1/LEFT_POP1 - LEFT_SUM0/LEFT_POP0 
        END AS LEFT_SCORE,
        LEFT_POP1 + LEFT_POP0 AS LEFT_POP,

        CASE 
          WHEN LEAST(RIGHT_POP1, RIGHT_POP0) > 0 
          THEN RIGHT_SUM1/RIGHT_POP1 - RIGHT_SUM0/RIGHT_POP0 
        END AS RIGHT_SCORE,
        RIGHT_POP1 + RIGHT_POP0 AS RIGHT_POP
    
      FROM
        SUMMARY
    );
  
    execute by &connection (CALL UT_DROP_TABLE_IF_EXISTS('BEST'));
    execute by &connection (
      CREATE TEMP TABLE BEST AS
      SELECT 
        NODE, 
        MAX(TEST) AS TEST 
      FROM 
        CUMSUM 
      WHERE 
        LEAST(RIGHT_POP0, LEFT_POP0) >= GREATEST(&leaf_size, 1) 
        AND LEAST(RIGHT_POP1, LEFT_POP1) >= GREATEST(&leaf_size, 1) 
        AND TTL_POP1 + TTL_POP0 >= &min_search
        AND RIGHT_SSE0 > 0 AND LEFT_SSE0 > 0 
        AND RIGHT_SSE1 > 0 AND LEFT_SSE1 > 0
      GROUP BY 1
    );
  
  %end;
  
  execute by &connection (
    CALL UT_DROP_TABLE_IF_EXISTS(%str(%')&rules_out.%str(%'))
  );

  execute by &connection (
    CREATE TEMP TABLE &rules_out AS
    SELECT
      A.NODE AS PARENT_NODE,
      A.VAR,
      A.VAL,
      1 AS COND,
      NEXT VALUE FOR &sequence AS NODE,
      LEFT_SCORE AS SCORE,
      LEFT_POP AS POP
    FROM
      CUMSUM A
    JOIN
      BEST B
    ON
      A.NODE = B.NODE
      AND A.TEST = B.TEST

    UNION ALL    

    SELECT 
      A.NODE AS PARENT_NODE,
      A.VAR,
      A.VAL,
      0 AS COND,
      NEXT VALUE FOR &sequence AS NODE,
      RIGHT_SCORE AS SCORE,
      RIGHT_POP AS POP
    FROM
      CUMSUM A
    JOIN
      BEST B
    ON
      A.NODE = B.NODE
      AND A.TEST = B.TEST
  );

%mend node_split;
