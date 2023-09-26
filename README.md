# utl-select-the-first-two-observations-from-each-group-wps-r-python-sql
Select the first two observations from each group wps r python sql
    %let pgm=utl-select-the-first-two-observations-from-each-group-wps-r-python-sql;

    Select the first two observations from each group wps r python sql

    github
    https://tinyurl.com/bddeh5ec
    https://github.com/rogerjdeangelis/utl-select-the-first-two-observations-from-each-group-wps-r-python-sql

    stackoverflow R
    https://tinyurl.com/4jwy5hv2
    https://stackoverflow.com/questions/77181502/how-to-keep-two-lots-when-matching-on-unique-combinations-across-multiple-r-data


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*                                                                                                                        */
    /*       INPUT                            PROCESS                                                  OUTPUT                 */
    /*                                                                                                                        */
    /* VAR_1  VAR_2  VAR_3  LOT         Select top 2 by var_1-var_3              VAR_1    VAR_2    VAR_3    LOT    PARTITION  */
    /*                                                                                                                        */
    /*   A      B      E     X          set sd1.have;                               A        B        E       X         1     */
    /*   A      B      E     Y          by Var_1 Var_2 Var_3 notsorted;             A        B        E       Y         2     */
    /*   A      B      E     Z DROP                                                                                           */
    /*                                  partition=ifn(first.var_3,1,partition+1);                                             */
    /*   A      C      E     X                                                      A        C        E       X         1     */
    /*   A      C      E     Y          if partition >  2 then do;                  A        C        E       Y         2     */
    /*   A      C      E     Z DROP           delete;                                                                         */
    /*                                        partition=0;                                                                    */
    /*   A      C      D     X          end;                                        A        C        D       X         1     */
    /*   A      C      D     Y                                                      A        C        D       Y         2     */
    /*   A      C      D     Z DROP                                                                                           */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                                                _
    / | __      ___ __  ___   _ __   ___    ___  __ _| |
    | | \ \ /\ / / `_ \/ __| | `_ \ / _ \  / __|/ _` | |
    | |  \ V  V /| |_) \__ \ | | | | (_) | \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/ |_| |_|\___/  |___/\__, |_|
                 |_|                               |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
      input (Var_1 Var_2 Var_3 Lot) ($2.);
    cards4;
    A B E X
    A B E Y
    A B E Z
    A B E L
    A C E X
    A C E Y
    A C E Z
    A C D X
    A C D Y
    A C D Z
    ;;;;
    run;quit;

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    data sd1.want;
      retain partition 0;
      set sd1.have;
      by Var_1 Var_2 Var_3 notsorted;
      partition=ifn(first.var_3,1,partition+1);
      if partition >  2 then do; delete; partition=0; end;
    run;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  SD1.WANT total obs=6                                                                                                  */
    /*                                                                                                                        */
    /*  Obs    PARTITION    VAR_1    VAR_2    VAR_3    LOT                                                                    */
    /*                                                                                                                        */
    /*   1         1          A        B        E       X                                                                     */
    /*   2         2          A        B        E       Y                                                                     */
    /*   3         1          A        C        E       X                                                                     */
    /*   4         2          A        C        E       Y                                                                     */
    /*   5         1          A        C        D       X                                                                     */
    /*   6         2          A        C        D       Y                                                                     */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                                   _
    |___ \  __      ___ __  ___   ___  __ _| |
      __) | \ \ /\ / / `_ \/ __| / __|/ _` | |
     / __/   \ V  V /| |_) \__ \ \__ \ (_| | |
    |_____|   \_/\_/ | .__/|___/ |___/\__, |_|
                     |_|                 |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    options validvarname=any;
    proc sql;
      create
        table sd1.want as
      select
         Var_1
        ,Var_2
        ,Var_3
        ,Lot
        ,row_number - min(row_number)+1 as partition
     from
         (select *, monotonic() as row_number from
          (select *, max(lot) as delete from sd1.have group by Var_1, Var_2, Var_3 ))
      group
        by Var_1, Var_2, Var_3
      having
        (row_number - min(row_number)+1) <= 2
    ;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  SD1.WANT total obs=6                                                                                                  */
    /*                                                                                                                        */
    /*  Obs    PARTITION    VAR_1    VAR_2    VAR_3    LOT                                                                    */
    /*                                                                                                                        */
    /*   1         1          A        B        E       X                                                                     */
    /*   2         2          A        B        E       Y                                                                     */
    /*   3         1          A        C        E       X                                                                     */
    /*   4         2          A        C        E       Y                                                                     */
    /*   5         1          A        C        D       X                                                                     */
    /*   6         2          A        C        D       Y                                                                     */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____
    |___ /  __      ___ __  ___   _ __
      |_ \  \ \ /\ / / `_ \/ __| | `__|
     ___) |  \ V  V /| |_) \__ \ | |
    |____/    \_/\_/ | .__/|___/ |_|
                     |_|
    */

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    library(dplyr);
    have;
    want <- have %>% slice_head(n = 2,by = c(VAR_1, VAR_2, VAR_3));
    want;
    endsubmit;
    import data=sd1.want r=want;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  R                                                                                                                     */
    /*                                                                                                                        */
    /*     VAR_1 VAR_2 VAR_3 LOT                                                                                              */
    /*  1      A     B     E   X                                                                                              */
    /*  2      A     B     E   Y                                                                                              */
    /*  3      A     B     E   Z                                                                                              */
    /*  4      A     B     E   L                                                                                              */
    /*  5      A     C     E   X                                                                                              */
    /*  6      A     C     E   Y                                                                                              */
    /*  7      A     C     E   Z                                                                                              */
    /*  8      A     C     D   X                                                                                              */
    /*  9      A     C     D   Y                                                                                              */
    /*  10     A     C     D   Z                                                                                              */
    /*                                                                                                                        */
    /* WPS                                                                                                                    */
    /*                                                                                                                        */
    /*  Obs    VAR_1    VAR_2    VAR_3    LOT                                                                                 */
    /*                                                                                                                        */
    /*   1       A        B        E       X                                                                                  */
    /*   2       A        B        E       Y                                                                                  */
    /*   3       A        C        E       X                                                                                  */
    /*   4       A        C        E       Y                                                                                  */
    /*   5       A        C        D       X                                                                                  */
    /*   6       A        C        D       Y                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*  _                                           _
    | || |   __      ___ __  ___   _ __   ___  __ _| |
    | || |_  \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
    |__   _|  \ V  V /| |_) \__ \ | |    \__ \ (_| | |
       |_|     \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                      |_|                        |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    options validvarname=any;
    libname sd1 "d:/sd1";

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    library(sqldf);
    want <-sqldf("
       select
          recno % 3 as PARTITION
         ,var_1
         ,var_2
         ,var_3
         ,lot
       from
          (select *, row_number() over (partition by var_1, var_2, var_3) as RECNO from have )
       where
          RECNO < 3
    ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS Proc R System                                                                                                  */
    /*                                                                                                                        */
    /*   PARTITION VAR_1 VAR_2 VAR_3 LOT                                                                                      */
    /* 1         1     A     B     E   X                                                                                      */
    /* 2         2     A     B     E   Y                                                                                      */
    /* 3         1     A     C     D   X                                                                                      */
    /* 4         2     A     C     D   Y                                                                                      */
    /* 5         1     A     C     E   X                                                                                      */
    /* 6         2     A     C     E   Y                                                                                      */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                                     _   _                             _
    | ___|  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
    |___ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                     |_|         |_|    |___/                                |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    proc sql;select max(cnt) into :_cnt from (select count(nam) as cnt from sd1.have group by nam);quit;
    %array(_unq,values=1-&_cnt);
    proc python;
    export data=sd1.have python=have;
    submit;
    print(have);
    from os import path;
    import pandas as pd;
    import numpy as np;
    import pandas as pd;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    want = pdsql('''
       select
          recno % 3 as PARTITION
         ,var_1
         ,var_2
         ,var_3
         ,lot
       from
          (select *, row_number() over (partition by var_1, var_2, var_3) as RECNO from have )
       where
          RECNO < 3
    ''');
    print(want);
    want;
    endsubmit;
    import data=sd1.want python=want;
    run;quit;
    ");

    proc print data=sd1.want;
    run;quit;



    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*                                                                                                                        */
    /*  The PYTHON Procedure                                                                                                  */
    /*                                                                                                                        */
    /*    VAR_1 VAR_2 VAR_3 LOT                                                                                               */
    /*  0    A     B     E   X                                                                                                */
    /*  1    A     B     E   Y                                                                                                */
    /*  2    A     B     E   Z                                                                                                */
    /*  3    A     B     E   L                                                                                                */
    /*  4    A     C     E   X                                                                                                */
    /*  5    A     C     E   Y                                                                                                */
    /*  6    A     C     E   Z                                                                                                */
    /*  7    A     C     D   X                                                                                                */
    /*  8    A     C     D   Y                                                                                                */
    /*  9    A     C     D   Z                                                                                                */
    /*                                                                                                                        */
    /* WPS                                                                                                                    */
    /*                                                                                                                        */
    /*     PARTITION VAR_1 VAR_2 VAR_3 LOT                                                                                    */
    /*  0          1    A     B     E   X                                                                                     */
    /*  1          2    A     B     E   Y                                                                                     */
    /*  2          1    A     C     D   X                                                                                     */
    /*  3          2    A     C     D   Y                                                                                     */
    /*  4          1    A     C     E   X                                                                                     */
    /*  5          2    A     C     E   Y                                                                                     */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
