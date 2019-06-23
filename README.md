# dbConnection

install with `devtools::install_github('DavidPatShuiFong/dbConnection')`

R6 'object-oriented' objects for generic connection to databases. 

Preferentially uses 'pool' package, but does not depend on 'pool'.  Falls back to 'DBI' connections either if 'pool' is not available, or specifically if 'usepool == FALSE'. 

Exports (makes available) a single R6 class definition, 'dbConnection'.

dbConnection class

## Fields

**DBIconn** : connection using DBI (NULL if not connected)

**poolconn** : connection using pool (NULL is not connected)

**conn()** : connection using whichever connection is available


## Methods

**connect** : connect to database

**close** : close connection to database

**dbSendQuery** : send query (statement) to connection


## Examples

```
dbConnection$new()   # creates new object

dbConnection$connect(drv = RSQLite::SQLite(),
                     dbname = "mydatabase.sqlite",
                     usepool = FALSE)
                     # sets $DBIconn or $poolcon to connection
                     # both set to NULL if fail/warn
                     # by default 'usepool' is TRUE and attempts
                     # to use 'pool' package (if available)
                     # if 'usepool' is FALSE, then do not use
                     # the 'pool' package, so use 'DBI' instead
                     # $connect arguments are defined as (drv, ... , usepool = TRUE)
                     
                     
dbConnection$close() # closes all connections

query <- "UPDATE Users SET Name = ? WHERE id = ?"
data_for_sql <- list(c("John"), id)
dbConnection$dbSendQuery(query, data_for_sql)
                     # send parametized 'SQL query'
                     # with 'data_for_sql'`
```
