#' dbConnection class
#' @title dbConnection class
#' @doctype class
#' @description connect using DBI or pool
#' @field DBIconn - connection using DBI (NULL if not connected)
#' @field poolconn - connection using pool (NULL is not connected)
#' @field conn() - connection using whichever connection is available
#'
#' @section Methods:
#' \describe{
#' \item{\strong{connect} connect to database}
#' \item{\strong{close} close connection to database}
#' \item{\strong{dbSendQuery} send query (statement) to connection}
#' \item{\strong{dbGetQuery} send query to connection}
#' }
#'
#' @examples
#' dbConnection$new()   # creates new object
#'
#' dbConnection$connect(drv = RSQLite::SQLite(),
#'                      dbname = "mydatabase.sqlite",
#'                      usepool = FALSE)
#'                      # sets $DBIconn or $poolcon to connection
#'                      # both set to NULL if fail/warn
#'                      # by default 'usepool' is TRUE and attempts
#'                      # to use 'pool' package (if available)
#'                      # if 'usepool' is FALSE, then do not use
#'                      # the 'pool' package, so use 'DBI' instead
#'
#' dbConnection$close() # closes all connections
#'
#' query <- "UPDATE Users SET Name = ? WHERE id = ?"
#' data_for_sql <- list(c("John"), id)
#' dbConnection$dbSendQuery(query, data_for_sql)
#'                      # send parametized 'SQL query'
#'                      # with 'data_for_sql'
#'
#' @export
dbConnection <-
  R6::R6Class("dbConnection",
              public = list(
                DBIconn = NULL, # connection using DBI
                poolconn = NULL, # connection using pool
                connect = function (drv, ..., usepool = TRUE) {
                  # connect to database, using 'pool' package if available and desired
                  # accepts standard database opening parameters (drv, ...)
                  # and 'usepool', whether to try to open the pool
                  # by default, try to open using pool (TRUE)
                  #
                  if (usepool & requireNamespace("pool", quietly = TRUE)) {
                    # if use of pool is requested, and 'pool' package is available
                    tryCatch(self$poolconn <- pool::dbPool(drv, ...),
                             error = function(e) {NULL},
                             warning = function(w) {NULL})
                  } else {
                    tryCatch(self$DBIconn <- DBI::dbConnect(drv, ...),
                             error = function(e) {NULL},
                             warning = function(w) {NULL})
                  }
                  invisible(self)
                },
                close = function() {
                  # close any open connections
                  if (!is.null(self$DBIconn)) {
                    DBI::dbDisconnect(self$DBIconn)
                    self$DBIconn <- NULL
                  }
                  if (!is.null(self$poolconn)) {
                    pool::poolClose(self$poolconn)
                    self$poolconn <- NULL
                  }
                  invisible(self)
                },
                is_open = function() {
                  # is any connection open?
                  # returns TRUE or FALSE
                  return(!is.null(self$DBIconn) | !is.null(self$poolconn))
                },
                finalize = function() {
                  # object being destroyed/removed
                  # close all open connections
                  self$close()
                },
                conn = function(value) {
                  # return open connection
                  if (missing(value)) {
                    # called without argument, which is the default
                    if (!is.null(self$DBIconn)) {
                      return(self$DBIconn)
                    }
                    if (!is.null(self$poolconn)) {
                      return(self$poolconn)
                    }
                    return(NULL) # no open connections
                  } else {
                    stop("Can't set `$conn`, use $connect to open a database", call. = FALSE)
                  }
                },
                dbSendQuery = function(query, data_for_sql) {
                  # send SQL statement/query to active connection,
                  # either DBI or pool
                  # @param query - the SQL query
                  # @data_for_sql - the data
                  # returns result 'rs'

                  rs <- NULL # by default, there is no result

                  if (!is.null(self$DBIconn)) {
                    q <- DBI::dbSendQuery(self$DBIconn, query)
                    # parameterized query can handle apostrophes etc.
                    DBI::dbBind(q, data_for_sql)
                    tryCatch(rs <- DBI::dbFetch(q),
                             warning = function(w) {})
                    # for statements, rather than queries, we don't need to dbFetch(q)
                    # update database
                    # the tryCatch suppresses the warning:
                    # Warning message:
                    #  In result_fetch(res@ptr, n = n) :
                    #   Don't need to call dbFetch() for statements, only for queries
                    DBI::dbClearResult(q)
                  }
                  if (!is.null(self$poolconn)) {
                    temp_connection <- pool::poolCheckout(self$poolconn)
                    # can't write with the pool
                    q <- DBI::dbSendQuery(temp_connection, query)
                    # parameterized query can handle apostrophes etc.
                    DBI::dbBind(q, data_for_sql)
                    tryCatch(rs <- DBI::dbFetch(q),
                             warning = function(w) {})
                    # for statements, rather than queries, we don't need to dbFetch(q)
                    # update database
                    # the tryCatch suppresses the warning:
                    # Warning message:
                    #  In result_fetch(res@ptr, n = n) :
                    DBI::dbClearResult(q)
                    pool::poolReturn(temp_connection)
                  }
                  return(rs)
                },
                dbGetQuery = function(query) {
                  # send SQL query statement to active connection,
                  # either DBI or pool
                  # dbGetQuery is a combination of
                  #  dbSendQuery, dbFetch and dbClearResult
                  # @param query - the SQL query
                  if (!is.null(self$DBIconn)) {
                    rs <- DBI::dbGetQuery(self$DBIconn, query)
                  }
                  if (!is.null(self$poolconn)) {
                    rs <- DBI::dbGetQuery(self$poolconn, query)
                  }
                  return(rs)
                }
              ))
