#' @import pipeR
NULL

#' dbConnection class
#'
#' connect using DBI or pool
#'
#' @title dbConnection class
#' @field DBIconn - connection using DBI (NULL if not connected)
#' @field poolconn - connection using pool (NULL is not connected)
#' @field conn() - connection using whichever connection is available
#'
#' @section Methods:
#' \describe{
#' \item{\strong{connect}}{connect to database}
#' \item{\strong{close}}{close connection to database}
#' \item{\strong{dbSendQuery}}{send query (statement) to connection}
#' \item{\strong{dbGetQuery}}{send query to connection}
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

                  if (self$keep_log) {
                    start_time <- Sys.time()
                    random_id <- stringi::stri_rand_strings(1, 15) # random string

                    self$log_db$dbSendQuery(
                      "INSERT INTO logs (Time, ID, Tag, Query, Data) VALUES (?, ?, ?, ?, ?)",
                      as.list.data.frame(c(as.character(start_time), random_id,
                                           self$log_tag,
                                           query,
                                           paste(sQuote(data_for_sql), collapse = ", ")))
                    )
                  }

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

                  if (self$keep_log) {
                    self$log_db$dbSendQuery(
                      "UPDATE logs SET Duration = ? WHERE Time = ? AND ID = ? AND Tag = ?",
                      as.list.data.frame(c(as.character(Sys.time()-start_time),
                                           as.character(start_time), random_id, self$log_tag))
                    )
                  }

                  return(rs)
                },
                dbGetQuery = function(query) {
                  # send SQL query statement to active connection,
                  # either DBI or pool.
                  # dbGetQuery is a combination of
                  #  dbSendQuery, dbFetch and dbClearResult
                  # @param query - the SQL query

                  if (self$keep_log) {
                    start_time <- Sys.time()
                    random_id <- stringi::stri_rand_strings(1, 15) # random string

                    self$log_db$dbSendQuery(
                      "INSERT INTO logs (Time, ID, Tag, Query) VALUES (?, ?, ?, ?)",
                      as.list.data.frame(c(as.character(start_time), random_id,
                                           self$log_tag,
                                           query))
                    )
                  }

                  if (!is.null(self$DBIconn)) {
                    rs <- DBI::dbGetQuery(self$DBIconn, query)
                  }
                  if (!is.null(self$poolconn)) {
                    rs <- DBI::dbGetQuery(self$poolconn, query)
                  }

                  if (self$keep_log) {

                    self$log_db$dbSendQuery(
                      "UPDATE logs SET Duration = ? WHERE Time = ? AND ID = ? AND Tag = ?",
                      as.list.data.frame(c(as.character(Sys.time()-start_time),
                                           as.character(start_time), random_id, self$log_tag))
                    )
                  }

                  return(rs)
                },
                keep_log = FALSE, # by default, don't keep logs
                log_db = NULL, # another dbConnection object!
                # requirements - has TABLE logs
                #   which has fields Time, ID, Tag, Query, Data and Duration
                log_tag = "", # later, will be a tag for this connection's logs
                open_log_db = function(filename, tag) {
                  # create or open a new log database file (a SQLite file)
                  # and start logging
                  # @param filename - filename of SQLite database
                  #  will create SQLite database if filename doesn't exist
                  # @param tag - string tag to attach to this connection's logs
                  #
                  # @return nothing
                  self$keep_log <- TRUE
                  self$log_tag <- tag

                  self$log_db <- dbConnection::dbConnection$new()
                  # self-referential!


                  if (file.exists(filename)) {
                    # open config database file
                    self$log_db$connect(RSQLite::SQLite(),
                                        dbname = filename)
                  } else {
                    # if the config database doesn't exist,
                    # then create it (note create = TRUE option)
                    self$log_db$connect(RSQLite::SQLite(),
                                        dbname = filename)
                    # create = TRUE not a valid option?
                    # always tries to create file if it doesn't exist
                  }
                  initialize_data_table = function(db, tablename, variable_list ) {
                    # make sure the table in the database has all the right variable headings
                    # allows 'update' of old databases
                    #
                    # input - db : R6 object of configuration database
                    # input - tablename : name of table
                    # input - variable_list : list of variable headings, with variable type
                    #   e.g. list(c("id", "integer"), c("Name", "character"))
                    #
                    # alters table in database directly
                    #
                    # returns - nothing

                    tablenames <- db$conn() %>>% DBI::dbListTables()

                    if (tablename %in% tablenames) {
                      # if table exists in db database
                      columns <- db$conn() %>>% dplyr::tbl(tablename) %>>% colnames()
                      # list of column (variable) names
                      data <- db$conn() %>>% dplyr::tbl(tablename) %>>% dplyr::collect()
                      # get a copy of the table's data
                    } else {
                      # table does not exist, needs to be created
                      columns <- NULL
                      data <- data.frame(NULL)
                    }

                    changed <- FALSE
                    # haven't changed anything yet

                    for (a in variable_list) {
                      if (!(a[[1]] %in% columns)) {
                        # if a required variable name is not in the table
                        data <- data %>>%
                          dplyr::mutate(!!a[[1]] := vector(a[[2]], nrow(data)))
                        # use of !! and := to dynamically specify a[[1]] as a column name
                        # potentially could use data[,a[[1]]] <- ...
                        changed <- TRUE
                      }
                    }
                    if (changed == TRUE) {
                      DBI::dbWriteTable(db$conn(), tablename, data, overwrite = TRUE)
                    }
                  }

                  if (!is.null(self$log_db$conn())) {
                    # check that tables exist in the config file
                    # also create new columns (variables) as necessary
                    initialize_data_table(self$log_db, "logs",
                                          list(c("Time", "character"),
                                               c("ID", "character"),
                                               c("Tag", "character"),
                                               c("Query", "character"),
                                               c("Data", "character"),
                                               c("Duration", "character")))
                    # initialize_data_table will create table and/or ADD 'missing' columns to existing table
                  }

                },
                close_log_db = function() {
                  # turns off logging
                  # and closes the open log database connection
                  self$keep_log <- FALSE
                  self$log_db$close()
                }
              ))
