#' IcechunkStore for Zarr
#' @title IcechunkStore Class
#' @docType class
#' @description
#' Store backed by an Icechunk session handed over from the `icechunk`
#' package. pizzarr never opens repositories, commits, or authorises virtual
#' chunk containers: the `icechunk` package does that, then passes the
#' session to pizzarr as bytes (`icechunk::ic_session_bytes()`). pizzarr
#' rebuilds the session inside its zarrs backend, so reads and writes run
#' through the same Rust code paths as every other zarrs store.
#'
#' Writes accumulate in pizzarr's copy of the session. Hand them back with
#' `$session_bytes()` and `icechunk::ic_session_merge()`, then commit with
#' `icechunk::ic_commit()`.
#'
#' Requires pizzarr built with the `icechunk` Cargo feature
#' (`PIZZARR_FEATURES=s3,gcs,icechunk`) and an `icechunk` package built
#' against the same icechunk crate version.
#'
#' @format [R6::R6Class] inheriting from [Store].
#' @family Store classes
#' @rdname IcechunkStore
#' @examples
#' \dontrun{
#' library(icechunk)
#' repo <- ic_repo_open(ic_storage_local("path/to/repo"))
#' s <- ic_session_writable(repo, "main")
#' store <- IcechunkStore$new(s)
#' z <- zarr_open(store, path = "sst")
#' z$get_item("...")
#' # write, then hand changes back and commit
#' ic_session_merge(s, store$session_bytes())
#' ic_commit(s, "update from pizzarr")
#' store$close()
#' }
#' @export
IcechunkStore <- R6::R6Class("IcechunkStore",
  inherit = Store,
  private = list(
    key = NULL,
    snapshot = NULL,
    # Icechunk is Zarr v3 only, and zarrs_icechunk errors (rather than
    # returning None) on some missing keys, so pizzarr's v2 probes
    # (.zarray, .zgroup, ...) never reach Rust and "not found" maps to NULL.
    fetch = function(key) {
      private$check_open()
      if (grepl("(^|/)[.]z(array|group|attrs|metadata)$", key)) return(NULL)
      tryCatch(zarrs_get_key(private$key, key), error = function(e) {
        if (grepl("not found", conditionMessage(e), ignore.case = TRUE)) NULL
        else stop(e)
      })
    },
    check_open = function() {
      if (is.null(private$key)) stop("IcechunkStore is closed")
    },
    finalize = function() {
      if (!is.null(private$key)) zarrs_icechunk_close_session(private$key)
    }
  ),
  public = list(
    #' @description
    #' Create an `IcechunkStore`.
    #' @param session An `icechunk_session` (from the `icechunk` package) or
    #'   session bytes (raw vector with the `ICR1` header, from
    #'   `icechunk::ic_session_bytes()`).
    #' @param read_only Logical or `NULL`. `NULL` takes it from the session
    #'   when an `icechunk_session` is given, else assumes writable (icechunk
    #'   itself refuses writes to a read-only session).
    initialize = function(session, read_only = NULL) {
      super$initialize()
      if (!.pizzarr_env$zarrs_available ||
          !("icechunk" %in% pizzarr_compiled_features())) {
        stop("IcechunkStore requires pizzarr built with the 'icechunk' feature ",
             "(install with PIZZARR_FEATURES=s3,gcs,icechunk)")
      }
      if (inherits(session, "icechunk_session")) {
        if (!requireNamespace("icechunk", quietly = TRUE)) {
          stop("the 'icechunk' package is needed to read an icechunk_session")
        }
        if (is.null(read_only)) {
          read_only <- isTRUE(icechunk::ic_session_info(session)$read_only)
        }
        session <- icechunk::ic_session_bytes(session)
      }
      if (!is.raw(session)) {
        stop("session must be an icechunk_session or raw session bytes")
      }
      private$key <- zarrs_icechunk_open_session(unclass(session))
      private$snapshot <- sub("^icechunk://([^/]+)/.*$", "\\1", private$key)
      private$store_version <- 3
      private$writeable <- !isTRUE(read_only)
      private$erasable <- private$writeable
      private$listable <- TRUE
    },
    #' @description
    #' Get an item from the store.
    #' @param key The item key.
    #' @return The item data in a vector of type raw.
    get_item = function(key) {
      v <- private$fetch(key)
      if (is.null(v)) stop("KeyError: ", key)
      v
    },
    #' @description
    #' Set an item in the store (accumulates in the session; not committed).
    #' @param key The item key.
    #' @param value The item value as a vector of type raw.
    #' @return `NULL` (called for side effects).
    set_item = function(key, value) {
      private$check_open()
      if (!private$writeable) stop("IcechunkStore is read-only")
      zarrs_set_key(private$key, key, value)
      invisible(NULL)
    },
    #' @description
    #' Determine whether the store contains an item.
    #' @param key The item key.
    #' @return A boolean value.
    contains_item = function(key) {
      !is.null(private$fetch(key))
    },
    #' @description
    #' Delete an item from the session (not committed).
    #' @param key The item key.
    #' @return `NULL` (called for side effects).
    delete_item = function(key) {
      private$check_open()
      if (!private$writeable) stop("IcechunkStore is read-only")
      zarrs_erase_key(private$key, key)
      invisible(NULL)
    },
    #' @description
    #' Remove everything under `path` from the session (not committed).
    #' @param path Character path (`NA` or `""` for the root).
    #' @return `NULL` (called for side effects).
    rmdir = function(path = NA) {
      private$check_open()
      if (!private$writeable) stop("IcechunkStore is read-only")
      prefix <- if (is_na(path) || !nzchar(path)) "" else
        paste0(sub("/+$", "", normalize_storage_path(path)), "/")
      zarrs_erase_prefix(private$key, prefix)
      invisible(NULL)
    },
    #' @description
    #' List entries one level below `path`.
    #' @param path Character path.
    #' @return `character()` vector of entry names.
    listdir = function(path = NA) {
      private$check_open()
      prefix <- if (is_na(path) || !nzchar(path)) "" else
        paste0(sub("/+$", "", normalize_storage_path(path)), "/")
      r <- zarrs_list_dir(private$key, prefix)
      ents <- c(r$keys, sub("/$", "", r$prefixes))
      sort(unique(basename(ents)))
    },
    #' @description
    #' Serialise the session, including writes made through this store, for
    #' `icechunk::ic_session_merge()`.
    #' @return Raw vector of class `icechunk_session_bytes`.
    session_bytes = function() {
      private$check_open()
      structure(zarrs_icechunk_session_bytes(private$key),
                class = "icechunk_session_bytes")
    },
    #' @description
    #' Release the session held by pizzarr.
    #' @return `NULL` (invisibly).
    close = function() {
      if (!is.null(private$key)) {
        zarrs_icechunk_close_session(private$key)
        private$key <- NULL
      }
      invisible(NULL)
    },
    #' @description
    #' Return the synthetic `icechunk://` store key for zarrs dispatch.
    #' @return A character string.
    get_store_identifier = function() {
      private$check_open()
      private$key
    },
    #' @description
    #' Print a human-readable summary of the store.
    #' @param ... Ignored.
    #' @return `self` (invisibly).
    print = function(...) {
      cat("<IcechunkStore> snapshot ", private$snapshot,
          if (is.null(private$key)) " (closed)" else
            if (private$writeable) " (writable)" else " (read-only)",
          "\n", sep = "")
      invisible(self)
    }
  )
)
