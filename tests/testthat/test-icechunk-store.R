skip_if_no_icechunk <- function() {
  skip_if_not("icechunk" %in% pizzarr_compiled_features(), "pizzarr built without icechunk feature")
  skip_if_not_installed("icechunk")
  skip_if_not(identical(icechunk::ic_icechunk_version(), zarrs_icechunk_version()),
              "icechunk package and pizzarr built against different icechunk versions")
}

test_that("IcechunkStore: create, write, hand back, commit, read", {
  skip_if_no_icechunk()
  repo <- icechunk::ic_repo_create(icechunk::ic_storage_local(withr::local_tempdir()))
  s <- icechunk::ic_session_writable(repo, "main")
  st <- IcechunkStore$new(s)
  expect_true(st$is_writeable())
  expect_match(st$get_store_identifier(), "^icechunk://")

  g <- zarr_open_group(st, mode = "w", zarr_format = 3L)
  a <- g$create_dataset("x", shape = c(20L, 30L), chunks = c(10L, 10L),
                        dtype = "<f8", fill_value = -1)
  vals <- array(as.double(seq_len(200)), c(10, 20))
  a$set_item(list(slice(1L, 10L), slice(1L, 20L)), vals)
  expect_true("x" %in% st$listdir())

  b <- st$session_bytes()
  expect_s3_class(b, "icechunk_session_bytes")
  icechunk::ic_session_merge(s, b)
  st$close()
  expect_error(st$get_item("zarr.json"), "closed")
  icechunk::ic_commit(s, "from pizzarr")

  st2 <- IcechunkStore$new(icechunk::ic_session_readonly(repo, branch = "main"))
  on.exit(st2$close(), add = TRUE)
  expect_false(st2$is_writeable())
  got <- zarr_open(st2, mode = "r", path = "x")$get_item("...")$data
  expect_equal(dim(got), c(20L, 30L))
  expect_equal(got[1:10, 1:20], vals)
  # chunks never written read as fill value
  expect_true(all(got[11:20, ] == -1))
  expect_true(all(got[, 21:30] == -1))
  expect_false(st2$contains_item("x/.zarray"))
  expect_error(st2$set_item("y/zarr.json", charToRaw("{}")), "read-only")
})

test_that("IcechunkStore rejects bytes that are not a session", {
  skip_if_no_icechunk()
  expect_error(IcechunkStore$new(charToRaw("nope")), "ICR1")
})
