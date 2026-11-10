test_that("ShuffleCodec matches numcodecs byte layout and round-trips", {
  x <- as.raw(1:12)                       # three 4-byte elements
  sh <- ShuffleCodec$new(elementsize = 4L)
  expect_identical(sh$encode(x), as.raw(c(1, 5, 9, 2, 6, 10, 3, 7, 11, 4, 8, 12)))
  expect_identical(sh$decode(sh$encode(x)), x)
  y <- as.raw(1:14)                       # trailing bytes stay in place
  expect_identical(tail(sh$encode(y), 2), as.raw(13:14))
  expect_identical(sh$decode(sh$encode(y)), y)
  expect_identical(ShuffleCodec$new(1L)$encode(x), x)
  expect_identical(sh$get_config()$elementsize, jsonlite::unbox(4L))
})

test_that("v3 numcodecs.shuffle + numcodecs.zlib arrays decode in R", {
  vals <- as.integer(c(-999, 0:10, 2000L, -5L, 31000L, 7L))   # 16 int16
  raw_c <- writeBin(vals, raw(), size = 2, endian = "little")
  enc <- memCompress(ShuffleCodec$new(2L)$encode(raw_c), type = "gzip")
  d <- tempfile(); dir.create(file.path(d, "a", "c", "0"), recursive = TRUE)
  writeLines('{"zarr_format":3,"node_type":"group","attributes":{}}', file.path(d, "zarr.json"))
  writeLines('{"zarr_format":3,"node_type":"array","shape":[4,4],"data_type":"int16",
    "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4,4]}},
    "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
    "fill_value":-999,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},
    {"name":"numcodecs.shuffle","configuration":{"elementsize":2}},
    {"name":"numcodecs.zlib","configuration":{"level":1}}],"attributes":{}}', file.path(d, "a", "zarr.json"))
  writeBin(enc, file.path(d, "a", "c", "0", "0"))
  st <- DirectoryStore$new(d)
  got <- zarr_open(st, mode = "r", path = "a")$get_item("...")$data
  expect_equal(as.vector(t(got)), vals)
})
