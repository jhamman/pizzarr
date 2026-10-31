# IcechunkStore: read-only Icechunk repositories through the zarrs backend.
#
# Fixtures: inst/extdata/fixtures/icechunk/icechunk.zip, written by
# generate-icechunk.py in the same directory (icechunk-python 2.3). The
# expected values below follow the formulas in that script.

has_icechunk <- function() {
  isTRUE(.pizzarr_env$zarrs_available) &&
    "icechunk" %in% zarrs_compiled_features()
}

skip_if_no_icechunk <- function() {
  skip_if(!has_icechunk(), "icechunk feature not compiled")
}

ic_fixtures <- local({
  dir <- NULL
  function() {
    if (is.null(dir)) {
      zip <- system.file("extdata/fixtures/icechunk/icechunk.zip",
                         package = "pizzarr")
      dir <<- tempfile("icechunk_fixtures_")
      dir.create(dir)
      utils::unzip(zip, exdir = dir)
    }
    dir
  }
})

ic_path <- function(name) file.path(ic_fixtures(), name)

ic_refs <- function() jsonlite::read_json(ic_path("refs.json"))

ic_basic <- function(...) IcechunkStore$new(ic_path("basic.icechunk"), ...)

# Read a whole array through the zarrs path only (no R-native fallback).
zarrs_read_all <- function(store, path) {
  meta <- zarrs_open_array_metadata(store$get_store_identifier(), path)
  ranges <- lapply(meta$shape, function(n) c(0L, as.integer(n)))
  res <- zarrs_get_subset(store$get_store_identifier(), path, ranges, NULL)
  array(res$data, dim = res$shape)
}

base_2d <- outer(0:5 * 10, 0:4, "+")

test_that("IcechunkStore errors clearly without the icechunk feature", {
  skip_if(has_icechunk(), "icechunk feature compiled")
  expect_error(
    IcechunkStore$new(tempdir()),
    "needs pizzarr's zarrs backend built with the 'icechunk' feature"
  )
})

test_that("icechunk is listed in compiled features", {
  skip_if_no_icechunk()
  expect_true("icechunk" %in% pizzarr_compiled_features())
})

test_that("IcechunkStore opens a group with zarr_open_group and zarr_open", {
  skip_if_no_icechunk()
  store <- ic_basic()
  expect_s3_class(store, "IcechunkStore")
  expect_match(store$get_store_identifier(),
               "^icechunk\\+file:///.*basic\\.icechunk@branch:main$")
  expect_false(store$is_writeable())

  g <- zarr_open_group(store, mode = "r")
  expect_s3_class(g, "ZarrGroup")
  expect_equal(g$get_attrs()$to_list()$title, "pizzarr icechunk fixture")

  g2 <- zarr_open(store)
  expect_s3_class(g2, "ZarrGroup")

  sub <- g$get_item("sub")
  expect_s3_class(sub, "ZarrGroup")
  expect_equal(sub$get_attrs()$to_list()$level, 1)
  expect_equal(as.vector(sub$get_item("arr")$get_item("...")$data), c(7L, 8L, 9L))
})

test_that("IcechunkStore lists children", {
  skip_if_no_icechunk()
  store <- ic_basic()
  root <- store$listdir()
  expect_true(all(c("zarr.json", "sub", "sharded", "int32_zstd") %in% root))
  expect_setequal(store$listdir("sub"), c("arr", "zarr.json"))
})

test_that("IcechunkStore reads every dtype and codec", {
  skip_if_no_icechunk()
  store <- ic_basic()
  g <- zarr_open_group(store, mode = "r")
  cases <- list(
    int8_plain = list(type = "integer", add = 0),
    int16_gzip = list(type = "integer", add = 0),
    int32_zstd = list(type = "integer", add = 0),
    int64_blosc = list(type = "double", add = 0),
    uint8_plain = list(type = "integer", add = 0),
    uint16_gzip = list(type = "integer", add = 0),
    uint32_zstd = list(type = "double", add = 0),
    uint64_blosc = list(type = "double", add = 0),
    float32_zstd = list(type = "double", add = 0.5),
    float64_gzip = list(type = "double", add = 0.5)
  )
  for (name in names(cases)) {
    expected <- base_2d + cases[[name]]$add
    # uint64 warns that bit64 is signed; the fixture values are small.
    x <- suppressWarnings(g$get_item(name))$get_item("...")$data
    expect_type(x, cases[[name]]$type)
    expect_equal(as.vector(x), as.vector(expected), info = name)
    expect_equal(dim(x), c(6L, 5L), info = name)
    expect_equal(as.vector(zarrs_read_all(store, name)), as.vector(expected),
                 info = name)
  }

  expect_equal(as.vector(g$get_item("bool")$get_item("...")$data),
               (0:6) %% 3 == 0)
  # pizzarr's R-level metadata parser does not know crc32c, so read this one
  # through zarrs directly.
  expect_equal(zarrs_read_all(store, "float64_bigendian_crc32c"),
               matrix(0:11 / 4, 4, 3, byrow = TRUE))
  expect_equal(g$get_item("int32_transpose")$get_item("...")$data,
               matrix(0:23, 4, 6, byrow = TRUE), ignore_attr = TRUE)
})

test_that("IcechunkStore reads slices and falls back for strided selections", {
  skip_if_no_icechunk()
  a <- zarr_open_array(ic_basic(), path = "int32_zstd", mode = "r")
  expect_equal(a$get_item(list(slice(2, 4), slice(1, 3)))$data,
               base_2d[2:4, 1:3], ignore_attr = TRUE)
  # step > 1 is not eligible for zarrs: read key by key in R.
  expect_equal(a$get_item(list(slice(1, 6, 2), "..."))$data,
               base_2d[c(1, 3, 5), ], ignore_attr = TRUE)
})

test_that("IcechunkStore reads missing chunks as the fill value", {
  skip_if_no_icechunk()
  store <- ic_basic()
  expected <- matrix(-1L, 8, 8)
  expected[1:4, 1:4] <- matrix(0:15, 4, 4, byrow = TRUE)
  expect_equal(zarrs_read_all(store, "missing"), expected)
  a <- zarr_open_array(store, path = "missing", mode = "r")
  expect_equal(a$get_item("...")$data, expected, ignore_attr = TRUE)
})

test_that("IcechunkStore reads sharded arrays, including sparse shards", {
  skip_if_no_icechunk()
  store <- ic_basic()
  expect_equal(zarrs_read_all(store, "sharded"),
               matrix(as.double(0:63), 8, 8, byrow = TRUE))
  id <- store$get_store_identifier()
  part <- zarrs_get_subset(id, "sharded", list(c(1L, 3L), c(5L, 7L)), NULL)
  expect_equal(array(part$data, part$shape),
               matrix(as.double(0:63), 8, 8, byrow = TRUE)[2:3, 6:7])

  sparse <- matrix(NaN, 8, 8)
  sparse[1:2, 1:2] <- matrix(c(1, 2, 3, 4), 2, 2, byrow = TRUE)
  expect_equal(zarrs_read_all(store, "sharded_sparse"), sparse)
  # Part of a written shard, and part of a never-written shard: these use
  # partial reads, which zarrs_icechunk 0.6.0 turns into errors without
  # pizzarr's wrapper.
  p1 <- zarrs_get_subset(id, "sharded_sparse", list(c(1L, 3L), c(1L, 3L)), NULL)
  expect_equal(array(p1$data, p1$shape), sparse[2:3, 2:3])
  p2 <- zarrs_get_subset(id, "sharded_sparse", list(c(5L, 7L), c(4L, 6L)), NULL)
  expect_true(all(is.nan(p2$data)))
})

test_that("IcechunkStore reads a branch, a tag and a snapshot", {
  skip_if_no_icechunk()
  refs <- ic_refs()
  version_at <- function(...) {
    a <- zarr_open_array(ic_basic(...), path = "version", mode = "r")
    as.vector(a$get_item("...")$data)
  }
  expect_equal(version_at(), rep(2L, 4))
  expect_equal(version_at(branch = "main"), rep(2L, 4))
  expect_equal(version_at(branch = "dev"), rep(3L, 4))
  expect_equal(version_at(tag = "v1"), rep(1L, 4))
  expect_equal(version_at(snapshot = refs$v1), rep(1L, 4))
  expect_equal(version_at(snapshot = refs$dev), rep(3L, 4))

  s <- ic_basic(tag = "v1")
  expect_match(s$get_store_identifier(), "@tag:v1$")
})

test_that("IcechunkStore errors on unknown refs", {
  skip_if_no_icechunk()
  expect_error(ic_basic(branch = "nope"), "IcechunkRefNotFound.*branch \"nope\"")
  expect_error(ic_basic(tag = "nope"), "IcechunkRefNotFound.*tag \"nope\"")
  expect_error(ic_basic(snapshot = "not-a-snapshot"), "not a valid snapshot id")
  expect_error(ic_basic(snapshot = "AAAAAAAAAAAAAAAAAAAA"), "IcechunkRefNotFound")
  expect_error(ic_basic(branch = "main", tag = "v1"), "at most one")
})

test_that("IcechunkStore errors on bad URLs and options", {
  skip_if_no_icechunk()
  expect_error(IcechunkStore$new("ftp://host/repo"), "unsupported URL scheme")
  expect_error(IcechunkStore$new(file.path(tempdir(), "no-such-repo")),
               "no repository directory")
  expect_error(IcechunkStore$new(tempdir()), "IcechunkOpen")
  expect_error(ic_basic(storage_options = list(regoin = "x")), "unknown field")
  expect_error(ic_basic(storage_options = list("x")), "named list")
  expect_error(
    IcechunkStore$new("az://container/repo"),
    "need storage_options\\$account"
  )
})

test_that("IcechunkStore is read-only", {
  skip_if_no_icechunk()
  store <- ic_basic()
  expect_error(store$set_item("x", as.raw(1)), "read-only")
  a <- zarr_open_array(store, path = "int32_zstd", mode = "r")
  expect_error(a$set_item("...", base_2d))
  expect_false(can_use_zarrs_write(NULL, store))
})

test_that("IcechunkStore reads virtual chunks from an authorized location", {
  skip_if_no_icechunk()
  prefix <- paste0("file://", normalizePath(ic_path("virtual-source")), "/")
  store <- IcechunkStore$new(
    ic_path("virtual.icechunk"),
    virtual_chunk_containers = setNames(list(list(name = "fixtures")), prefix)
  )
  expect_match(store$get_store_identifier(), "#[0-9a-f]{16}$")
  expect_equal(as.vector(zarrs_read_all(store, "v")), 0:7 * 1.5)
  a <- zarr_open_array(store, path = "v", mode = "r")
  expect_equal(as.vector(a$get_item("...")$data), 0:7 * 1.5)
})

test_that("IcechunkStore refuses unauthorized virtual chunk locations", {
  skip_if_no_icechunk()
  store <- IcechunkStore$new(ic_path("virtual.icechunk"))
  a <- zarr_open_array(store, path = "v", mode = "r")
  expect_error(a$get_item("..."), "unauthorized virtual chunk location")
  expect_error(
    zarrs_get_subset(store$get_store_identifier(), "v", list(c(0L, 8L)), NULL),
    "unauthorized virtual chunk location"
  )
})

test_that("IcechunkStore reopens after zarrs_close_store", {
  skip_if_no_icechunk()
  store <- ic_basic()
  id <- store$get_store_identifier()
  store$close()
  expect_error(zarrs_get_key(id, "zarr.json"), "session is not open")
  expect_true(store$contains_item("zarr.json"))
  expect_equal(zarrs_read_all(store, "sub/arr"), array(c(7L, 8L, 9L)))
})
