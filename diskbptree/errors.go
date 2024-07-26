package diskbptree

import "errors"

var KEY_NOT_FOUND_ERROR = errors.New("Key not found")
var KEY_ALREADY_EXISTS_ERROR = errors.New("Key already exists")
var INVALID_KEY_ERROR = errors.New("Invalid key")
var INVALID_DATA_ERROR = errors.New("Invalid data")
var KEY_SIZE_TOO_LARGE = errors.New("The key size is too large.")
var INVALID_KEY_SIZE_ERROR = errors.New("Invalid key size. All keys must have the same length")
var INVALID_KEY_INDEX_ERROR = errors.New("Invalid key index")
var INVALID_POINTER_INDEX_ERROR = errors.New("Invalid pointer index")
var TYPE_CONVERSION_ERROR = errors.New("Error while converting interface to type")
