package constants

// These values will be injected at build time, DO NOT EDIT.

// BackendVersion 当前后端版本号
var BackendVersion = "4.19.9"

// LastCommit 最后commit id
var LastCommit = "000000"

const (
	APIPrefix      = "/api/v4"
	APIPrefixSlave = "/api/v4/slave"
	CrHeaderPrefix = "X-Cr-"
)

const CloudreveScheme = "cloudreve"

type (
	FileSystemType string
)

const (
	FileSystemMy           = FileSystemType("my")
	FileSystemShare        = FileSystemType("share")
	FileSystemTrash        = FileSystemType("trash")
	FileSystemSharedWithMe = FileSystemType("shared_with_me")
	FileSystemUnknown      = FileSystemType("unknown")
)
