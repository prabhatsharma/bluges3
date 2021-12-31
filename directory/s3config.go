package directory

import (
	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
)

func GetS3Config() bluge.Config {

	return bluge.DefaultConfigWithDirectory(func() index.Directory {
		return NewS3Directory("zinc1", "index1")
	})
}
