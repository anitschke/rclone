// Store the parsing of file patterns

package nixplay

import (
	"context"
	"path"
	"regexp"
	"strings"
	"time"

	nixplaytypes "github.com/anitschke/go-nixplay/types"
	"github.com/rclone/rclone/fs"
)

//xxx

// lister describes the subset of the interfaces on Fs needed for the
// file pattern parsing
type lister interface {
	listContainers(ctx context.Context, prefix string, containerType nixplaytypes.ContainerType) (entries fs.DirEntries, err error)
	listPhotos(ctx context.Context, prefix string, containerType nixplaytypes.ContainerType, dir string) (entries fs.DirEntries, err error)
	dirTime() time.Time //xxx needed?
}

// dirPattern describes a single directory pattern
type dirPattern struct {
	re        string         // match for the path
	match     *regexp.Regexp // compiled match
	canUpload bool           // true if can upload here
	canMkdir  bool           // true if can make a directory here
	isFile    bool           // true if this is a file
	// function to turn a match into DirEntries
	toEntries func(ctx context.Context, f lister, prefix string, match []string) (fs.DirEntries, error)
}

// dirPatters is a slice of all the directory patterns
type dirPatterns []dirPattern

// patterns describes the layout of the google photos backend file system.
//
// NB no trailing / on paths
var patterns = dirPatterns{
	{
		re: `^$`,
		toEntries: func(ctx context.Context, f lister, prefix string, match []string) (fs.DirEntries, error) {
			return fs.DirEntries{
				fs.NewDir(prefix+"album", f.dirTime()),
				fs.NewDir(prefix+"playlist", f.dirTime()),
			}, nil
		},
	},
	{
		re: `^album$`,
		toEntries: func(ctx context.Context, f lister, prefix string, match []string) (entries fs.DirEntries, err error) {
			return f.listContainers(ctx, prefix, nixplaytypes.AlbumContainerType)
		},
	},
	{
		re:       `^album/(.+)$`,
		canMkdir: true,
		toEntries: func(ctx context.Context, f lister, prefix string, match []string) (entries fs.DirEntries, err error) {
			return f.listPhotos(ctx, prefix, nixplaytypes.AlbumContainerType, match[1])
		},
	},
	{
		re:        `^album/(.+?)/([^/]+)$`,
		canUpload: true,
		isFile:    true,
	},
	{
		re: `^playlist$`,
		toEntries: func(ctx context.Context, f lister, prefix string, match []string) (entries fs.DirEntries, err error) {
			return f.listContainers(ctx, prefix, nixplaytypes.PlaylistContainerType)
		},
	},
	{
		re:       `^playlist/(.+)$`,
		canMkdir: true,
		toEntries: func(ctx context.Context, f lister, prefix string, match []string) (entries fs.DirEntries, err error) {
			return f.listPhotos(ctx, prefix, nixplaytypes.PlaylistContainerType, match[1])

		},
	},
	{
		re:        `^playlist/(.+?)/([^/]+)$`,
		canUpload: true,
		isFile:    true,
	},
}.mustCompile()

// mustCompile compiles the regexps in the dirPatterns
func (ds dirPatterns) mustCompile() dirPatterns {
	for i := range ds {
		pattern := &ds[i]
		pattern.match = regexp.MustCompile(pattern.re)
	}
	return ds
}

// match finds the path passed in the matching structure and
// returns the parameters and a pointer to the match, or nil.
func (ds dirPatterns) match(root string, itemPath string, isFile bool) (match []string, prefix string, pattern *dirPattern) {
	itemPath = strings.Trim(itemPath, "/")
	absPath := path.Join(root, itemPath)
	prefix = strings.Trim(absPath[len(root):], "/")
	if prefix != "" {
		prefix += "/"
	}
	for i := range ds {
		pattern = &ds[i]
		if pattern.isFile != isFile {
			continue
		}
		match = pattern.match.FindStringSubmatch(absPath)
		if match != nil {
			return
		}
	}
	return nil, "", nil
}
