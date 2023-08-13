package nixplay

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"time"

	nixplayapi "github.com/anitschke/go-nixplay"
	nixplaytypes "github.com/anitschke/go-nixplay/types"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/log"
)

var (
	errCantUpload = errors.New("can't upload files here")
	errCantRmdir  = errors.New("can't remove this directory")
	errCantMkdir  = errors.New("can't make directories here")
)

//xxx test what happens if I try to put two photos with same content in same album

//xxx test what happens if I try to put two photos with same name in same album

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "nixplay",
		Prefix:      "nixplay",
		Description: "Nixplay",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "user_name",
			Required: true,
			Help:     "xxx", //xxx add help
		}, {
			Name:       "password",
			Required:   true,
			IsPassword: true,
			Help:       "xxx", //xxx add help
		}},
	})
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name string, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	root = strings.Trim(path.Clean(root), "/")
	if root == "." || root == "/" {
		root = ""
	}

	auth := nixplaytypes.Authorization{
		Username: opt.UserName,
	}

	auth.Password, err = obscure.Reveal(opt.Password)
	if err != nil {
		return nil, err
	}

	nixplayClient, err := nixplayapi.NewDefaultClient(ctx, auth, nixplayapi.DefaultClientOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create nixplay client: %w", err)
	}

	f := &Fs{
		name:          name,
		root:          root,
		opt:           *opt,
		startTime:     time.Now(),
		nixplayClient: nixplayClient,
	}

	//xxx double check this
	f.features = (&fs.Features{
		ReadMimeType: true, //xxx
	}).Fill(ctx, f)

	_, _, pattern := patterns.match(f.root, "", true)
	if pattern != nil && pattern.isFile {
		oldRoot := f.root
		var leaf string
		f.root, leaf = path.Split(f.root)
		f.root = strings.TrimRight(f.root, "/")
		_, err := f.NewObject(ctx, leaf)
		if err == nil {
			return f, fs.ErrorIsFile
		}
		f.root = oldRoot
	}
	return f, nil
}

// Options defines the configuration for this backend
type Options struct {
	UserName string `config:"user_name"`
	Password string `config:"password"`
}

// Fs represents a remote storage server
type Fs struct {
	name     string       // name of this remote
	root     string       // the path we are working on if any
	opt      Options      // parsed options
	features *fs.Features // optional features
	//pacer         *fs.Pacer              // To pace the API calls //xxx add
	startTime     time.Time // time Fs was started - used for datestamps //xxx do I really need this?
	nixplayClient nixplayapi.Client
}

// Photo describes a storage object
//
// # Will definitely have info but maybe not meta
//
// xxx rename to photo?
type Photo struct {
	fs     *Fs
	parent nixplayapi.Container
	photo  nixplayapi.Photo
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("NixPlay path %q", f.root)
}

// Precision returns the precision
func (f *Fs) Precision() time.Duration {
	//xxx
	return fs.ModTimeNotSupported
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	defer log.Trace(f, "dir=%q", dir)("err=%v", &err)
	match, prefix, pattern := patterns.match(f.root, dir, false)
	if pattern == nil || pattern.isFile {
		return nil, fs.ErrorDirNotFound
	}
	if pattern.toEntries != nil {
		return pattern.toEntries(ctx, f, prefix, match)
	}
	return nil, fs.ErrorDirNotFound
}

func (f *Fs) listContainers(ctx context.Context, prefix string, containerType nixplaytypes.ContainerType) (entries fs.DirEntries, err error) {
	defer log.Trace(f, "prefix=%q", prefix)("err=%v", &err)

	containers, err := f.nixplayClient.Containers(ctx, containerType)
	if err != nil {
		return nil, err
	}

	for _, c := range containers {
		name, err := c.Name(ctx)
		if err != nil {
			return nil, err
		}
		d := fs.NewDir(prefix+name, f.dirTime())
		d.SetID(idString(c.ID()))

		itemCount, err := c.PhotoCount(ctx)
		if err != nil {
			return nil, err
		}
		d.SetItems(itemCount)

		entries = append(entries, d)
	}

	return entries, nil
}

func (f *Fs) listPhotos(ctx context.Context, prefix string, containerType nixplaytypes.ContainerType, dir string) (entries fs.DirEntries, err error) {
	defer log.Trace(f, "prefix=%q dir=%q", prefix, dir)("err=%v", &err)

	c, err := f.nixplayClient.ContainerWithUniqueName(ctx, containerType, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to get container %q: %w", dir, err)
	}
	if c == nil {
		return nil, fmt.Errorf("container %q does not exist", dir)
	}

	photos, err := c.Photos(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get photos: %w", err)
	}

	for _, p := range photos {
		entries = append(entries, &Photo{
			fs:     f,
			parent: c,
			photo:  p,
		})
	}

	return entries, nil
}

// dirTime returns the time to set a directory to
func (f *Fs) dirTime() time.Time {
	return f.startTime
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (_ fs.Object, err error) {
	defer log.Trace(f, "remote=%q", remote)("err=%v", &err)
	match, _, pattern := patterns.match(f.root, remote, true)
	if pattern == nil {
		return nil, fs.ErrorObjectNotFound
	}
	containerName := match[1]
	photoName := match[2]
	c, err := f.nixplayClient.ContainerWithUniqueName(ctx, pattern.containerType, containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get container: %w", err)
	}
	if c == nil {
		return nil, fs.ErrorObjectNotFound
	}
	photo, err := c.PhotoWithUniqueName(ctx, photoName)
	if err != nil {
		return nil, fmt.Errorf("failed to get photos: %w", err)
	}
	if photo == nil {
		return nil, fs.ErrorObjectNotFound
	}

	return &Photo{
		fs:     f,
		parent: c,
		photo:  photo,
	}, nil
}

// Put the object into the bucket
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	defer log.Trace(f, "src=%+v", src)("")
	match, _, pattern := patterns.match(f.root, src.Remote(), true)
	if pattern == nil || !pattern.isFile || !pattern.canUpload {
		return nil, errCantUpload
	}
	containerName := match[1]
	fileName := match[2]

	c, err := f.nixplayClient.ContainerWithUniqueName(ctx, pattern.containerType, containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get container to upload photo into: %w", err)
	}

	opts := nixplayapi.AddPhotoOptions{
		FileSize: src.Size(),
	}

	// The nixplayClient already has code to determine mime type that is geared
	// toward the photo types that are supported by nixplay, so we will only get
	// the mime type if the src is a MimeTyper instead of using the MimeType
	// helper function since that tries to determine mime type if not know and
	// it doesn't necessarily support all the photo types supported by nixplay.
	if mimeTyper, ok := src.(fs.MimeTyper); ok {
		opts.MIMEType = mimeTyper.MimeType(ctx)
	}

	photo, err := c.AddPhoto(ctx, fileName, in, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to add photo: %w", err)
	}

	//xxx can I just ignore OpenOption?

	o := &Photo{
		fs:     f,
		parent: c,
		photo:  photo,
	}
	return o, nil
}

// Mkdir creates the album if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) (err error) {
	defer log.Trace(f, "dir=%q", dir)("err=%v", &err)
	match, _, pattern := patterns.match(f.root, dir, false)
	if pattern == nil {
		return fs.ErrorDirNotFound
	}
	if !pattern.canMkdir {
		return errCantMkdir
	}
	containerName := match[1]
	_, err = f.nixplayClient.CreateContainer(ctx, pattern.containerType, containerName)
	return err
}

// Rmdir deletes the bucket if the fs is at the root
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) (err error) {
	defer log.Trace(f, "dir=%q")("err=%v", &err)
	match, _, pattern := patterns.match(f.root, dir, false)
	if pattern == nil {
		return fs.ErrorDirNotFound
	}
	if !pattern.canMkdir {
		return errCantRmdir
	}
	containerName := match[1]
	c, err := f.nixplayClient.ContainerWithUniqueName(ctx, pattern.containerType, containerName)
	if err != nil {
		return err
	}
	if c == nil {
		return fs.ErrorDirNotFound
	}
	return c.Delete(ctx)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	//xxx todo
	return f.features
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Photo) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Photo) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.Remote()
}

// Remote returns the remote path
func (o *Photo) Remote() string {
	containerType := o.parent.ContainerType()
	parentName, err := o.parent.Name(context.TODO())
	if err != nil {
		fs.Debugf(o, "Remote: Failed to read parent name: %v", err)
		return ""
	}

	name, err := o.photo.NameUnique(context.TODO())
	if err != nil {
		fs.Debugf(o, "Remote: Failed to read name: %v", err)
		return ""
	}

	remote := filepath.Join(string(containerType), parentName, name)
	return remote
}

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Photo) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}

	hash, err := o.photo.MD5Hash(ctx)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(hash[:]), nil
}

// Size returns the size of an object in bytes
func (o *Photo) Size() int64 {
	size, err := o.photo.Size(context.TODO())
	if err != nil {
		fs.Debugf(o, "Size: Failed to get size: %v", err)
		return -1
	}
	return size
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Photo) ModTime(ctx context.Context) time.Time {
	//xxx todo
	return time.Time{}
}

// SetModTime sets the modification time of the local fs object
func (o *Photo) SetModTime(ctx context.Context, modTime time.Time) (err error) {
	//xxx todo
	return fs.ErrorCantSetModTime
}

// Storable returns a boolean as to whether this object is storable
func (o *Photo) Storable() bool {
	//xxx todo
	return true
}

// Open an object for read
func (o *Photo) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {

	//xxx can I just ignore OpenOption?

	return o.photo.Open(ctx)
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The new object may have been created if an error is returned
func (o *Photo) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	defer log.Trace(o, "src=%+v", src)("err=%v", &err)
	// Nixplay doesn't allow us to update existing items so what we will do
	// instead is delete the existing item and then upload the new version. The
	// downside to this is if photo is in an album and happens to be linked to a
	// playlist then we we delete the photo in the album it will delete it from
	// the playlist too.
	//
	// xxx doc this

	// We need to use the name instead of the unique name here when we re-upload
	// because that is the name that nixplay knows the photo as, so when we
	// upload the new copy we want it to have the same name that nixplay already
	// knows about.
	name, err := o.photo.Name(ctx)
	if err != nil {
		return fmt.Errorf("failed to get name of existing photo: %w", err)
	}

	if err := o.photo.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete existing photo: %w", err)
	}

	//xxx can I just ignore OpenOption like this?

	newPhoto, err := o.parent.AddPhoto(ctx, name, in, nixplayapi.AddPhotoOptions{})
	if err != nil {
		return fmt.Errorf("failed to add new photo: %w", err)
	}

	o.photo = newPhoto
	return nil
}

// Remove an object
func (o *Photo) Remove(ctx context.Context) (err error) {
	return o.photo.Delete(ctx)
}

// MimeType of an Object if known, "" otherwise
func (o *Photo) MimeType(ctx context.Context) string {
	//xxx todo
	return ""
}

// ID of an Object if known, "" otherwise
func (o *Photo) ID() string {
	return idString(o.photo.ID())
}

func idString(id nixplaytypes.ID) string {
	return base64.URLEncoding.EncodeToString(id[:])
}

// Check the interfaces are satisfied
var (
	_ fs.Fs        = &Fs{}
	_ fs.Object    = &Photo{}
	_ fs.MimeTyper = &Photo{}
	_ fs.IDer      = &Photo{}
)
