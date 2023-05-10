package nixplay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	nixplayapi "github.com/andrewjjenkins/picsync/pkg/nixplay"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/log"
)

var (
	errCantUpload = errors.New("can't upload files here")
)

// xxx "Use lib/encoder to make sure we can encode any path name and rclone info to help determine the encodings needed"

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

	opt.Password, err = obscure.Reveal(opt.Password)
	if err != nil {
		return nil, err
	}

	nixplayClient, err := nixplayapi.NewClient(opt.UserName, opt.Password, nil)
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
		ReadMimeType: true,
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
	//unAuth        *rest.Client           // unauthenticated http client  //xxx add
	//srv           *rest.Client           // the connection to the server //xxx add
	//pacer         *fs.Pacer              // To pace the API calls //xxx add
	startTime     time.Time  // time Fs was started - used for datestamps //xxx do I really need this?
	createMu      sync.Mutex // held when creating albums to prevent dupes //xxx do I need this?
	nixplayClient nixplayapi.Client
}

// Object describes a storage object
//
// # Will definitely have info but maybe not meta
//
// xxx rename to photo?
type Object struct {
	fs       *Fs       // what this object is part of
	remote   string    // The remote path
	url      string    // download path
	id       int       // ID of this object
	bytes    int64     // Bytes in the object
	modTime  time.Time // Modified time of the object
	mimeType string
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
	//xxx I think that we can support hashes
	return hash.Set(hash.None)
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

func (f *Fs) listAlbums(ctx context.Context, prefix string) (entries fs.DirEntries, err error) {
	defer log.Trace(f, "prefix=%q", prefix)("err=%v", &err)
	albums, err := f.nixplayClient.GetAlbums()
	if err != nil {
		return nil, err
	}

	for _, a := range albums {
		d := fs.NewDir(prefix+a.Title, f.dirTime())
		d.SetID(strconv.Itoa(a.ID)).SetItems(int64(a.PhotoCount))
		entries = append(entries, d)
	}

	fmt.Println(len(entries))

	return entries, nil
}

func (f *Fs) listAlbumPhotos(ctx context.Context, prefix string, dir string) (entries fs.DirEntries, err error) {
	defer log.Trace(f, "prefix=%q dir=%q", prefix, dir)("err=%v", &err)

	albums, err := f.nixplayClient.GetAlbumsByName(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to get album: %w", err)
	}
	if len(albums) != 1 {
		return nil, fmt.Errorf("got %d albums for %q", len(albums), dir)
	}

	//xxx needs pagination
	page := 1
	limit := 50
	photos, err := f.nixplayClient.GetPhotos(albums[0].ID, page, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get photos: %w", err)
	}

	for _, p := range photos {
		entries = append(entries, &Object{
			fs:      f,
			id:      p.ID,
			modTime: f.startTime, //xxx can I do better?
			remote:  prefix + p.Filename,
		})
	}

	return entries, nil
}

func (f *Fs) listPlaylists(ctx context.Context, prefix string) (entries fs.DirEntries, err error) {
	defer log.Trace(f, "prefix=%q", prefix)("err=%v", &err)
	//xxx TODO
	return fs.DirEntries{}, nil
}

func (f *Fs) listPlaylistPhotos(ctx context.Context, prefix string, dir string) (entries fs.DirEntries, err error) {
	defer log.Trace(f, "prefix=%q dir=%q", prefix, dir)("err=%v", &err)
	//xxx TODO
	return fs.DirEntries{}, nil
}

// dirTime returns the time to set a directory to
func (f *Fs) dirTime() time.Time {
	return f.startTime
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (_ fs.Object, err error) {
	defer log.Trace(f, "remote=%q", remote)("err=%v", &err)

	//xxx todo
	return nil, fs.ErrorObjectNotFound
}

// Put the object into the bucket
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	defer log.Trace(f, "src=%+v", src)("")
	// Temporary Object under construction
	o := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir creates the album if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) (err error) {
	//xxx todo
	return errors.New("TODO")
}

// Rmdir deletes the bucket if the fs is at the root
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) (err error) {
	//xxx todo
	return errors.New("TODO")
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	//xxx todo
	return f.features
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	//xxx todo
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	//xxx todo
	return o.remote
}

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	//xxx todo
	return "", hash.ErrUnsupported
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	//xxx todo
	return 0
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(ctx context.Context) time.Time {
	//xxx todo
	return time.Time{}
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) (err error) {
	//xxx todo
	return fs.ErrorCantSetModTime
}

// Storable returns a boolean as to whether this object is storable
func (o *Object) Storable() bool {
	//xxx todo
	return true
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	//xxx todo
	return nil, errors.New("TODO")
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The new object may have been created if an error is returned
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	defer log.Trace(o, "src=%+v", src)("err=%v", &err)
	match, _, pattern := patterns.match(o.fs.root, o.remote, true)
	if pattern == nil || !pattern.isFile || !pattern.canUpload {
		return errCantUpload
	}

	albumName := match[1]
	fileName := match[2]

	//xxx
	fmt.Println(albumName)
	fmt.Println(fileName)

	albums, err := o.fs.nixplayClient.GetAlbumsByName(albumName)
	if err != nil {
		return fmt.Errorf("failed to get album: %w", err)
	}
	if len(albums) != 1 {
		return fmt.Errorf("got %d albums for %q", len(albums), albumName)
	}

	// xxx why is file name and file type separate? Is it mime type or extension or ...?
	// lets just guess it is the mime type for now and hardcode jpeg?

	// xxx ugh this is a really inefficient way to get the file size... but it works for now.
	var buf bytes.Buffer
	size, err := io.Copy(&buf, in)
	if err != nil {
		fmt.Println(err)
	}

	fileType := "image/jpeg"
	err = o.fs.nixplayClient.UploadPhoto(albums[0].ID, fileName, fileType, uint64(size), io.NopCloser(&buf))
	if err != nil {
		return fmt.Errorf("failed to upload photo: %w", err)
	}

	// xxx so now we have uploaded the photo, but the problem is UploadPhoto
	// doesn't tell us the new ID of the photo that was uploaded
	// So for now we will just error out, this needs to get fixed though.
	return errors.New("TODO upload worked but due to issue where UploadPhoto doesn't tell us ID of uploaded photo we can't continue")
}

// Remove an object
func (o *Object) Remove(ctx context.Context) (err error) {
	//xxx todo
	return errors.New("TODO")
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	//xxx todo
	return o.mimeType
}

// ID of an Object if known, "" otherwise
func (o *Object) ID() string {
	//xxx todo
	return strconv.Itoa(o.id)
}

// Check the interfaces are satisfied
var (
	_ fs.Fs        = &Fs{}
	_ fs.Object    = &Object{}
	_ fs.MimeTyper = &Object{}
	_ fs.IDer      = &Object{}
)
