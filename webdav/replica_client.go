package webdav

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/emersion/go-webdav"
)

// Note: Based on sftp.

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "webdav"

// ReplicaClientTypeSSL is another client type for this package.
const ReplicaClientTypeSSL = "webdavs"

// Default settings for replica client.
const (
	DefaultDialTimeout = 30 * time.Second
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing snapshots & WAL segments to disk.
type ReplicaClient struct {
	mu sync.Mutex
	dc *webdav.Client

	// WebDAV connection info
	Scheme      string
	Host        string
	User        string
	Password    string
	Path        string
	DialTimeout time.Duration
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		DialTimeout: DefaultDialTimeout,
	}
}

// Type returns "webdav" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the WebDAV client. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) (_ *webdav.Client, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.dc != nil {
		return c.dc, nil
	}

	j, _ := cookiejar.New(nil)

	var hc webdav.HTTPClient
	hc = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   c.DialTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     false,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Jar: j,
	}

	var ui *url.Userinfo
	if c.User != "" || c.Password != "" {
		if c.Password == "" {
			ui = url.User(c.User)
		} else {
			ui = url.UserPassword(c.User, c.Password)
		}
		hc = webdav.HTTPClientWithBasicAuth(hc, c.User, c.Password)
	}

	var scheme string
	switch c.Scheme {
	case "webdav", "http":
		scheme = "http"
	case "webdavs", "https":
		scheme = "https"
	default:
		return nil, fmt.Errorf("unsupported webdav url scheme %q", c.Scheme)
	}

	c.dc, err = webdav.NewClient(hc, (&url.URL{
		Scheme: scheme,
		Host:   c.Host,
		User:   ui,
		Path:   c.Path,
	}).String())

	return c.dc, nil
}

// Generations returns a list of available generation names.
func (c *ReplicaClient) Generations(ctx context.Context) (_ []string, err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	fis, err := davClient.Readdir(path.Join(c.Path, "generations"), false)
	if isNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var generations []string
	for _, fi := range fis {
		if !fi.IsDir {
			continue
		}

		name := path.Base(fi.Path)
		if !litestream.IsGenerationName(name) {
			continue
		}
		generations = append(generations, name)
	}

	sort.Strings(generations)

	return generations, nil
}

// DeleteGeneration deletes all snapshots & WAL segments within a generation.
func (c *ReplicaClient) DeleteGeneration(ctx context.Context, generation string) (err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return err
	} else if generation == "" {
		return fmt.Errorf("generation required")
	}

	dir := path.Join(c.Path, "generations", generation)
	if err := removeAll(davClient, dir); err != nil && !isNotFound(err) {
		time.Sleep(time.Second) // some DAV servers have locking issues
		if err := removeAll(davClient, dir); err != nil && !isNotFound(err) {
			return err
		}
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	return nil
}

// Snapshots returns an iterator over all available snapshots for a generation.
func (c *ReplicaClient) Snapshots(ctx context.Context, generation string) (_ litestream.SnapshotIterator, err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	} else if generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	dir := path.Join(c.Path, "generations", generation, "snapshots")

	fis, err := davClient.Readdir(dir, false)
	if isNotFound(err) {
		return litestream.NewSnapshotInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	infos := make([]litestream.SnapshotInfo, 0, len(fis))
	for _, fi := range fis {
		// Parse index from filename.
		index, err := internal.ParseSnapshotPath(path.Base(fi.Path))
		if err != nil {
			continue
		}

		infos = append(infos, litestream.SnapshotInfo{
			Generation: generation,
			Index:      index,
			Size:       fi.Size,
			CreatedAt:  fi.ModTime.UTC(),
		})
	}

	sort.Sort(litestream.SnapshotInfoSlice(infos))

	return litestream.NewSnapshotInfoSliceIterator(infos), nil
}

// WriteSnapshot writes LZ4 compressed data from rd to the object storage.
func (c *ReplicaClient) WriteSnapshot(ctx context.Context, generation string, index int, rd io.Reader) (info litestream.SnapshotInfo, err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return info, err
	} else if generation == "" {
		return info, fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", generation, "snapshots", litestream.FormatIndex(index)+".snapshot.lz4")
	startTime := time.Now()

	if err := mkdirAll(davClient, path.Dir(filename)); err != nil {
		return info, fmt.Errorf("cannot make parent wal segment directory %q: %w", path.Dir(filename), err)
	}

	f, err := davClient.Create(filename)
	if err != nil {
		return info, fmt.Errorf("cannot open snapshot file for writing: %w", err)
	}
	closer := internal.OnceCloser(f)
	defer closer.Close()

	n, err := io.Copy(f, rd)
	if err != nil {
		return info, err
	} else if err := closer.Close(); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	return litestream.SnapshotInfo{
		Generation: generation,
		Index:      index,
		Size:       n,
		CreatedAt:  startTime.UTC(),
	}, nil
}

// SnapshotReader returns a reader for snapshot data at the given generation/index.
func (c *ReplicaClient) SnapshotReader(ctx context.Context, generation string, index int) (_ io.ReadCloser, err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	} else if generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", generation, "snapshots", litestream.FormatIndex(index)+".snapshot.lz4")

	f, err := davClient.Open(filename)
	if err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	return f, nil
}

// DeleteSnapshot deletes a snapshot with the given generation & index.
func (c *ReplicaClient) DeleteSnapshot(ctx context.Context, generation string, index int) (err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return err
	} else if generation == "" {
		return fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", generation, "snapshots", litestream.FormatIndex(index)+".snapshot.lz4")

	if err := removeAll(davClient, filename); err != nil && !isNotFound(err) {
		time.Sleep(time.Second) // some DAV servers have locking issues
		if err := removeAll(davClient, filename); err != nil && !isNotFound(err) {
			return fmt.Errorf("cannot delete snapshot %q: %w", filename, err)
		}
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

	return nil
}

// WALSegments returns an iterator over all available WAL files for a generation.
func (c *ReplicaClient) WALSegments(ctx context.Context, generation string) (_ litestream.WALSegmentIterator, err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	} else if generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	dir := path.Join(c.Path, "generations", generation, "wal")

	fis, err := davClient.Readdir(dir, false)
	if isNotFound(err) {
		return litestream.NewWALSegmentInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	indexes := make([]int, 0, len(fis))
	for _, fi := range fis {
		index, err := litestream.ParseIndex(path.Base(fi.Path))
		if err != nil || !fi.IsDir {
			continue
		}
		indexes = append(indexes, index)
	}

	sort.Ints(indexes)

	return newWALSegmentIterator(ctx, c, dir, generation, indexes), nil
}

// WriteWALSegment writes LZ4 compressed data from rd into a file on disk.
func (c *ReplicaClient) WriteWALSegment(ctx context.Context, pos litestream.Pos, rd io.Reader) (info litestream.WALSegmentInfo, err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return info, err
	} else if pos.Generation == "" {
		return info, fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", pos.Generation, "wal", litestream.FormatIndex(pos.Index), litestream.FormatOffset(pos.Offset)+".wal.lz4")
	startTime := time.Now()

	if err := mkdirAll(davClient, path.Dir(filename)); err != nil {
		return info, fmt.Errorf("cannot make parent snapshot directory %q: %w", path.Dir(filename), err)
	}

	f, err := davClient.Create(filename)
	if err != nil {
		return info, fmt.Errorf("cannot open snapshot file for writing: %w", err)
	}
	closer := internal.OnceCloser(f)
	defer closer.Close()

	n, err := io.Copy(f, rd)
	if err != nil {
		return info, err
	} else if err := closer.Close(); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	return litestream.WALSegmentInfo{
		Generation: pos.Generation,
		Index:      pos.Index,
		Offset:     pos.Offset,
		Size:       n,
		CreatedAt:  startTime.UTC(),
	}, nil
}

// WALSegmentReader returns a reader for a section of WAL data at the given index.
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) WALSegmentReader(ctx context.Context, pos litestream.Pos) (_ io.ReadCloser, err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	} else if pos.Generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", pos.Generation, "wal", litestream.FormatIndex(pos.Index), litestream.FormatOffset(pos.Offset)+".wal.lz4")

	f, err := davClient.Open(filename)
	if err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	return f, nil
}

// DeleteWALSegments deletes WAL segments with at the given positions.
func (c *ReplicaClient) DeleteWALSegments(ctx context.Context, a []litestream.Pos) (err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return err
	}

	for _, pos := range a {
		if pos.Generation == "" {
			return fmt.Errorf("generation required")
		}

		filename := path.Join(c.Path, "generations", pos.Generation, "wal", litestream.FormatIndex(pos.Index), litestream.FormatOffset(pos.Offset)+".wal.lz4")

		if err := removeAll(davClient, filename); err != nil && !isNotFound(err) {
			time.Sleep(time.Second) // some DAV servers have locking issues
			if err := removeAll(davClient, filename); err != nil && !isNotFound(err) {
				return fmt.Errorf("cannot delete wal segment %q: %w", filename, err)
			}
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// Cleanup deletes path & generations directories after empty.
func (c *ReplicaClient) Cleanup(ctx context.Context) (err error) {
	defer func() {
		if isNotFound(err) {
			err = os.ErrNotExist
		}
	}()

	davClient, err := c.Init(ctx)
	if err != nil {
		return err
	}

	if err := removeAll(davClient, c.Path); err != nil && !isNotFound(err) {
		time.Sleep(time.Second) // some DAV servers have locking issues
		if err := removeAll(davClient, c.Path); err != nil && !isNotFound(err) {
			return fmt.Errorf("cannot delete: %w", err)
		}
	}
	return nil
}

type walSegmentIterator struct {
	ctx        context.Context
	client     *ReplicaClient
	dir        string
	generation string
	indexes    []int

	infos []litestream.WALSegmentInfo
	err   error
}

func newWALSegmentIterator(ctx context.Context, client *ReplicaClient, dir, generation string, indexes []int) *walSegmentIterator {
	return &walSegmentIterator{
		ctx:        ctx,
		client:     client,
		dir:        dir,
		generation: generation,
		indexes:    indexes,
	}
}

func (itr *walSegmentIterator) Close() (err error) {
	return itr.err
}

func (itr *walSegmentIterator) Next() bool {
	davClient, err := itr.client.Init(itr.ctx)
	if err != nil {
		itr.err = err
		return false
	}

	// Exit if an error has already occurred.
	if itr.err != nil {
		return false
	}

	for {
		// Move to the next segment in cache, if available.
		if len(itr.infos) > 1 {
			itr.infos = itr.infos[1:]
			return true
		}
		itr.infos = itr.infos[:0] // otherwise clear infos

		// Move to the next index unless this is the first time initializing.
		if itr.infos != nil && len(itr.indexes) > 0 {
			itr.indexes = itr.indexes[1:]
		}

		// If no indexes remain, stop iteration.
		if len(itr.indexes) == 0 {
			return false
		}

		// Read segments into a cache for the current index.
		index := itr.indexes[0]
		fis, err := davClient.Readdir(path.Join(itr.dir, litestream.FormatIndex(index)), false)
		if err != nil {
			itr.err = err
			return false
		}

		for _, fi := range fis {
			filename := path.Base(fi.Path)
			if fi.IsDir {
				continue
			}

			offset, err := litestream.ParseOffset(strings.TrimSuffix(filename, ".wal.lz4"))
			if err != nil {
				continue
			}

			itr.infos = append(itr.infos, litestream.WALSegmentInfo{
				Generation: itr.generation,
				Index:      index,
				Offset:     offset,
				Size:       fi.Size,
				CreatedAt:  fi.ModTime.UTC(),
			})
		}

		if len(itr.infos) > 0 {
			return true
		}
	}
}

func (itr *walSegmentIterator) Err() error { return itr.err }

func (itr *walSegmentIterator) WALSegment() litestream.WALSegmentInfo {
	if len(itr.infos) == 0 {
		return litestream.WALSegmentInfo{}
	}
	return itr.infos[0]
}

func isNotFound(err error) bool {
	// this is ugly, but it's the only way to do this currently (see internal/internal.go)
	return err != nil && strings.Contains(strings.ToLower(err.Error()), strings.ToLower(http.StatusText(http.StatusNotFound)))
}

func removeAll(dc *webdav.Client, p string) error {
	if err := dc.RemoveAll(p); err != nil {
		if _, err := dc.Stat(p); err != nil {
			if isNotFound(err) {
				return nil
			}
			return err
		}
		return err
	}
	return nil
}

func mkdirAll(dc *webdav.Client, p string) (err error) {
	// this is also ugly, but there's no proper way to create all dirs on webdav
	// e.g., <error xmlns="DAV:"><exception xmlns="http://sabredav.org/ns">Sabre_DAV_Exception_Conflict</exception><message xmlns="http://sabredav.org/ns">Parent node does not exist</message></error>

	c := "/" + strings.Trim(p, "/")

	var ds []string
	for c != "" && c != "/" && c != "." {
		if fi, err := dc.Stat(c); err != nil {
			if !isNotFound(err) {
				return fmt.Errorf("make dirs %q: stat %q: %w", p, c, err)
			}
		} else if !fi.IsDir {
			return fmt.Errorf("make dirs %q: stat %q: is a file", p, c)
		} else {
			break
		}
		ds = append(ds, path.Base(c))
		c = path.Dir(c)
	}
	for len(ds) != 0 {
		var d string
		d, ds = ds[len(ds)-1], ds[:len(ds)-1]
		c = path.Join(c, d)
		if err := dc.Mkdir(c); err != nil {
			return fmt.Errorf("make dirs %q: mkdir %q (remaining: %q): %w", p, c, d, err)
		}
	}
	return nil
}
