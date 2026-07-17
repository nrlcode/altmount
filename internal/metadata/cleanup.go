package metadata

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"google.golang.org/protobuf/proto"
)

// cleanupOperationMu serializes destructive cleanup across MetadataService
// instances. Cleanup is rare, and one process may own the same root twice.
var cleanupOperationMu sync.Mutex

// ConfigureCleanupRoots is the narrow production wrapper around cleanup
// authority configuration.
func (ms *MetadataService) ConfigureCleanupRoots(storeRoot string, sourceRoots ...string) error {
	return ms.configureCleanupRoots(storeRoot, sourceRoots)
}

func (ms *MetadataService) configureCleanupRoots(storeRoot string, sourceRoots []string) error {
	store, err := canonicalCleanupRoot(storeRoot)
	var roots []string
	if err == nil {
		seen := make(map[string]struct{}, len(sourceRoots))
		for _, sourceRoot := range append([]string(nil), sourceRoots...) {
			var root string
			root, err = canonicalCleanupRoot(sourceRoot)
			if err != nil {
				break
			}
			if _, ok := seen[root]; !ok {
				seen[root] = struct{}{}
				roots = append(roots, root)
			}
		}
		for i := 0; err == nil && i < len(roots); i++ {
			for j := i + 1; j < len(roots); j++ {
				if pathWithinRoot(roots[i], roots[j]) || pathWithinRoot(roots[j], roots[i]) {
					err = fmt.Errorf("cleanup source roots overlap: %q and %q", roots[i], roots[j])
					break
				}
			}
		}
	}
	ms.cleanupMu.Lock()
	defer ms.cleanupMu.Unlock()
	if err != nil {
		err = fmt.Errorf("configure cleanup roots: %w", err)
		ms.cleanupConfigErr = err
		return err
	}
	ms.cleanupStoreRoot = store
	ms.cleanupSourceRoot = roots
	ms.cleanupConfigErr = nil
	return nil
}

func canonicalCleanupRoot(name string) (string, error) {
	if name == "" {
		return "", errors.New("cleanup root is empty")
	}
	root, err := filepath.Abs(name)
	if err != nil {
		return "", fmt.Errorf("canonicalize cleanup root %q: %w", name, err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return root, nil
		}
		return "", fmt.Errorf("inspect cleanup root %q: %w", root, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("cleanup root %q is not an unambiguous directory", root)
	}
	return root, nil
}

type cleanupRoots struct {
	metadata, store string
	sources         []string
	err             error
}

func (ms *MetadataService) cleanupRoots() cleanupRoots {
	ms.cleanupMu.RLock()
	defer ms.cleanupMu.RUnlock()
	err := ms.metadataRootErr
	if err == nil {
		err = ms.cleanupConfigErr
	}
	return cleanupRoots{
		metadata: ms.metadataRoot,
		store:    ms.cleanupStoreRoot,
		sources:  append([]string(nil), ms.cleanupSourceRoot...),
		err:      err,
	}
}

type cleanupAuthority struct {
	root    *os.Root
	missing bool
}

type cleanupTarget struct {
	authority *cleanupAuthority
	absolute  string
	relative  string
	exists    bool
	mode      fs.FileMode
}

type cleanupPlanner struct{ authorities map[string]*cleanupAuthority }

func newCleanupPlanner() *cleanupPlanner {
	return &cleanupPlanner{authorities: make(map[string]*cleanupAuthority)}
}

func (p *cleanupPlanner) close() {
	for _, authority := range p.authorities {
		if authority.root != nil {
			_ = authority.root.Close()
		}
	}
}

func (p *cleanupPlanner) authority(name string) (*cleanupAuthority, error) {
	if authority := p.authorities[name]; authority != nil {
		return authority, nil
	}
	root, err := os.OpenRoot(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			info, inspectErr := os.Lstat(name)
			if errors.Is(inspectErr, fs.ErrNotExist) {
				authority := &cleanupAuthority{missing: true}
				p.authorities[name] = authority
				return authority, nil
			}
			if inspectErr != nil {
				return nil, fmt.Errorf("inspect cleanup root %q: %w", name, inspectErr)
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return nil, fmt.Errorf("cleanup root %q is not an unambiguous directory", name)
			}
			return nil, fmt.Errorf("cleanup root %q changed while acquiring authority", name)
		}
		return nil, fmt.Errorf("open cleanup root %q: %w", name, err)
	}
	keepRoot := false
	defer func() {
		if !keepRoot {
			_ = root.Close()
		}
	}()
	info, err := os.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup root %q: %w", name, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("cleanup root %q is not an unambiguous directory", name)
	}
	rootInfo, err := root.Stat(".")
	if err != nil {
		return nil, fmt.Errorf("inspect opened cleanup root %q: %w", name, err)
	}
	if !os.SameFile(info, rootInfo) {
		return nil, fmt.Errorf("cleanup root %q changed while acquiring authority", name)
	}
	authority := &cleanupAuthority{root: root}
	p.authorities[name] = authority
	keepRoot = true
	return authority, nil
}

func (p *cleanupPlanner) externalFile(rootName, name string) (*cleanupTarget, error) {
	if rootName == "" {
		return nil, errors.New("cleanup target has no configured authority")
	}
	absolute, err := filepath.Abs(name)
	if err != nil {
		return nil, fmt.Errorf("canonicalize cleanup target %q: %w", name, err)
	}
	relative, err := filepath.Rel(rootName, absolute)
	if err != nil || !filepath.IsLocal(relative) || relative == "." {
		return nil, fmt.Errorf("cleanup target %q is outside root %q", name, rootName)
	}
	return p.relativeTarget(rootName, relative, false)
}

func (p *cleanupPlanner) relativeTarget(rootName, relative string, directory bool) (*cleanupTarget, error) {
	if !filepath.IsLocal(relative) || relative == "." {
		return nil, fmt.Errorf("cleanup target %q is not a contained child", relative)
	}
	authority, err := p.authority(rootName)
	if err != nil {
		return nil, err
	}
	target := &cleanupTarget{
		authority: authority,
		absolute:  filepath.Join(rootName, relative),
		relative:  relative,
	}
	if authority.missing {
		return target, nil
	}
	missingParent := false
	for parent := filepath.Dir(relative); parent != "."; parent = filepath.Dir(parent) {
		info, statErr := authority.root.Lstat(parent)
		if errors.Is(statErr, fs.ErrNotExist) {
			missingParent = true
			continue
		}
		if statErr != nil {
			return nil, fmt.Errorf("inspect cleanup parent %q: %w", parent, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, fmt.Errorf("cleanup parent %q is not an unambiguous directory", parent)
		}
	}
	if missingParent {
		return target, nil
	}
	info, err := authority.root.Lstat(relative)
	if errors.Is(err, fs.ErrNotExist) {
		return target, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup target %q: %w", target.absolute, err)
	}
	if directory {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, fmt.Errorf("cleanup directory target %q is ambiguous", target.absolute)
		}
	} else if info.Mode()&os.ModeSymlink == 0 && !info.Mode().IsRegular() {
		return nil, fmt.Errorf("cleanup file target %q is not a regular file or symlink", target.absolute)
	}
	target.exists = true
	target.mode = info.Mode()
	return target, nil
}

func pathWithinRoot(root, name string) bool {
	relative, err := filepath.Rel(root, name)
	return err == nil && filepath.IsLocal(relative) && relative != "."
}

func cleanupVirtualPath(name string) (string, error) {
	if name == "" || filepath.IsAbs(name) || !filepath.IsLocal(name) {
		return "", fmt.Errorf("virtual cleanup path %q is not local", name)
	}
	for _, component := range strings.Split(filepath.ToSlash(name), "/") {
		if component == ".." {
			return "", fmt.Errorf("virtual cleanup path %q contains traversal", name)
		}
	}
	name = filepath.Clean(name)
	if name == "." {
		return "", fmt.Errorf("virtual cleanup path %q resolves to the metadata root", name)
	}
	return name, nil
}

func rawCleanupMetadata(target *cleanupTarget) (*metapb.FileMetadata, error) {
	if !target.exists {
		return nil, nil
	}
	if target.mode&os.ModeSymlink != 0 {
		return nil, nil
	}
	data, err := target.authority.root.ReadFile(target.relative)
	if err != nil {
		return nil, fmt.Errorf("read cleanup metadata %q: %w", target.absolute, err)
	}
	if isV3Meta(data) {
		data = data[len(metaMagicV3):]
	}
	metadata := new(metapb.FileMetadata)
	if err := proto.Unmarshal(data, metadata); err != nil {
		return nil, fmt.Errorf("decode cleanup metadata %q: %w", target.absolute, err)
	}
	return metadata, nil
}

func (p *cleanupPlanner) sourceFile(roots cleanupRoots, name string) (*cleanupTarget, error) {
	absolute, err := filepath.Abs(name)
	if err != nil {
		return nil, fmt.Errorf("canonicalize source cleanup target %q: %w", name, err)
	}
	for _, root := range roots.sources {
		if pathWithinRoot(root, absolute) {
			return p.externalFile(root, absolute)
		}
	}
	return nil, fmt.Errorf("source cleanup target %q has no configured authority", name)
}

type fileCleanupPlan struct {
	virtual      string
	metadata     *cleanupTarget
	id           *cleanupTarget
	sources      map[string]*cleanupTarget
	store        *cleanupTarget
	physical     *cleanupTarget
	pruneSources bool
}

func (ms *MetadataService) planFileCleanup(planner *cleanupPlanner, roots cleanupRoots, virtualPath string, deleteSource bool, explicitSource *cleanupTarget, physicalPath, physicalRoot string) (*fileCleanupPlan, error) {
	virtualPath, err := cleanupVirtualPath(virtualPath)
	if err != nil {
		return nil, err
	}
	metadata, err := planner.relativeTarget(roots.metadata, virtualPath+".meta", false)
	if err != nil {
		return nil, err
	}
	id, err := planner.relativeTarget(roots.metadata, virtualPath+".meta.id", false)
	if err != nil {
		return nil, err
	}
	raw, err := rawCleanupMetadata(metadata)
	if err != nil {
		return nil, err
	}
	plan := &fileCleanupPlan{virtual: virtualPath, metadata: metadata, id: id, sources: make(map[string]*cleanupTarget)}
	if explicitSource != nil {
		plan.sources[explicitSource.absolute] = explicitSource
	}
	if raw != nil && deleteSource && raw.SourceNzbPath != "" {
		source, targetErr := planner.sourceFile(roots, raw.SourceNzbPath)
		if targetErr != nil {
			return nil, targetErr
		}
		plan.sources[source.absolute] = source
	}
	if raw != nil && raw.StoreRef != "" {
		store, targetErr := planner.externalFile(roots.store, raw.StoreRef)
		if targetErr != nil {
			return nil, targetErr
		}
		plan.store = store
		delete(plan.sources, store.absolute)
	}
	if physicalPath != "" {
		root, rootErr := canonicalCleanupRoot(physicalRoot)
		if rootErr != nil {
			return nil, fmt.Errorf("configure physical cleanup root: %w", rootErr)
		}
		physical, targetErr := planner.externalFile(root, physicalPath)
		if targetErr != nil {
			return nil, targetErr
		}
		plan.physical = physical
	}
	return plan, nil
}

func (ms *MetadataService) executeFileCleanup(ctx context.Context, plan *fileCleanupPlan) error {
	removeStore := false
	if plan.store != nil && ms.storeRefCounter != nil {
		count, err := ms.storeRefCounter.DecStoreRef(ctx, plan.store.absolute)
		if err != nil {
			return fmt.Errorf("decrement store reference %q: %w", plan.store.absolute, err)
		}
		removeStore = count == 0
	}
	if err := removeCleanupTarget(plan.metadata); err != nil {
		return fmt.Errorf("remove metadata: %w", err)
	}
	if err := removeCleanupTarget(plan.id); err != nil {
		return fmt.Errorf("remove metadata sidecar: %w", err)
	}
	ms.liteCache.Remove(plan.virtual)
	if err := pruneCleanupParents(plan.metadata); err != nil {
		return fmt.Errorf("prune metadata directories: %w", err)
	}
	for _, source := range plan.sources {
		if err := removeAndPrune(source, plan.pruneSources); err != nil {
			return fmt.Errorf("remove source NZB %q: %w", source.absolute, err)
		}
	}
	if plan.physical != nil {
		if err := removeAndPrune(plan.physical, true); err != nil {
			return fmt.Errorf("remove physical file: %w", err)
		}
	}
	if removeStore {
		if err := removeCleanupTarget(plan.store); err != nil {
			return fmt.Errorf("remove orphaned store %q: %w", plan.store.absolute, err)
		}
		ms.store.cache.Remove(plan.store.absolute)
	}
	return nil
}

func (ms *MetadataService) cleanupOperation(run func(*cleanupPlanner, cleanupRoots) error) error {
	cleanupOperationMu.Lock()
	defer cleanupOperationMu.Unlock()
	roots := ms.cleanupRoots()
	if roots.err != nil {
		return fmt.Errorf("cleanup authority unavailable: %w", roots.err)
	}
	planner := newCleanupPlanner()
	defer planner.close()
	return run(planner, roots)
}

func (ms *MetadataService) deleteFileMetadata(ctx context.Context, virtualPath string, deleteSource bool, physicalPath, physicalRoot string) error {
	return ms.cleanupOperation(func(planner *cleanupPlanner, roots cleanupRoots) error {
		plan, err := ms.planFileCleanup(planner, roots, virtualPath, deleteSource, nil, physicalPath, physicalRoot)
		if err != nil {
			return err
		}
		return ms.executeFileCleanup(ctx, plan)
	})
}

type directoryEntry struct {
	virtual  string
	metadata *cleanupTarget
	id       *cleanupTarget
	store    *cleanupTarget
}

type directoryCleanupPlan struct {
	virtual string
	target  *cleanupTarget
	entries []directoryEntry
	source  *cleanupTarget
}

func (ms *MetadataService) planDirectoryCleanup(planner *cleanupPlanner, roots cleanupRoots, virtualPath string, source *cleanupTarget) (*directoryCleanupPlan, error) {
	virtualPath, err := cleanupVirtualPath(virtualPath)
	if err != nil {
		return nil, err
	}
	target, err := planner.relativeTarget(roots.metadata, virtualPath, true)
	if err != nil {
		return nil, err
	}
	plan := &directoryCleanupPlan{virtual: virtualPath, target: target, source: source}
	if !target.exists {
		return plan, nil
	}
	stores := make(map[string]*cleanupTarget)
	subroot, err := target.authority.root.OpenRoot(target.relative)
	if err != nil {
		return nil, fmt.Errorf("open metadata directory %q: %w", target.absolute, err)
	}
	defer subroot.Close()
	err = fs.WalkDir(subroot.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(name, ".meta") {
			return nil
		}
		metadataRelative := filepath.Join(target.relative, filepath.FromSlash(name))
		metadataTarget, targetErr := planner.relativeTarget(roots.metadata, metadataRelative, false)
		if targetErr != nil {
			return targetErr
		}
		idTarget, targetErr := planner.relativeTarget(roots.metadata, metadataRelative+".id", false)
		if targetErr != nil {
			return targetErr
		}
		data, readErr := metadataTarget.authority.root.ReadFile(metadataTarget.relative)
		if readErr != nil {
			return fmt.Errorf("read metadata %q: %w", name, readErr)
		}
		if !isV3Meta(data) {
			return nil
		}
		metadata := new(metapb.FileMetadata)
		if unmarshalErr := proto.Unmarshal(data[len(metaMagicV3):], metadata); unmarshalErr != nil {
			return fmt.Errorf("decode metadata %q: %w", name, unmarshalErr)
		}
		if metadata.StoreRef == "" {
			return nil
		}
		store, targetErr := planner.externalFile(roots.store, metadata.StoreRef)
		if targetErr != nil {
			return targetErr
		}
		if current := stores[store.absolute]; current != nil {
			store = current
		} else {
			stores[store.absolute] = store
		}
		plan.entries = append(plan.entries, directoryEntry{
			virtual:  filepath.Join(virtualPath, strings.TrimSuffix(filepath.FromSlash(name), ".meta")),
			metadata: metadataTarget,
			id:       idTarget,
			store:    store,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk metadata directory %q: %w", target.absolute, err)
	}
	if source != nil {
		if _, ownedByStore := stores[source.absolute]; ownedByStore {
			plan.source = nil
		}
	}
	return plan, nil
}

func (ms *MetadataService) executeDirectoryCleanup(ctx context.Context, plan *directoryCleanupPlan) error {
	if ms.storeRefCounter != nil {
		for _, entry := range plan.entries {
			count, err := ms.storeRefCounter.DecStoreRef(ctx, entry.store.absolute)
			if err != nil {
				return fmt.Errorf("decrement store reference %q: %w", entry.store.absolute, err)
			}
			if err := removeCleanupTarget(entry.metadata); err != nil {
				return fmt.Errorf("remove metadata %q: %w", entry.metadata.absolute, err)
			}
			ms.liteCache.Remove(entry.virtual)
			if err := removeCleanupTarget(entry.id); err != nil {
				return fmt.Errorf("remove metadata sidecar %q: %w", entry.id.absolute, err)
			}
			if count == 0 {
				if err := removeCleanupTarget(entry.store); err != nil {
					return fmt.Errorf("remove orphaned store %q: %w", entry.store.absolute, err)
				}
				ms.store.cache.Remove(entry.store.absolute)
			}
		}
	}
	if plan.target.exists {
		if err := plan.target.authority.root.RemoveAll(plan.target.relative); err != nil {
			return fmt.Errorf("remove metadata directory %q: %w", plan.target.absolute, err)
		}
	}
	prefix := plan.virtual + string(filepath.Separator)
	for _, key := range ms.liteCache.Keys() {
		if key == plan.virtual || strings.HasPrefix(key, prefix) {
			ms.liteCache.Remove(key)
		}
	}
	if plan.source != nil {
		if err := removeAndPrune(plan.source, true); err != nil {
			return fmt.Errorf("remove source NZB %q: %w", plan.source.absolute, err)
		}
	}
	return nil
}

func (ms *MetadataService) deleteDirectory(ctx context.Context, virtualPath string) error {
	return ms.cleanupOperation(func(planner *cleanupPlanner, roots cleanupRoots) error {
		plan, err := ms.planDirectoryCleanup(planner, roots, virtualPath, nil)
		if err != nil {
			return err
		}
		return ms.executeDirectoryCleanup(ctx, plan)
	})
}

// DeleteStoragePathWithSourceNzb is the Stremio cleanup composite. The explicit
// source is preflighted before metadata, source, parent-directory, or
// queue-visible mutation.
func (ms *MetadataService) DeleteStoragePathWithSourceNzb(ctx context.Context, storagePath, sourcePath string) error {
	return ms.cleanupOperation(func(planner *cleanupPlanner, roots cleanupRoots) error {
		source, err := planner.sourceFile(roots, sourcePath)
		if err != nil {
			return err
		}
		if storagePath == "" {
			return removeAndPrune(source, true)
		}
		directory, err := ms.planDirectoryCleanup(planner, roots, storagePath, source)
		if err != nil {
			return err
		}
		if directory.target.exists {
			return ms.executeDirectoryCleanup(ctx, directory)
		}
		plan, err := ms.planFileCleanup(planner, roots, storagePath, true, source, "", "")
		if err != nil {
			return err
		}
		plan.pruneSources = true
		return ms.executeFileCleanup(ctx, plan)
	})
}

func removeCleanupTarget(target *cleanupTarget) error {
	if target == nil || !target.exists || target.authority.root == nil {
		return nil
	}
	if err := target.authority.root.Remove(target.relative); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func removeAndPrune(target *cleanupTarget, prune bool) error {
	if err := removeCleanupTarget(target); err != nil {
		return err
	}
	if prune {
		return pruneCleanupParents(target)
	}
	return nil
}

func pruneCleanupParents(target *cleanupTarget) error {
	if target == nil || target.authority.root == nil {
		return nil
	}
	for current := filepath.Dir(target.relative); current != "."; current = filepath.Dir(current) {
		if err := target.authority.root.Remove(current); err != nil {
			if errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST) {
				return nil
			}
			if !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}
