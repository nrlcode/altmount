package parser

import (
	stderrors "errors"
	"time"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

// ParseOptions carries pre-parse knowledge into ParseNzb, allowing the
// processor to skip Body fetches for files whose segments are already known to
// be missing (identified by a pre-parse Stat check).
type ParseOptions struct {
	// BrokenFileIndexes contains the 0-based positions (in the Nzb.Files slice)
	// of files whose sampled segments failed a pre-parse Stat check. Their first
	// segments are short-circuited to MissingFirstSegment=true without a Body call.
	BrokenFileIndexes map[int]struct{}
	// KnownMissingSegmentIDs seeds notFoundIDs so yEnc normalisation and 16KB
	// completion never re-issue Stat/Body calls for already-known-missing IDs.
	KnownMissingSegmentIDs map[string]struct{}
	// RequireCompleteFinalLayout makes parsing fail closed when any NZB file
	// required by the import's final output cannot contribute a fully normalized
	// canonical layout. It is opt-in so existing ParseNzb callers retain the
	// legacy partial-import behavior.
	RequireCompleteFinalLayout bool
	// OptionalFileIndexes contains declared files that cannot become a selected
	// playback output or required archive dependency under the active import
	// policy. Missing optional sidecars must not reject an otherwise complete
	// final layout. Ambiguous names are deliberately never placed here.
	OptionalFileIndexes map[int]struct{}
}

type finalLayoutPrerequisiteKind uint8

const (
	finalLayoutPrerequisiteConfirmation finalLayoutPrerequisiteKind = iota + 1
	finalLayoutPrerequisiteIncomplete
	finalLayoutPrerequisiteInvalid
)

// FinalLayoutPrerequisiteError is a sanitized parser boundary used by durable
// import admission. It deliberately carries neither filenames nor article IDs;
// those values must not reach queue errors or durable evidence.
type FinalLayoutPrerequisiteError struct {
	kind finalLayoutPrerequisiteKind
}

func (e *FinalLayoutPrerequisiteError) Error() string {
	if e == nil {
		return "final layout prerequisites are unavailable"
	}
	switch e.kind {
	case finalLayoutPrerequisiteConfirmation:
		return "final layout prerequisites require provider confirmation"
	case finalLayoutPrerequisiteIncomplete:
		return "final layout prerequisite checking is incomplete"
	case finalLayoutPrerequisiteInvalid:
		return "NZB cannot produce a complete final layout"
	default:
		return "final layout prerequisites are unavailable"
	}
}

func newFinalLayoutConfirmationError() error {
	return &FinalLayoutPrerequisiteError{kind: finalLayoutPrerequisiteConfirmation}
}

func newFinalLayoutIncompleteError() error {
	return &FinalLayoutPrerequisiteError{kind: finalLayoutPrerequisiteIncomplete}
}

func newFinalLayoutInvalidError() error {
	return &FinalLayoutPrerequisiteError{kind: finalLayoutPrerequisiteInvalid}
}

// IsFinalLayoutConfirmationRequired reports that a complete provider attempt
// found a prerequisite unavailable. Durable admission may reject only after its
// separate delayed confirmation attempt reaches the same conclusion.
func IsFinalLayoutConfirmationRequired(err error) bool {
	var prerequisite *FinalLayoutPrerequisiteError
	return stderrors.As(err, &prerequisite) &&
		prerequisite.kind == finalLayoutPrerequisiteConfirmation
}

// IsFinalLayoutIncomplete reports omitted, canceled, or otherwise inconclusive
// prerequisite work. This state is always resumable and cannot authorize reject.
func IsFinalLayoutIncomplete(err error) bool {
	var prerequisite *FinalLayoutPrerequisiteError
	return stderrors.As(err, &prerequisite) &&
		prerequisite.kind == finalLayoutPrerequisiteIncomplete
}

// IsFinalLayoutInvalid reports a structurally malformed NZB that cannot map
// every declared file into a canonical layout.
func IsFinalLayoutInvalid(err error) bool {
	var prerequisite *FinalLayoutPrerequisiteError
	return stderrors.As(err, &prerequisite) &&
		prerequisite.kind == finalLayoutPrerequisiteInvalid
}

// NzbType represents the type of NZB content
type NzbType string

const (
	NzbTypeSingleFile NzbType = "single_file"
	NzbTypeMultiFile  NzbType = "multi_file"
	NzbTypeRarArchive NzbType = "rar_archive"
	NzbType7zArchive  NzbType = "7z_archive"
	NzbTypeStrm       NzbType = "strm_file"
)

type ExtractedFileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// ParsedNzb contains the parsed NZB data and extracted metadata
type ParsedNzb struct {
	Path           string
	Filename       string
	TotalSize      int64
	Type           NzbType
	Files          []ParsedFile
	SegmentsCount  int
	password       string // Private field - use GetPassword() to access
	ExtractedFiles []ExtractedFileInfo
	Store          *metapb.NzbStore // NzbStore for this release (built at parse time)
	SegmentIndex   map[string]int64 // message-id → flat store index
}

// GetPassword returns the password for this NZB
func (p *ParsedNzb) GetPassword() string {
	return p.password
}

// SetPassword sets the password for this NZB
func (p *ParsedNzb) SetPassword(password string) {
	p.password = password
}

// ParsedFile represents a file extracted from the NZB
type ParsedFile struct {
	Subject       string
	Filename      string
	Size          int64
	Segments      []*metapb.SegmentData
	SegmentRefs   []*metapb.SegmentRef // v3: refs into NzbStore (populated when Store != nil)
	Groups        []string
	IsRarArchive  bool
	Is7zArchive   bool
	IsPar2Archive bool
	Encryption    metapb.Encryption // Encryption type (e.g., "rclone"), nil if not encrypted
	Password      string            // Password from NZB meta, nil if not encrypted
	Salt          string            // Salt from NZB meta, nil if not encrypted
	ReleaseDate   time.Time         // Release date from the Usenet post
	OriginalIndex int               // Original position in the parsed NZB file list
	NzbdavID      string            // Original ID from nzbdav (for backward compatibility)
	AesKey        []byte            // AES encryption key (for nzbdav compatibility)
	AesIv         []byte            // AES initialization vector (for nzbdav compatibility)

	// FirstSegmentBytes holds the decoded leading bytes (≤16KB) of this file's first
	// segment, captured when the parser fetched it during first-segment analysis. It
	// lets the archive analysis phase (UsenetFileSystem) serve a volume's header read,
	// which starts at offset 0, from memory instead of re-fetching a segment already
	// pulled over the wire this import. Empty when the first segment was skipped/missing
	// or for files built outside the parser — those paths fall through to the network.
	// Transient (not persisted); valid only for the lifetime of the import.
	FirstSegmentBytes []byte
}
