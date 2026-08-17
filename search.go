// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/boyter/cs/v3/pkg/common"
	"github.com/boyter/cs/v3/pkg/ranker"
	"github.com/boyter/cs/v3/pkg/search"
	"github.com/boyter/cs/v3/pkg/snippet"
	"github.com/boyter/gocodewalker"
	"github.com/boyter/scc/v3/processor"
	pp "github.com/qydysky/part/pool"
	"github.com/wlynxg/chardet"
	"github.com/wlynxg/chardet/lookup"
)

// var searchG = max(runtime.NumCPU()-1, 1)

// SearchStats holds counters readable after the search channel drains.
type SearchStats struct {
	FileCount     atomic.Int64
	TextFileCount atomic.Int64
}

var (
	readFilePool = pp.New(pp.PoolFunc[[]byte]{
		New: func() *[]byte {
			return &[]byte{}
		},
		Reuse: func(b *[]byte) *[]byte {
			clear(*b)
			return b
		},
	}, 1000)
	cache = NewSearchCache()
)

// DoSearch runs the search pipeline and returns a channel of matched FileJob results
// plus stats that are populated as the search runs.
// If cache is non-nil, it will attempt to use cached file locations from a previous
// prefix query instead of walking the filesystem, and will store results for future use.
func DoSearch(ctx context.Context, cfg *Config, query string) (<-chan *common.FileJob, *SearchStats, error) {
	out := make(chan *common.FileJob, 1000)
	stats := &SearchStats{}

	var (
		ErrFileEmpty          = errors.New(`ErrFileEmpty`)
		ErrIncludeBinaryFiles = errors.New(`ErrIncludeBinaryFiles`)
		ErrIncludeMinified    = errors.New(`ErrIncludeMinified`)
	)

	lasyReadF := func(path string, content []byte) ([]byte, search.SearchFile) {
		stats.FileCount.Add(1)

		// Read file content into pooled buffer (avoids fstat + per-file alloc)
		content, modT, enc, err := readFileContentBuf(path, content)
		if err != nil {
			return content, search.SearchFile{
				ModT: modT,
				Enc:  enc,
				Err:  err,
			}
		} else if len(content) == 0 {
			err = ErrFileEmpty
			return content, search.SearchFile{
				ModT: modT,
				Enc:  enc,
				Err:  err,
			}
		}

		// Binary check: look for NUL byte in first 10KB
		if !cfg.IncludeBinaryFiles {
			if enc == "" {
				err = ErrIncludeBinaryFiles
				return content, search.SearchFile{
					ModT: modT,
					Enc:  enc,
					Err:  err,
				}
			}
		}

		// Minified check
		if !cfg.IncludeMinified {
			lineCount := bytes.Count(content, []byte("\n")) + 1
			avgLineLength := len(content) / lineCount
			if avgLineLength > cfg.MinifiedLineByteLength {
				err = ErrIncludeMinified
				return content, search.SearchFile{
					ModT: modT,
					Enc:  enc,
					Err:  err,
				}
			}
		}

		stats.TextFileCount.Add(1)
		return content, search.SearchFile{
			ModT: modT,
			Enc:  enc,
			Err:  err,
		}
	}

	// Validate query character length
	if cfg.MaxQueryChars > 0 && len(query) > cfg.MaxQueryChars {
		close(out)
		return out, stats, fmt.Errorf("query too long: %d characters exceeds maximum of %d", len(query), cfg.MaxQueryChars)
	}

	// Parse query into AST
	defaultOp, err := cfg.ResolveDefaultOperator()
	if err != nil {
		close(out)
		return out, stats, err
	}
	lexer := search.NewLexer(strings.NewReader(query))
	parser := search.NewParser(lexer, search.WithDefaultOperator(defaultOp))
	ast, _ := parser.ParseQuery()
	if ast == nil {
		close(out)
		return out, stats, nil
	}

	// Filters (path:, ext:, ...) constrain the whole query, so lift them out of
	// any implicit OR grouping introduced by a default-or operator.
	ast = search.HoistFilters(ast)

	// Validate query term count
	if cfg.MaxQueryTerms > 0 && search.CountAllTerms(ast) > cfg.MaxQueryTerms {
		close(out)
		return out, stats, fmt.Errorf("query too complex: %d unique terms exceeds maximum of %d. Please refine your search terms.", search.CountAllTerms(ast), cfg.MaxQueryTerms)
	}
	transformer := &search.Transformer{}
	ast, _ = transformer.TransformAST(ast)
	ast = search.PlanAST(ast)

	// Resolve language types to extensions
	if len(cfg.LanguageTypes) > 0 {
		langExts := languageExtensions(cfg.LanguageTypes)
		cfg.AllowListExtensions = append(cfg.AllowListExtensions, langExts...)
	}

	// Determine walk directory
	dir := "."
	if strings.TrimSpace(cfg.Directory) != "" {
		dir = cfg.Directory
	}

	var dirs []string
	for tmp := range strings.SplitSeq(dir, ",") {
		if cfg.FindRoot {
			tmp = gocodewalker.FindRepositoryRoot(tmp)
		}

		// Resolve to absolute path once so downstream filepath.Abs() calls
		// (inside gitignore matching, etc.) become no-op filepath.Clean()
		// instead of issuing an os.Getwd() syscall per file.
		// Error only possible if Getwd fails, in which case dir is unchanged
		// and the walker still functions with the original relative path.
		tmp, _ = filepath.Abs(tmp)

		dirs = append(dirs, tmp)
	}

	fileQueue := make(chan *gocodewalker.File, 1000)

	// Try cache hit path: feed cached file locations instead of walking
	var walkerToTerminate *gocodewalker.FileWalker
	cacheQuery := cfg.ContentFilterCachePrefix() + query
	if cachedFiles, ok := cache.FindPrefixFiles(cfg.AllowListExtensions, cacheQuery); ok {
		go func() {
			defer close(fileQueue)
			for _, loc := range cachedFiles {
				select {
				case <-ctx.Done():
					return
				case fileQueue <- &gocodewalker.File{
					Location: loc,
					Filename: filepath.Base(loc),
				}:
				}
			}
		}()
		// goto startWorkers
	} else {
		// Set up file walker (cache miss or no cache)
		walker := gocodewalker.NewParallelFileWalker(dirs, fileQueue)
		walker.AllowListExtensions = cfg.AllowListExtensions
		walker.IgnoreIgnoreFile = cfg.IgnoreIgnoreFile
		walker.IgnoreGitIgnore = cfg.IgnoreGitIgnore
		walker.LocationExcludePattern = cfg.LocationExcludePattern
		walker.IncludeHidden = cfg.IncludeHidden
		walker.ExcludeDirectory = cfg.PathDenylist
		walkerToTerminate = walker

		go func() { _ = walker.Start() }()
	}

	// startWorkers:
	// Ensure walker is terminated on context cancellation
	searchDone := make(chan struct{})
	if walkerToTerminate != nil {
		walker := walkerToTerminate
		go func() {
			select {
			case <-ctx.Done():
				walker.Terminate()
			case <-searchDone:
			}
		}()
	}

	// Fan out workers to read and search files in parallel
	// maxRead := cfg.MaxReadSizeBytes
	// var wg sync.WaitGroup
	// for range searchG {
	// }

	go func() {
		// Track matched file locations for cache population
		// var matchedMu sync.Mutex
		var matchedLocations []string

		// if v := bufPool.Get(); v != nil {
		// 	poolBuf = v.([]byte)
		// }
		// if diff := maxRead - int64(len(*poolBuf)); diff > 0 {
		// 	*poolBuf = append(*poolBuf, make([]byte, diff)...)
		// }
		// defer bufPool.Put(poolBuf)
		// var bc = pfc.NewBlockFuncN(1)

		for f := range fileQueue {
			// ul := bc.Block()
			func() {
				// defer ul()

				select {
				case <-ctx.Done():
					return
				default:
				}

				// Per-worker pooled buffer, reused across files
				var poolBuf = readFilePool.Get()
				fileP, e := os.Open(f.Location)
				if e != nil {
					readFilePool.Put(poolBuf)
					return
				} else if info, e := fileP.Stat(); e != nil {
					readFilePool.Put(poolBuf)
					return
				} else if diff := min(info.Size(), cfg.MaxReadSizeBytes) - int64(cap(*poolBuf)); diff > 0 {
					*poolBuf = append(*poolBuf, make([]byte, diff)...)
				}
				fileP.Close()
				// defer readFilePool.Put(poolBuf)

				// lasy read
				lr := search.NewLasyRead(lasyReadF, f.Location, *poolBuf)

				// Evaluate query AST against file content
				matched, matchLocations := search.EvaluateFile(ast, lr, f.Filename, f.Location, cfg.CaseSensitive)
				if !matched || err != nil {
					readFilePool.Put(poolBuf)
					return
				}

				lr.Read()

				arg := lr.Arg()

				if arg.Err != nil {
					readFilePool.Put(poolBuf)
					return
				}

				// encoder is used, new buf allocs
				if useEncoder(arg.Enc) {
					readFilePool.Put(poolBuf)
				}

				content := lr.Byte()

				// if !useEncoder(arg.Enc) {
				// 	// File matched — copy content out of the pooled buffer so it can
				// 	// be safely stored in FileJob while the pool buffer is reused.
				// 	// This must happen before post-eval filters (lang, content-type,
				// 	// declarations) since they read content and the pool may reclaim
				// 	// the buffer. The heavy filter (EvaluateFile) already passed, so
				// 	// few files reach here only to be rejected by later filters.
				// 	ownedContent := make([]byte, len(content))
				// 	copy(ownedContent, content)
				// 	content = ownedContent
				// } else {
				// 	// encoder is used, new buf allocs
				// }

				lang, sccLines, sccCode, sccComment, sccBlank, sccComplexity, contentByteType := fileCodeStats(f.Filename, content)

				// Post-evaluate metadata filters (lang, complexity) now that metadata is available
				if !search.PostEvalMetadataFilters(ast, lang, sccComplexity) {
					readFilePool.Put(poolBuf)
					return
				}

				// Filter match locations by content type when a filter is active
				if cfg.OnlyCode || cfg.OnlyComments || cfg.OnlyStrings {
					var survived bool
					matchLocations, survived = filterMatchLocations(matchLocations, contentByteType, cfg)
					if !survived {
						readFilePool.Put(poolBuf)
						return
					}
				}

				// Filter by declaration/usage when filter is active
				if cfg.OnlyDeclarations || cfg.OnlyUsages {
					declarations, usages := ranker.ClassifyMatchLocations(content, matchLocations, lang)

					if cfg.OnlyDeclarations {
						matchLocations = declarations
					} else {
						matchLocations = usages
					}

					anySurvived := false
					for _, locs := range matchLocations {
						if len(locs) > 0 {
							anySurvived = true
							break
						}
					}
					if !anySurvived {
						readFilePool.Put(poolBuf)
						return
					}
				}

				// Track matched file location for cache
				// if cache != nil {
				// matchedMu.Lock()
				matchedLocations = append(matchedLocations, f.Location)
				// matchedMu.Unlock()
				// }

				snippet.AddPhraseMatchLocations(content, strings.Trim(query, "\""), matchLocations)

				select {
				case <-ctx.Done():
					readFilePool.Put(poolBuf)
					return
				case out <- &common.FileJob{
					Filename:        f.Filename,
					Extension:       gocodewalker.GetExtension(f.Filename),
					Location:        f.Location,
					ModTime:         arg.ModT,
					ContentP:        poolBuf,
					Content:         content,
					ContentByteType: contentByteType,
					Bytes:           len(content),
					MatchLocations:  matchLocations,
					Language:        lang,
					Lines:           sccLines,
					Code:            sccCode,
					Comment:         sccComment,
					Blank:           sccBlank,
					Complexity:      sccComplexity,
				}:
				}
			}()
		}

		// bc.BlockAll()()

		close(out)
		close(searchDone)

		// Populate cache with matched file locations
		if cache != nil && len(matchedLocations) > 0 {
			cache.Store(cfg.AllowListExtensions, cacheQuery, matchedLocations)
		}
	}()

	return out, stats, nil
}

// filterMatchLocations removes match locations that don't belong to the
// content type selected by the active filter. Returns the filtered map
// and true if any locations survived. When contentByteType is nil
// (unrecognised language) and a content filter is active, the file is
// excluded because we cannot verify the content type.
func filterMatchLocations(matchLocations map[string][][]int, contentByteType []byte, cfg *Config) (map[string][][]int, bool) {
	if contentByteType == nil {
		if cfg.OnlyCode || cfg.OnlyComments || cfg.OnlyStrings {
			return nil, false
		}
		return matchLocations, len(matchLocations) > 0
	}

	var allowedTypes []byte
	switch {
	case cfg.OnlyCode:
		allowedTypes = []byte{processor.ByteTypeCode, processor.ByteTypeBlank}
	case cfg.OnlyComments:
		allowedTypes = []byte{processor.ByteTypeComment}
	case cfg.OnlyStrings:
		allowedTypes = []byte{processor.ByteTypeString}
	}

	allowed := make(map[byte]bool, len(allowedTypes))
	for _, t := range allowedTypes {
		allowed[t] = true
	}

	filtered := make(map[string][][]int, len(matchLocations))
	anySurvived := false
	for term, locs := range matchLocations {
		var kept [][]int
		for _, loc := range locs {
			if len(loc) < 2 {
				continue
			}
			startByte := loc[0]
			if startByte >= 0 && startByte < len(contentByteType) && allowed[contentByteType[startByte]] {
				kept = append(kept, loc)
			}
		}
		if len(kept) > 0 {
			filtered[term] = kept
			anySurvived = true
		}
	}
	return filtered, anySurvived
}

// bufPool holds reusable read buffers for the search worker hot path.
// var bufPool sync.Pool

var decoderPool = pp.New(pp.PoolFunc[chardet.UniversalDetector]{
	New: func() *chardet.UniversalDetector {
		return chardet.NewUniversalDetector(0)
	},
	Reuse: func(ud *chardet.UniversalDetector) *chardet.UniversalDetector {
		ud.Reset()
		return ud
	},
}, -1)

// readFileContentBuf reads a file into buf, limiting to len(buf) bytes.
// Returns the sub-slice of buf containing the file content.
// Eliminates the fstat syscall by reading directly into the pre-sized buffer.
func readFileContentBuf(location string, buf []byte) (data []byte, modT time.Time, enc string, e error) {
	f, err := os.Open(location)
	if err != nil {
		return nil, time.Time{}, "", err
	}
	defer f.Close()

	if state, e := f.Stat(); e == nil {
		modT = state.ModTime()
	}

	detector := decoderPool.Get()
	defer decoderPool.Put(detector)

	var n int
	for tn := 0; err == nil && n < cap(buf); n += tn {
		tn, err = f.Read(buf[n:min(n+1024, cap(buf))])
		if detector.Feed(buf[n : n+tn]) {
			enc = detector.GetResult().Charset
		}
	}
	if useEncoder(enc) {
		if encoder, _ := lookup.LookupEncoding(enc); encoder != nil {
			buf, _ = encoder.NewDecoder().Bytes(buf)
		}
	}
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if n == 0 {
				return
			}
			data = (buf)[:n]
			return
		}
		return
	}
	data = (buf)[:n]
	return
}

// readFileContent reads a file, limiting to maxBytes if the file is larger.
func readFileContent(location string, maxBytes int64) (buf []byte, enc string, e error) {
	f, err := os.Open(location)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, "", err
	}

	size := fi.Size()
	if size == 0 {
		return nil, "", nil
	}
	if size > maxBytes {
		size = maxBytes
	}

	buf = make([]byte, size)

	detector := decoderPool.Get()
	defer decoderPool.Put(detector)

	var n int
	for tn := 0; err == nil && n < cap(buf); n += tn {
		tn, err = f.Read(buf[n:min(n+1024, cap(buf))])
		if detector.Feed(buf[n : n+tn]) {
			enc = detector.GetResult().Charset
		}
	}
	if useEncoder(enc) {
		if encoder, _ := lookup.LookupEncoding(enc); encoder != nil {
			buf, _ = encoder.NewDecoder().Bytes(buf)
		}
	}

	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, enc, err
	}
	return buf[:n], enc, nil
}

func useEncoder(enc string) bool {
	return enc != "US-ASCII" && !strings.HasPrefix(enc, "UTF")
}
