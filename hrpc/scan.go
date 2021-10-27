// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultMaxVersions defualt value for maximum versions to return for scan queries
	DefaultMaxVersions uint32 = 1
	// MinTimestamp default value for minimum timestamp for scan queries
	MinTimestamp uint64 = 0
	// MaxTimestamp default value for maximum timestamp for scan queries
	MaxTimestamp = math.MaxUint64
	// DefaultMaxResultSize Maximum number of bytes fetched when calling a scanner's
	// next method. The default value is 2MB, which is good for 1ge networks.
	// With faster and/or high latency networks this value should be increased.
	DefaultMaxResultSize = 2097152
	// DefaultNumberOfRows is default maximum number of rows fetched by scanner
	DefaultNumberOfRows = math.MaxInt32
	// DefaultMaxResultsPerColumnFamily is the default max number of cells fetched
	// per column family for each row
	DefaultMaxResultsPerColumnFamily = math.MaxInt32
	// DefaultCacheBlocks is the default setting to enable the block cache for get/scan queries
	DefaultCacheBlocks = true
)

// Scanner is used to read data sequentially from HBase.
// Scanner will be automatically closed if there's no more data to read,
// otherwise Close method should be called.
type Scanner interface {
	// Next returns a row at a time.
	// Once all rows are returned, subsequent calls will return io.EOF error.
	//
	// In case of an error, only the first call to Next() will return partial
	// result (could be not a complete row) and the actual error,
	// the subsequent calls will return io.EOF error.
	Next() (*Result, error)

	// Close should be called if it is desired to stop scanning before getting all of results.
	// If you call Next() after calling Close() you might still get buffered results.
	// Othwerwise, in case all results have been delivered or in case of an error, the Scanner
	// will be closed automatically. It's okay to close an already closed scanner.
	Close() error
}

// Scan represents a scanner on an HBase table.
type Scan struct {
	base
	baseQuery

	startRow []byte
	stopRow  []byte

	scannerID uint64

	maxResultSize uint64
	numberOfRows  uint32
	reversed      bool

	closeScanner        bool
	allowPartialResults bool
	isRenewScan         bool

	// renewer settings aren't used the scan directly but are read by the
	// implementing function to allow per scan settings
	renewerStart    bool
	renewerLimit    uint
	renewerInterval time.Duration
}

// baseScan returns a Scan struct with default values set.
func baseScan(ctx context.Context, table []byte,
	options ...func(Call) error) (*Scan, error) {
	s := &Scan{
		base: base{
			table:    table,
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		baseQuery:     newBaseQuery(),
		scannerID:     math.MaxUint64,
		maxResultSize: DefaultMaxResultSize,
		numberOfRows:  DefaultNumberOfRows,
		reversed:      false,
	}
	err := applyOptions(s, options...)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Scan) String() string {
	return fmt.Sprintf("Scan{Table=%q StartRow=%q StopRow=%q TimeRange=(%d, %d) "+
		"MaxVersions=%d NumberOfRows=%d MaxResultSize=%d Familes=%v Filter=%v "+
		"StoreLimit=%d StoreOffset=%d ScannerID=%d Close=%v, "+
		"IsRenew=%v, RenewerEnable=%v, RenewerLimit=%v, RenewerInterval=%v}",
		s.table, s.startRow, s.stopRow, s.fromTimestamp, s.toTimestamp,
		s.maxVersions, s.numberOfRows, s.maxResultSize, s.families, s.filter,
		s.storeLimit, s.storeOffset, s.scannerID, s.closeScanner,
		s.isRenewScan, s.renewerStart, s.renewerLimit, s.renewerInterval)
}

// NewScan creates a scanner for the given table.
func NewScan(ctx context.Context, table []byte, options ...func(Call) error) (*Scan, error) {
	return baseScan(ctx, table, options...)
}

// NewScanRange creates a scanner for the given table and key range.
// The range is half-open, i.e. [startRow; stopRow[ -- stopRow is not
// included in the range.
func NewScanRange(ctx context.Context, table, startRow, stopRow []byte,
	options ...func(Call) error) (*Scan, error) {
	scan, err := baseScan(ctx, table, options...)
	if err != nil {
		return nil, err
	}
	scan.startRow = startRow
	scan.stopRow = stopRow
	scan.key = startRow
	return scan, nil
}

// NewScanStr creates a scanner for the given table.
func NewScanStr(ctx context.Context, table string, options ...func(Call) error) (*Scan, error) {
	return NewScan(ctx, []byte(table), options...)
}

// NewScanRangeStr creates a scanner for the given table and key range.
// The range is half-open, i.e. [startRow; stopRow[ -- stopRow is not
// included in the range.
func NewScanRangeStr(ctx context.Context, table, startRow, stopRow string,
	options ...func(Call) error) (*Scan, error) {
	return NewScanRange(ctx, []byte(table), []byte(startRow), []byte(stopRow), options...)
}

// Name returns the name of this RPC call.
func (s *Scan) Name() string {
	return "Scan"
}

// StopRow returns the end key (exclusive) of this scanner.
func (s *Scan) StopRow() []byte {
	return s.stopRow
}

// StartRow returns the start key (inclusive) of this scanner.
func (s *Scan) StartRow() []byte {
	return s.startRow
}

// IsClosing returns whether this scan closes scanner prematurely
func (s *Scan) IsClosing() bool {
	return s.closeScanner
}

// AllowPartialResults returns true if client handles partials.
func (s *Scan) AllowPartialResults() bool {
	return s.allowPartialResults
}

// Reversed returns true if scanner scans in reverse.
func (s *Scan) Reversed() bool {
	return s.reversed
}

// NumberOfRows returns how many rows this scan
// fetches from regionserver in a single response.
func (s *Scan) NumberOfRows() uint32 {
	return s.numberOfRows
}

// RenewOptions is in an internal method which provides the Scan Renewer options
// for the specific scan. You should not need to use this method.
func (s *Scan) RenewOptions() (bool, uint, time.Duration) {
	// Handle uninitalized variables here - essentially selecting defaults.
	if s.renewerLimit == 0 {
		// No change - by default enforce no limit.
	}
	if s.renewerInterval == 0 {
		s.renewerInterval = 30 * time.Second
	}
	return s.renewerStart, s.renewerLimit, s.renewerInterval
}

// ToProto converts this Scan into a protobuf message
func (s *Scan) ToProto() proto.Message {
	scan := &pb.ScanRequest{
		Region:       s.regionSpecifier(),
		CloseScanner: &s.closeScanner,
		NumberOfRows: &s.numberOfRows,
		// tell server that we can process results that are only part of a row
		ClientHandlesPartials: proto.Bool(true),
		// tell server that we "handle" heartbeats by ignoring them
		// since we don't really time out our scans (unless context was cancelled)
		ClientHandlesHeartbeats: proto.Bool(true),
		// tell server if we want to renew, this will ignore all other requests and
		// just renew the scanner lease.
		Renew: &s.isRenewScan,
	}
	if s.scannerID != math.MaxUint64 {
		scan.ScannerId = &s.scannerID
		return scan
	}
	scan.Scan = &pb.Scan{
		Column:        familiesToColumn(s.families),
		StartRow:      s.startRow,
		StopRow:       s.stopRow,
		TimeRange:     &pb.TimeRange{},
		MaxResultSize: &s.maxResultSize,
	}
	if s.maxVersions != DefaultMaxVersions {
		scan.Scan.MaxVersions = &s.maxVersions
	}

	/* added support for limit number of cells per row */
	if s.storeLimit != DefaultMaxResultsPerColumnFamily {
		scan.Scan.StoreLimit = &s.storeLimit
	}
	if s.storeOffset != 0 {
		scan.Scan.StoreOffset = &s.storeOffset
	}

	if s.fromTimestamp != MinTimestamp {
		scan.Scan.TimeRange.From = &s.fromTimestamp
	}
	if s.toTimestamp != MaxTimestamp {
		scan.Scan.TimeRange.To = &s.toTimestamp
	}
	if s.reversed {
		scan.Scan.Reversed = &s.reversed
	}
	if s.cacheBlocks != DefaultCacheBlocks {
		scan.Scan.CacheBlocks = &s.cacheBlocks
	}
	scan.Scan.Filter = s.filter
	return scan
}

// NewResponse creates an empty protobuf message to read the response
// of this RPC.
func (s *Scan) NewResponse() proto.Message {
	return &pb.ScanResponse{}
}

// DeserializeCellBlocks deserializes scan results from cell blocks
func (s *Scan) DeserializeCellBlocks(m proto.Message, b []byte) (uint32, error) {
	scanResp := m.(*pb.ScanResponse)
	partials := scanResp.GetPartialFlagPerResult()
	scanResp.Results = make([]*pb.Result, len(partials))
	var readLen uint32
	for i, numCells := range scanResp.GetCellsPerResult() {
		cells, l, err := deserializeCellBlocks(b[readLen:], numCells)
		if err != nil {
			return 0, err
		}
		scanResp.Results[i] = &pb.Result{
			Cell:    cells,
			Partial: proto.Bool(partials[i]),
		}
		readLen += l
	}
	return readLen, nil
}

// ScannerID is an option for scan requests.
// This is an internal option to fetch the next set of results for an ongoing scan.
func ScannerID(id uint64) func(Call) error {
	return func(s Call) error {
		scan, ok := s.(*Scan)
		if !ok {
			return errors.New("'ScannerID' option can only be used with Scan queries")
		}
		scan.scannerID = id
		return nil
	}
}

// EnableScanRenewer is an option for scan requests.
// This will start a goroutine per scan to automatically re-enable that scan.
func EnableScanRenewer() func(Call) error {
	return func(s Call) error {
		scan, ok := s.(*Scan)
		if !ok {
			return errors.New("'EnableScanRenewer' option can only be used with Scan queries")
		}
		scan.renewerStart = true
		return nil
	}
}

// ScanRenewerLimit is an option for scan requests.
// Specifies a limit of renewals to scans in the background. When this limit is reached, the
// scan will no longer be renewed and a log is emitted, but the scan call may stil return
// successfully. Set to zero to enforce no such limit.
func ScanRenewerLimit(limit uint) func(Call) error {
	return func(s Call) error {
		scan, ok := s.(*Scan)
		if !ok {
			return errors.New("'ScanRenewerLimit' option can only be used with Scan queries")
		}
		scan.renewerLimit = limit
		return nil
	}
}

// ScanRenewerInterval is an option for scan requests.
// Specifies the duration to automatically renew scanners.
// By default, when left unset, it is set to 30 seconds.
func ScanRenewerInterval(d time.Duration) func(Call) error {
	return func(s Call) error {
		scan, ok := s.(*Scan)
		if !ok {
			return errors.New("'ScanRenewerInterval' option can only be used with Scan queries")
		}
		if d == 0 {
			return errors.New("'ScanRenewerInterval' option must be non-zero")
		}
		scan.renewerInterval = d
		return nil
	}
}

// InternalRenew is an option for scan requests.
// This is an internal option to renew leases, this should not be needed by the average
// user, instead use the ScanRenewerInterval option on the scanner.
func InternalRenew() func(Call) error {
	return func(s Call) error {
		scan, ok := s.(*Scan)
		if !ok {
			return errors.New("'InternalRenew' option can only be used with Scan queries")
		}
		scan.isRenewScan = true
		return nil
	}
}

// CloseScanner is an option for scan requests.
// Closes scanner after the first result is returned.  This is an internal option
// but could be useful if you know that your scan result fits into one response
// in order to save an extra request.
func CloseScanner() func(Call) error {
	return func(s Call) error {
		scan, ok := s.(*Scan)
		if !ok {
			return errors.New("'Close' option can only be used with Scan queries")
		}
		scan.closeScanner = true
		return nil
	}
}

// MaxResultSize is an option for scan requests.
// Maximum number of bytes fetched when calling a scanner's next method.
// MaxResultSize takes priority over NumberOfRows.
func MaxResultSize(n uint64) func(Call) error {
	return func(g Call) error {
		scan, ok := g.(*Scan)
		if !ok {
			return errors.New("'MaxResultSize' option can only be used with Scan queries")
		}
		if n == 0 {
			return errors.New("'MaxResultSize' option must be greater than 0")
		}
		scan.maxResultSize = n
		return nil
	}
}

// NumberOfRows is an option for scan requests.
// Specifies how many rows are fetched with each request to regionserver.
// Should be > 0, avoid extremely low values such as 1 because a request
// to regionserver will be made for every row.
func NumberOfRows(n uint32) func(Call) error {
	return func(g Call) error {
		scan, ok := g.(*Scan)
		if !ok {
			return errors.New("'NumberOfRows' option can only be used with Scan queries")
		}
		scan.numberOfRows = n
		return nil
	}
}

// AllowPartialResults is an option for scan requests.
// This option should be provided if the client has really big rows and
// wants to avoid OOM errors on her side. With this option provided, Next()
// will return partial rows.
func AllowPartialResults() func(Call) error {
	return func(g Call) error {
		scan, ok := g.(*Scan)
		if !ok {
			return errors.New("'AllowPartialResults' option can only be used with Scan queries")
		}
		scan.allowPartialResults = true
		return nil
	}
}

// Reversed is a Scan-only option which allows you to scan in reverse key order
// To use it the startKey would be greater than the end key
func Reversed() func(Call) error {
	return func(g Call) error {
		scan, ok := g.(*Scan)
		if !ok {
			return errors.New("'Reversed' option can only be used with Scan queries")
		}
		scan.reversed = true
		return nil
	}
}
