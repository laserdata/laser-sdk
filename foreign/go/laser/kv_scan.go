package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// KvBounds are the key bounds a scan and a bulk delete share. With none set
// they cover the whole namespace, and every bound composes with the others.
type KvBounds struct {
	// Prefix keeps keys starting with it in byte order.
	Prefix []byte
	// Start and End bound an inclusive-start, exclusive-end range.
	Start []byte
	End   []byte
	// KeyContains keeps only keys that are valid UTF-8 and hold the
	// substring, so a binary key is skipped rather than matched by accident.
	KeyContains *string
}

// KvScanRequest lists entries in a namespace, in key order.
type KvScanRequest struct {
	conversation *string
	kv           *Kv
	bounds       KvBounds
	page         uint32
	results      uint32
}

// Scan begins a listing of this namespace.
func (kv *Kv) Scan() *KvScanRequest { return &KvScanRequest{kv: kv} }

// Conversation selects entries written by this conversation.
func (request *KvScanRequest) Conversation(id wire.ConversationID) *KvScanRequest {
	value := id.String()
	request.conversation = &value
	return request
}

// Prefix keeps only keys starting with prefix.
func (request *KvScanRequest) Prefix(prefix []byte) *KvScanRequest {
	request.bounds.Prefix = prefix
	return request
}

// Range bounds the listing, inclusive of start and exclusive of end.
func (request *KvScanRequest) Range(start, end []byte) *KvScanRequest {
	request.bounds.Start, request.bounds.End = start, end
	return request
}

// KeyContains keeps only keys that are valid UTF-8 and hold substring.
func (request *KvScanRequest) KeyContains(substring string) *KvScanRequest {
	request.bounds.KeyContains = &substring
	return request
}

// Page selects a 1-based page over the matches. Zero reads the first.
func (request *KvScanRequest) Page(page uint32) *KvScanRequest {
	request.page = page
	return request
}

// Results sets the entries per page, at most the scan cap. Zero reads the
// default.
func (request *KvScanRequest) Results(results uint32) *KvScanRequest {
	request.results = results
	return request
}

// Fetch reads one page.
func (request *KvScanRequest) Fetch(ctx context.Context) (*wire.KvPage, error) {
	scan := wire.KvScan{
		Conversation: request.conversation,
		V:            wire.KvOpVersion,
		Namespace:    request.kv.ResourceNamespace(),
		Prefix:       request.bounds.Prefix,
		Start:        request.bounds.Start,
		End:          request.bounds.End,
		KeyContains:  request.bounds.KeyContains,
		Page:         request.page,
		Results:      request.results,
	}
	if stream := request.kv.laser.resourceStream(); request.conversation != nil && stream != "" {
		scan.Stream = &stream
	}
	outcome, err := request.kv.execute(ctx, wire.AgdxKvScanCode, scan)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.KvPageOutcome {
		return nil, unexpectedKvOutcome("scan", outcome.Kind)
	}
	return outcome.Page, nil
}

// Each reads every page in key order and calls visit once per entry, stopping
// early when visit returns false.
//
// Paging is driven by the page count the store reports rather than by a short
// page, because a page can be short while later pages still hold matches.
func (request *KvScanRequest) Each(
	ctx context.Context, visit func(wire.KvEntry) bool,
) error {
	page := request.page
	if page == 0 {
		page = 1
	}
	for {
		current, err := request.Page(page).Fetch(ctx)
		if err != nil {
			return err
		}
		if current == nil {
			return nil
		}
		for _, entry := range current.Items {
			if !visit(entry) {
				return nil
			}
		}
		if page >= current.TotalPages {
			return nil
		}
		page++
	}
}

// All reads every page and collects the entries. Each is the bounded way to
// walk a namespace that does not fit in memory.
func (request *KvScanRequest) All(ctx context.Context) ([]wire.KvEntry, error) {
	var entries []wire.KvEntry
	err := request.Each(ctx, func(entry wire.KvEntry) bool {
		entries = append(entries, entry)
		return true
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// KvDeleteManyRequest bulk-deletes entries matching the same bounds a scan
// takes. With no bounds it clears the whole namespace.
type KvDeleteManyRequest struct {
	conversation *string
	kv           *Kv
	bounds       KvBounds
}

// DeleteMany begins a bulk delete in this namespace.
func (kv *Kv) DeleteMany() *KvDeleteManyRequest { return &KvDeleteManyRequest{kv: kv} }

// Conversation limits deletion to entries written by id.
func (request *KvDeleteManyRequest) Conversation(id wire.ConversationID) *KvDeleteManyRequest {
	value := id.String()
	request.conversation = &value
	return request
}

// Prefix keeps only keys starting with prefix.
func (request *KvDeleteManyRequest) Prefix(prefix []byte) *KvDeleteManyRequest {
	request.bounds.Prefix = prefix
	return request
}

// Range bounds the delete, inclusive of start and exclusive of end.
func (request *KvDeleteManyRequest) Range(start, end []byte) *KvDeleteManyRequest {
	request.bounds.Start, request.bounds.End = start, end
	return request
}

// KeyContains keeps only keys that are valid UTF-8 and hold substring.
func (request *KvDeleteManyRequest) KeyContains(substring string) *KvDeleteManyRequest {
	request.bounds.KeyContains = &substring
	return request
}

// Send applies the delete and returns how many entries were removed.
//
// Expiry is ignored, so an entry that has expired but has not been collected
// is removed and counted too.
func (request *KvDeleteManyRequest) Send(ctx context.Context) (uint64, error) {
	command := wire.KvDeleteMany{
		Conversation: request.conversation,
		V:            wire.KvOpVersion,
		Namespace:    request.kv.ResourceNamespace(),
		Prefix:       request.bounds.Prefix,
		Start:        request.bounds.Start,
		End:          request.bounds.End,
		KeyContains:  request.bounds.KeyContains,
	}
	if stream := request.kv.laser.resourceStream(); request.conversation != nil && stream != "" {
		command.Stream = &stream
	}
	outcome, err := request.kv.execute(ctx, wire.AgdxKvDeleteManyCode, command)
	if err != nil {
		return 0, err
	}
	if outcome.Kind != wire.KvDeletedMany {
		return 0, unexpectedKvOutcome("delete many", outcome.Kind)
	}
	return outcome.DeletedMany, nil
}
