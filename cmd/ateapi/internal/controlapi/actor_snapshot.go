// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// actorSnapshotTagScopes lists the scopes a client may set on an ActorSnapshotTag.
// ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED is deliberately absent: scope is required
// on the wire, not defaulted. See validateActorSnapshotTagScope.
var actorSnapshotTagScopes = []ateapipb.ActorSnapshotTagScope{
	ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
}

// actorSnapshotTagScopeNames names actorSnapshotTagScopes for error messages.
var actorSnapshotTagScopeNames = func() []string {
	names := make([]string, len(actorSnapshotTagScopes))
	for i, scope := range actorSnapshotTagScopes {
		names[i] = scope.String()
	}
	return names
}()

func (s *RPCService) GetActorSnapshotTag(ctx context.Context, req *ateapipb.GetActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateGetActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	tag, err := s.impl.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRefFromObjectRef(req.GetActorSnapshotTag()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshotTag not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot tag: %w", err)
	}
	return tag, nil
}

func (s *ServiceImpl) GetActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: implement this
	return s.store.GetActorSnapshotTag(ctx, tagRef)
}

func validateGetActorSnapshotTagRequest(req *ateapipb.GetActorSnapshotTagRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.ActorSnapshotTag, fldPath.Child("actor_snapshot_tag"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *RPCService) ListActorSnapshotTags(ctx context.Context, req *ateapipb.ListActorSnapshotTagsRequest) (*ateapipb.ListActorSnapshotTagsResponse, error) {
	if errs := validateListActorSnapshotTagsRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	page, err := s.impl.ListActorSnapshotTags(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing actor snapshot tags: %w", err))
	}
	return &ateapipb.ListActorSnapshotTagsResponse{ActorSnapshotTags: page.Items, NextPageToken: page.NextPageToken}, nil
}

func (s *ServiceImpl) ListActorSnapshotTags(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorSnapshotTag], error) {
	// TODO: implement this
	return s.store.ListActorSnapshotTags(ctx, atespace, opts)
}

func validateListActorSnapshotTagsRequest(req *ateapipb.ListActorSnapshotTagsRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	// An empty atespace is allowed here and means "all atespaces".
	if val, fldPath := req.Atespace, fldPath.Child("atespace"); val != "" {
		errs = append(errs, resources.ValidateResourceName(val, fldPath)...)
	}

	if val, fldPath := req.PageSize, fldPath.Child("page_size"); val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must be greater than or equal to 0"))
	}

	return errs
}

func (s *RPCService) UpdateActorSnapshotTag(ctx context.Context, req *ateapipb.UpdateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateUpdateActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActorSnapshotTag()
	atespace, name := in.GetMetadata().GetAtespace(), in.GetMetadata().GetName()
	tagRef := resources.ActorSnapshotTagRef{Atespace: atespace, Name: name}

	storedTag, err := s.impl.UpdateActorSnapshotTag(ctx, tagRef, store.PreconditionFrom(in), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		// Metadata and status are server-owned fields.
		metadata, tagStatus := toUpdate.GetMetadata(), toUpdate.GetStatus()
		// Whole-object replace: clear first, so a field the client left unset is
		// cleared rather than kept from the stored tag. Merge cannot smuggle in
		// unknown fields because validation already rejected them.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, in)
		// Restore the server-owned fields, discarding whatever the request
		// carried in them.
		toUpdate.Metadata, toUpdate.Status = metadata, tagStatus
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrImmutableField) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating actor snapshot tag %s/%s: %v", atespace, name, err)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ActorSnapshotTag %s/%s not found with uid %s", atespace, name, in.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorSnapshotTag %s/%s not found", atespace, name)
		}
		if errors.Is(err, store.ErrPreconditionRequired) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating actor snapshot tag %s/%s: %v", atespace, name, err)
		}
		return nil, fmt.Errorf("while updating actor snapshot tag: %w", err)
	}
	return storedTag, nil
}

func (s *ServiceImpl) UpdateActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ActorSnapshotTag) error) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: implement this
	return s.store.UpdateActorSnapshotTag(ctx, tagRef, precondition, mutate)
}

func validateUpdateActorSnapshotTagRequest(req *ateapipb.UpdateActorSnapshotTagRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	tag := req.GetActorSnapshotTag()
	tagPath := fldPath.Child("actor_snapshot_tag")
	if tag == nil {
		return field.ErrorList{field.Required(tagPath, "")}
	}

	errs = append(errs, resources.ValidateUpdateMetadataRef(tag.GetMetadata(), tagPath.Child("metadata"))...)

	errs = append(errs, validateActorSnapshotTagScope(tag.GetScope(), tagPath.Child("scope"))...)

	return errs
}

// DeleteActorSnapshotTag releases the external snapshot the tag owns and then
// removes the row, in that order: the row is the only handle on that snapshot,
// so dropping it first would leak. A failure at any point fails the whole RPC;
// the client retries the same delete, which rediscovers the work from the row
// and resumes over whatever is left.
//
// The tag stays resolvable while its snapshot is being collected, so a
// CreateActor racing this delete can seed an Actor from content that is going
// away. That race is accepted for now.
//
// Note that this destroys the external snapshot: an Actor created from the tag
// and never suspended is still borrowing it and becomes unrecoverable. Do not
// delete a tag while clones of it exist.
func (s *RPCService) DeleteActorSnapshotTag(ctx context.Context, req *ateapipb.DeleteActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateDeleteActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	tagRef := resources.ActorSnapshotTagRefFromObjectRef(req.GetActorSnapshotTag())

	stored, err := s.impl.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorSnapshotTag %s/%s not found", tagRef.Atespace, tagRef.Name)
		}
		return nil, fmt.Errorf("while getting actor snapshot tag: %w", err)
	}
	if err := s.releaseTagSnapshot(ctx, stored); err != nil {
		return nil, err
	}

	tag, err := s.impl.DeleteActorSnapshotTag(ctx, tagRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "ActorSnapshotTag %s/%s not found", tagRef.Atespace, tagRef.Name)
	}
	if err != nil {
		return nil, fmt.Errorf("while deleting actor snapshot tag: %w", err)
	}
	return tag, nil
}

// releaseTagSnapshot deletes the objects the tag's external snapshot is made
// of. It tolerates a partly-collected snapshot, so a retry finishes cleanly.
func (s *RPCService) releaseTagSnapshot(ctx context.Context, tag *ateapipb.ActorSnapshotTag) error {
	snapshotURI := tag.GetStatus().GetSnapshot().GetSnapshotUri()
	if s.objectStore == nil || snapshotURI == "" {
		return nil
	}
	atespace, name := tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName()
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return fmt.Errorf("while parsing the external snapshot %q of tag %s/%s: %w", snapshotURI, atespace, name, err)
	}
	if err := objectstore.DeletePrefix(ctx, s.objectStore, uri); err != nil {
		return fmt.Errorf("while releasing the external snapshot of tag %s/%s: %w", atespace, name, err)
	}
	return nil
}

func (s *ServiceImpl) DeleteActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: implement this
	return s.store.DeleteActorSnapshotTag(ctx, tagRef)
}

func validateDeleteActorSnapshotTagRequest(req *ateapipb.DeleteActorSnapshotTagRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.ActorSnapshotTag, fldPath.Child("actor_snapshot_tag"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

// validateActorSnapshotTagScope checks that scope is one a client may set.
func validateActorSnapshotTagScope(scope ateapipb.ActorSnapshotTagScope, p *field.Path) field.ErrorList {
	switch {
	case scope == ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED:
		return field.ErrorList{field.Required(p, "must be one of: "+strings.Join(actorSnapshotTagScopeNames, ", "))}
	case !slices.Contains(actorSnapshotTagScopes, scope):
		return field.ErrorList{field.NotSupported(p, scope.String(), actorSnapshotTagScopeNames)}
	}
	return nil
}
