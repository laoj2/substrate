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

package functionaltest

import (
	"context"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// seedActorSnapshotTag seeds a suspended actor holding an external snapshot plus
// a tag over it, the state a suspend with --tag leaves behind. Tags are only
// ever born inside that transaction, so the fixture goes through it too.
// actorName must be unique within the test, since tags never move between
// actors; opts adjust the tag before it is written.
func seedActorSnapshotTag(t *testing.T, tc *testContext, actorName, tagName string, opts ...func(*ateapipb.ActorSnapshotTag)) *ateapipb.ActorSnapshotTag {
	t.Helper()
	ctx := context.Background()
	actor := storetest.MustCreateActor(t, ctx, tc.persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: actorName},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			ExternalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "gs://my-bucket/snapshots/" + testAtespace + "/" + actorName, ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		},
	})
	tag := &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		Status: &ateapipb.ActorSnapshotTagStatus{
			Snapshot: &ateapipb.ExternalSnapshot{SnapshotUri: actor.GetStatus().GetExternalSnapshot().GetSnapshotUri(), ContentScope: actor.GetStatus().GetExternalSnapshot().GetContentScope()},
		},
	}
	for _, opt := range opts {
		opt(tag)
	}
	return storetest.MustCreateActorSnapshotTag(t, ctx, tc.persistence, actor, tag)
}

// TestSuspendActor_ReusedTagName checks that a suspend cannot take a tag name
// that already names another external snapshot: tags never move, so the client
// is told the name is taken rather than having its snapshot silently untagged.
func TestSuspendActor_ReusedTagName(t *testing.T) {
	ns := namespaceForTest("ns-suspend-reused-tag")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	const tagName = "before-upgrade"
	existing := seedActorSnapshotTag(t, tc, "tagged", tagName)

	const name = "id1"
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	_, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
		Tag:   &ateapipb.SuspendActorRequest_Tag{Name: tagName},
	})
	assertGrpcError(t, err, codes.AlreadyExists, fmt.Sprintf("ActorSnapshotTag %s/%s already exists", testAtespace, tagName))

	// The rejected suspend left the tag exactly as its owner wrote it.
	stored, err := tc.client.GetActorSnapshotTag(context.Background(), &ateapipb.GetActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: tagName},
	})
	if err != nil {
		t.Fatalf("GetActorSnapshotTag failed: %v", err)
	}
	if got, want := stored.GetStatus().GetSnapshot().GetSnapshotUri(), existing.GetStatus().GetSnapshot().GetSnapshotUri(); got != want {
		t.Errorf("tag snapshot uri = %q, want %q: the rejected suspend moved the tag", got, want)
	}

	// The suspend itself is unfinished, and retrying it under a free name
	// finishes it: the collision is not a dead end for the Actor.
	suspended, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
		Tag:   &ateapipb.SuspendActorRequest_Tag{Name: "after-upgrade"},
	})
	if err != nil {
		t.Fatalf("SuspendActor with a free tag name failed: %v", err)
	}
	if got := suspended.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor state = %v, want %v", got, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}
}

// TestUpdateActorSnapshotTag_Preconditions verifies the required version and uid
// guards carried in the tag's metadata.
func TestUpdateActorSnapshotTag_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-update-tag-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	ctx := context.Background()
	const tagName = "before-upgrade"

	// Each call to update() flips the scope, so every accepted update is an
	// observable write that bumps the version.
	update := func(meta *ateapipb.ResourceMetadata, scope ateapipb.ActorSnapshotTagScope) (*ateapipb.ActorSnapshotTag, error) {
		meta.Atespace, meta.Name = testAtespace, tagName
		return tc.client.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{
			ActorSnapshotTag: &ateapipb.ActorSnapshotTag{Metadata: meta, Scope: scope},
		})
	}

	// Delete and recreate the same atespace/name tag, so the first lifecycle's
	// uid becomes stale.
	staleUID := seedActorSnapshotTag(t, tc, "actor-1", tagName).GetMetadata().GetUid()
	if _, err := tc.client.DeleteActorSnapshotTag(ctx, &ateapipb.DeleteActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: tagName},
	}); err != nil {
		t.Fatalf("DeleteActorSnapshotTag failed: %v", err)
	}

	tagged := seedActorSnapshotTag(t, tc, "actor-2", tagName)
	staleVersion := tagged.GetMetadata().GetVersion()
	uid := tagged.GetMetadata().GetUid()
	if uid == staleUID {
		t.Fatalf("recreated tag reused uid %s, want a fresh one", uid)
	}
	// No preconditions
	_, err := update(&ateapipb.ResourceMetadata{}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.InvalidArgument, "[actor_snapshot_tag.metadata.uid: Required value, actor_snapshot_tag.metadata.version: Required value]")

	// The uid from the deleted lifecycle must be rejected, even though the
	// atespace/name it was observed under still resolves and the version it
	// guards on matches the recreated tag's.
	_, err = update(&ateapipb.ResourceMetadata{Uid: staleUID, Version: staleVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.Aborted, fmt.Sprintf("ActorSnapshotTag %s/%s not found with uid %s", testAtespace, tagName, staleUID))

	// Both guards matching the observed state: the update goes through, and
	// moves the tag past the version observed above.
	first, err := update(&ateapipb.ResourceMetadata{Uid: uid, Version: staleVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag(matching guards) failed: %v", err)
	}
	currentVersion := first.GetMetadata().GetVersion()
	if currentVersion <= staleVersion {
		t.Fatalf("version = %d, want greater than %d after an update", currentVersion, staleVersion)
	}
	if got, want := first.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("scope = %v, want %v", got, want)
	}

	// The version observed before that write is now stale: rejected rather than
	// silently overwriting the concurrent change.
	_, err = update(&ateapipb.ResourceMetadata{Uid: uid, Version: staleVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE)
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	// Guarding on the version the last write produced succeeds again.
	updated, err := update(&ateapipb.ResourceMetadata{Uid: uid, Version: currentVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE)
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag(matching guards) failed: %v", err)
	}
	if got, want := updated.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE; got != want {
		t.Errorf("scope = %v, want %v", got, want)
	}
	if updated.GetMetadata().GetVersion() <= currentVersion {
		t.Errorf("version = %d, want greater than %d", updated.GetMetadata().GetVersion(), currentVersion)
	}

	// The guard the client just satisfied is now stale in turn.
	_, err = update(&ateapipb.ResourceMetadata{Uid: uid, Version: currentVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")
}

func TestUpdateActorSnapshotTag_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-update-tag-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: testAtespace,
				Name:     "does-not-exist",
				// Well-formed guards to pass preconditions validation
				Uid:     "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d",
				Version: 1,
			},
			Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
		},
	})
	assertGrpcError(t, err, codes.NotFound, "ActorSnapshotTag test-atespace/does-not-exist not found")
}
