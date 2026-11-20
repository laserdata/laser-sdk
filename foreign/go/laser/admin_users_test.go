package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// userServer answers the user and token verbs and records what was sent.
type userServer struct {
	clusterOfOne
	mutex sync.Mutex

	users []iggcon.UserInfo
	// details is what a user read answers with, nil for a user that is not
	// there.
	details *iggcon.UserInfoDetails
	// blank makes the user read answer with neither details nor an error,
	// which a client is free to do and is not the same as a read that failed.
	blank  bool
	tokens []iggcon.PersonalAccessTokenInfo
	// created is what a token create answers with, nil to answer with
	// neither a token nor an error.
	created *iggcon.RawPersonalAccessToken

	createdUser *createdUser
	update      *iggcon.UserUpdate
	// grants is what the last permissions update carried, and granted says
	// whether one was made at all.
	grants   *iggcon.Permissions
	granted  bool
	password [2]string
	// calls records the verbs that carry no payload worth keeping.
	calls []string
	// missing makes the delete verbs answer with the absence of the
	// resource rather than removing it.
	missing bool
	fail    error
}

// createdUser is the create request as the server saw it.
type createdUser struct {
	username    string
	password    string
	status      iggcon.UserStatus
	permissions *iggcon.Permissions
}

func (server *userServer) Close() error { return nil }

func (server *userServer) GetUsers(_ context.Context) ([]iggcon.UserInfo, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	return server.users, nil
}

func (server *userServer) GetUser(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.UserInfoDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	if server.blank {
		return nil, nil
	}
	if server.details == nil {
		return nil, ierror.ResourceNotFound{}
	}
	return server.details, nil
}

func (server *userServer) CreateUser(
	_ context.Context, username, password string, status iggcon.UserStatus,
	permissions *iggcon.Permissions, _ *iggcon.UserOptions,
) (*iggcon.UserInfoDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	server.createdUser = &createdUser{
		username: username, password: password, status: status,
		permissions: permissions,
	}
	return server.details, nil
}

func (server *userServer) UpdateUser(
	_ context.Context, _ iggcon.Identifier, update iggcon.UserUpdate,
) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	server.update = &update
	return nil
}

func (server *userServer) UpdatePermissions(
	_ context.Context, _ iggcon.Identifier, permissions *iggcon.Permissions,
) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	server.grants, server.granted = permissions, true
	return nil
}

func (server *userServer) ChangePassword(
	_ context.Context, _ iggcon.Identifier, current, next string,
) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	server.password = [2]string{current, next}
	return nil
}

func (server *userServer) DeleteUser(_ context.Context, _ iggcon.Identifier) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	if server.missing {
		return ierror.ResourceNotFound{}
	}
	server.calls = append(server.calls, "delete-user")
	return nil
}

func (server *userServer) GetPersonalAccessTokens(
	_ context.Context,
) ([]iggcon.PersonalAccessTokenInfo, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	return server.tokens, nil
}

func (server *userServer) CreatePersonalAccessToken(
	_ context.Context, name string, expiry iggcon.Duration,
) (*iggcon.RawPersonalAccessToken, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	server.calls = append(server.calls,
		"create-token:"+name+":"+itoaUint64(uint64(expiry)))
	return server.created, nil
}

func (server *userServer) DeletePersonalAccessToken(
	_ context.Context, name string,
) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	server.calls = append(server.calls, "delete-token:"+name)
	return nil
}

func (server *userServer) verbs() []string {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]string(nil), server.calls...)
}

func itoaUint64(value uint64) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}

func userHandle(server *userServer) *laser.Laser {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"})
}

func TestGivenAConnectionWhenUsersAreListedThenShouldReportWhatTheServerKnows(t *testing.T) {
	server := &userServer{users: []iggcon.UserInfo{{
		Id: 4, Username: "operator", Status: iggcon.Inactive,
		CreatedAt: 1_700_000_000_123_456,
	}}}

	listed, err := userHandle(server).Users(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := laser.UserInfo{
		ID: 4, Username: "operator", Status: laser.UserInactive,
		CreatedAtMicros: 1_700_000_000_123_456,
	}
	if len(listed) != 1 || listed[0] != want {
		t.Fatalf("got %+v, want %+v", listed, want)
	}
	// A listing carries no grants, so none are invented for it.
	if listed[0].Permissions != nil {
		t.Fatalf("a listing reports no permissions, got %+v", listed[0].Permissions)
	}
}

func TestGivenAUserWhenReadThenShouldReportItOrNothing(t *testing.T) {
	server := &userServer{details: &iggcon.UserInfoDetails{
		UserInfo: iggcon.UserInfo{Id: 4, Username: "operator"},
		Permissions: &iggcon.Permissions{
			Global: iggcon.GlobalPermissions{ReadStreams: true},
		},
	}}
	handle := userHandle(server)

	info, err := handle.User("operator").Get(context.Background())
	if err != nil || info == nil || info.ID != 4 {
		t.Fatalf("got %+v %v", info, err)
	}
	if info.Permissions == nil || !info.Permissions.Global.ReadStreams {
		t.Fatalf("got %+v", info.Permissions)
	}
	exists, err := handle.User("operator").Exists(context.Background())
	if err != nil || !exists {
		t.Fatalf("got %v %v", exists, err)
	}

	// A user that is not there is not an error: the caller asked whether it
	// exists, and the answer is no.
	server.details = nil
	info, err = handle.User("operator").Get(context.Background())
	if err != nil || info != nil {
		t.Fatalf("got %+v %v", info, err)
	}
	exists, err = handle.User("operator").Exists(context.Background())
	if err != nil || exists {
		t.Fatalf("got %v %v", exists, err)
	}
}

func TestGivenAUserReadThatAnswersWithNothingThenShouldReadAsAbsent(t *testing.T) {
	// A client is free to answer a missing user with neither details nor an
	// error. Reading past that would dereference nothing at all.
	server := &userServer{blank: true}

	info, err := userHandle(server).User("operator").Get(context.Background())
	if err != nil || info != nil {
		t.Fatalf("got %+v %v", info, err)
	}
}

func TestGivenANewUserWhenCreatedThenShouldSendItsPasswordStatusAndGrants(t *testing.T) {
	server := &userServer{details: &iggcon.UserInfoDetails{
		UserInfo: iggcon.UserInfo{Id: 9, Username: "operator"},
	}}
	grants := &laser.Permissions{
		Global:  laser.GlobalPermissions{ReadUsers: true},
		Streams: map[uint32]laser.StreamPermissions{7: {SendMessages: true}},
	}

	info, err := userHandle(server).User("operator").Create(
		context.Background(), "secret",
		laser.CreateUserOptions{Status: laser.UserInactive, Permissions: grants})
	if err != nil || info == nil || info.ID != 9 {
		t.Fatalf("got %+v %v", info, err)
	}
	sent := server.createdUser
	if sent == nil || sent.username != "operator" || sent.password != "secret" ||
		sent.status != iggcon.Inactive {
		t.Fatalf("got %+v", sent)
	}
	if sent.permissions == nil || !sent.permissions.Global.ReadUsers {
		t.Fatalf("got %+v", sent.permissions)
	}
	stream := sent.permissions.Streams[7]
	if stream == nil || !stream.SendMessages {
		t.Fatalf("got %+v", sent.permissions.Streams)
	}
}

func TestGivenAUserWithoutANameOrPasswordWhenCreatedThenShouldRefuseIt(t *testing.T) {
	// Both are refused before they are sent: the server would refuse them
	// too, and a nameless or passwordless user is never what was meant.
	server := &userServer{}
	if _, err := userHandle(server).User("").Create(
		context.Background(), "secret", laser.CreateUserOptions{}); err == nil {
		t.Fatal("a user needs a name")
	}
	if _, err := userHandle(server).User("operator").Create(
		context.Background(), "", laser.CreateUserOptions{}); err == nil {
		t.Fatal("a user needs a password")
	}
	if server.createdUser != nil {
		t.Fatalf("nothing must be sent, got %+v", server.createdUser)
	}
}

func TestGivenACreateThatReportsNoUserThenShouldSaySo(t *testing.T) {
	// The create reached the server, but nothing came back to report, so the
	// call must not hand out a user it never saw.
	server := &userServer{}
	if _, err := userHandle(server).User("operator").Create(
		context.Background(), "secret", laser.CreateUserOptions{}); err == nil {
		t.Fatal("a create that reports no user must not read as one")
	}
}

func TestGivenAUserPatchWhenAppliedThenShouldSendOnlyWhatChanged(t *testing.T) {
	inactive := laser.UserInactive
	for _, probe := range []struct {
		name    string
		options laser.UpdateUserOptions
		check   func(*testing.T, iggcon.UserUpdate)
	}{
		{"a rename", laser.UpdateUserOptions{Username: "renamed"},
			func(t *testing.T, update iggcon.UserUpdate) {
				if update.Username == nil || *update.Username != "renamed" {
					t.Fatalf("got %+v", update)
				}
				if update.Status != nil {
					t.Fatalf("an unset field must stay unset, got %+v", update)
				}
			}},
		{"a status", laser.UpdateUserOptions{Status: &inactive},
			func(t *testing.T, update iggcon.UserUpdate) {
				if update.Status == nil || *update.Status != iggcon.Inactive {
					t.Fatalf("got %+v", update)
				}
				if update.Username != nil {
					t.Fatalf("an unset field must stay unset, got %+v", update)
				}
			}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			server := &userServer{}
			if err := userHandle(server).User("operator").Update(
				context.Background(), probe.options); err != nil {
				t.Fatalf("update: %v", err)
			}
			if server.update == nil {
				t.Fatal("nothing was sent")
			}
			probe.check(t, *server.update)
		})
	}
}

func TestGivenAUserPatchThatChangesNothingWhenAppliedThenShouldRefuseIt(t *testing.T) {
	// An empty patch is a round trip the server refuses, and letting it
	// through would read as a change that happened.
	server := &userServer{}
	if err := userHandle(server).User("operator").Update(
		context.Background(), laser.UpdateUserOptions{}); err == nil {
		t.Fatal("an empty patch must be refused")
	}
	if server.update != nil {
		t.Fatal("nothing must be sent")
	}
}

func TestGivenAnActiveStatusWhenPatchedThenShouldStillSendIt(t *testing.T) {
	// Active is the zero value, so a plain field could not tell "activate
	// this user" from "leave the status alone". The pointer keeps them apart.
	active := laser.UserActive
	server := &userServer{}

	if err := userHandle(server).User("operator").Update(
		context.Background(), laser.UpdateUserOptions{Status: &active}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if server.update == nil || server.update.Status == nil ||
		*server.update.Status != iggcon.Active {
		t.Fatalf("got %+v", server.update)
	}
}

func TestGivenGrantsWhenReplacedThenShouldSendThemOrNone(t *testing.T) {
	server := &userServer{}
	user := userHandle(server).User("operator")
	grants := &laser.Permissions{Streams: map[uint32]laser.StreamPermissions{
		7: {ReadStream: true, Topics: map[uint32]laser.TopicPermissions{
			3: {ReadTopic: true}}},
	}}

	if err := user.SetPermissions(context.Background(), grants); err != nil {
		t.Fatalf("set: %v", err)
	}
	if server.grants == nil || server.grants.Streams[7] == nil ||
		server.grants.Streams[7].Topics[3] == nil ||
		!server.grants.Streams[7].Topics[3].ReadTopic {
		t.Fatalf("got %+v", server.grants)
	}

	// No grants is a grant block of its own: it removes every one the user
	// had, so it must reach the server as nothing rather than as an empty
	// block the server would read as stream grants.
	if err := user.SetPermissions(context.Background(), nil); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if !server.granted || server.grants != nil {
		t.Fatalf("got %+v", server.grants)
	}
}

func TestGivenNoStreamGrantsWhenSentThenShouldCarryNoStreamMap(t *testing.T) {
	// An empty map would claim the user was given stream grants and then
	// given none, which is not what an empty map means here.
	server := &userServer{}
	if err := userHandle(server).User("operator").SetPermissions(
		context.Background(),
		&laser.Permissions{Streams: map[uint32]laser.StreamPermissions{}},
	); err != nil {
		t.Fatalf("set: %v", err)
	}
	if server.grants == nil || server.grants.Streams != nil {
		t.Fatalf("got %+v", server.grants)
	}
}

func TestGivenGrantsWhenReadBackThenShouldKeepEveryOne(t *testing.T) {
	server := &userServer{details: &iggcon.UserInfoDetails{
		UserInfo: iggcon.UserInfo{Id: 4, Username: "operator"},
		Permissions: &iggcon.Permissions{
			Global: iggcon.GlobalPermissions{ReadStreams: true, PollMessages: true},
			Streams: map[int]*iggcon.StreamPermissions{
				7: {SendMessages: true, Topics: map[int]*iggcon.TopicPermissions{
					3: {ReadTopic: true}, 4: nil, -2: {PollMessages: true}}},
				// A key outside the native id range cannot name a stream, so
				// it is dropped rather than reported under a made-up id.
				-1: {ReadStream: true},
				// A grant block the server left empty carries no grant at
				// all, so it is dropped rather than read through.
				8: nil,
			},
		},
	}}

	info, err := userHandle(server).User("operator").Get(context.Background())
	if err != nil || info == nil || info.Permissions == nil {
		t.Fatalf("got %+v %v", info, err)
	}
	grants := *info.Permissions
	if !grants.Global.ReadStreams || !grants.Global.PollMessages ||
		grants.Global.ManageServers {
		t.Fatalf("got %+v", grants.Global)
	}
	if len(grants.Streams) != 1 || !grants.Streams[7].SendMessages ||
		!grants.Streams[7].Topics[3].ReadTopic {
		t.Fatalf("got %+v", grants.Streams)
	}
	if len(grants.Streams[7].Topics) != 1 {
		t.Fatalf("an empty or unnameable topic grant carries nothing, got %+v",
			grants.Streams[7].Topics)
	}
}

func TestGivenAPasswordWhenChangedThenShouldSendBothOfThem(t *testing.T) {
	server := &userServer{}
	user := userHandle(server).User("operator")

	if err := user.ChangePassword(context.Background(), "old", "new"); err != nil {
		t.Fatalf("change: %v", err)
	}
	if server.password != [2]string{"old", "new"} {
		t.Fatalf("got %v", server.password)
	}
	// An empty new password is refused before it is sent: it would leave a
	// user nobody can sign in as.
	if err := user.ChangePassword(context.Background(), "old", ""); err == nil {
		t.Fatal("a user needs a password")
	}
}

func TestGivenAUserWhenDeletedThenShouldSayWhetherOneWasThere(t *testing.T) {
	server := &userServer{}
	user := userHandle(server).User("operator")

	deleted, err := user.Delete(context.Background())
	if err != nil || !deleted {
		t.Fatalf("got %v %v", deleted, err)
	}

	// A user that is not there is not a failure: the caller asked for it to
	// be gone, and it is.
	server.missing = true
	deleted, err = user.Delete(context.Background())
	if err != nil || deleted {
		t.Fatalf("got %v %v", deleted, err)
	}
}

func TestGivenADeleteThatSaysTheUserIsGoneThenShouldTrustItOverAReread(t *testing.T) {
	// The delete acted on the server's own state, so its answer settles the
	// question. A read taken afterwards can only see a user someone else
	// created since, which is not the one the caller asked to remove.
	server := &userServer{
		missing: true,
		details: &iggcon.UserInfoDetails{UserInfo: iggcon.UserInfo{Id: 4}},
	}

	deleted, err := userHandle(server).User("operator").Delete(context.Background())
	if err != nil || deleted {
		t.Fatalf("got %v %v", deleted, err)
	}
}

func TestGivenADeleteRefusedForAUserThatIsGoneThenShouldReadAsAbsent(t *testing.T) {
	// Some servers answer a delete of a user that is gone with a refusal
	// rather than with the absence it means, so the user is read once more
	// before the refusal is reported.
	// Only the delete is refused here: the read that follows it still
	// answers, and it says the user is not there.
	server := &deletingServer{userServer: &userServer{}}

	handle := laser.FromClient(server, laser.Options{})
	deleted, err := handle.User("operator").Delete(context.Background())
	if err != nil || deleted {
		t.Fatalf("got %v %v", deleted, err)
	}
}

func TestGivenADeleteRefusedForAUserThatIsThereThenShouldReportTheRefusal(t *testing.T) {
	// The read has to succeed for the refusal to stand, so only the delete
	// fails here.
	server := &deletingServer{userServer: &userServer{
		details: &iggcon.UserInfoDetails{UserInfo: iggcon.UserInfo{Id: 4}},
	}}
	handle := laser.FromClient(server, laser.Options{})

	deleted, err := handle.User("operator").Delete(context.Background())
	var transport *laser.TransportError
	if deleted || !errors.As(err, &transport) {
		t.Fatalf("want a TransportError, got %v %T: %v", deleted, err, err)
	}
}

// deletingServer refuses only the delete, so the read that follows it still
// answers.
type deletingServer struct{ *userServer }

func (server *deletingServer) DeleteUser(_ context.Context, _ iggcon.Identifier) error {
	return ierror.ErrCannotDeleteUser
}

func TestGivenTokensWhenListedThenShouldReportTheirExpiry(t *testing.T) {
	expiry := time.UnixMicro(1_700_000_000_123_456).UTC()
	preEpoch := time.UnixMicro(-1_000).UTC()
	server := &userServer{tokens: []iggcon.PersonalAccessTokenInfo{
		{Name: "ci", Expiry: &expiry},
		{Name: "forever"},
		{Name: "pre-epoch", Expiry: &preEpoch},
	}}

	listed, err := userHandle(server).PersonalAccessTokens(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 3 {
		t.Fatalf("got %+v", listed)
	}
	if listed[0] != (laser.PersonalAccessTokenInfo{
		Name: "ci", ExpiresAtMicros: 1_700_000_000_123_456}) {
		t.Fatalf("got %+v", listed[0])
	}
	// Never-expiring is not a moment, so it reads as none rather than as the
	// furthest moment this build can hold.
	if listed[1].ExpiresAtMicros != 0 {
		t.Fatalf("got %+v", listed[1])
	}
	// Neither is a moment at or before the Unix epoch, which no server can
	// have meant as an expiry and which would otherwise wrap into the
	// furthest moment of all.
	if listed[2].ExpiresAtMicros != 0 {
		t.Fatalf("got %+v", listed[2])
	}
}

func TestGivenATokenWhenCreatedThenShouldReturnItsValueOnce(t *testing.T) {
	server := &userServer{created: &iggcon.RawPersonalAccessToken{Token: "pat_1"}}

	value, err := userHandle(server).PersonalAccessToken("ci").Create(
		context.Background(), time.Hour)
	if err != nil || value != "pat_1" {
		t.Fatalf("got %q %v", value, err)
	}
	want := "create-token:ci:" + itoaUint64(uint64(time.Hour.Microseconds()))
	if verbs := server.verbs(); len(verbs) != 1 || verbs[0] != want {
		t.Fatalf("got %v, want %v", verbs, want)
	}
}

func TestGivenNoExpiryWhenATokenIsCreatedThenShouldNeverExpire(t *testing.T) {
	server := &userServer{created: &iggcon.RawPersonalAccessToken{Token: "pat_1"}}

	if _, err := userHandle(server).PersonalAccessToken("ci").Create(
		context.Background(), 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	want := "create-token:ci:" + itoaUint64(uint64(iggcon.IggyExpiryNeverExpire))
	if verbs := server.verbs(); len(verbs) != 1 || verbs[0] != want {
		t.Fatalf("got %v, want %v", verbs, want)
	}
}

func TestGivenAnExpiryTheWireCannotHoldWhenATokenIsCreatedThenShouldRefuseIt(t *testing.T) {
	// The wire carries whole microseconds, so a shorter lifetime rounded
	// down to zero would become the token that never expires.
	server := &userServer{created: &iggcon.RawPersonalAccessToken{Token: "pat_1"}}
	token := userHandle(server).PersonalAccessToken("ci")

	for _, expiry := range []time.Duration{-time.Second, time.Nanosecond} {
		if _, err := token.Create(context.Background(), expiry); err == nil {
			t.Fatalf("%v must be refused", expiry)
		}
	}
	// A nameless token is refused for the same reason a nameless user is.
	if _, err := userHandle(server).PersonalAccessToken("").Create(
		context.Background(), time.Hour); err == nil {
		t.Fatal("a token needs a name")
	}
	if verbs := server.verbs(); len(verbs) != 0 {
		t.Fatalf("nothing must be sent, got %v", verbs)
	}
}

func TestGivenACreateThatReportsNoTokenThenShouldSaySo(t *testing.T) {
	// The value is shown once, so a create that reports none must say so
	// rather than hand out an empty token the caller would store.
	server := &userServer{}
	if _, err := userHandle(server).PersonalAccessToken("ci").Create(
		context.Background(), time.Hour); err == nil {
		t.Fatal("a create that reports no value must not read as one")
	}
}

func TestGivenATokenWhenDeletedThenShouldSayWhetherOneWasThere(t *testing.T) {
	server := &userServer{tokens: []iggcon.PersonalAccessTokenInfo{{Name: "ci"}}}
	handle := userHandle(server)

	deleted, err := handle.PersonalAccessToken("ci").Delete(context.Background())
	if err != nil || !deleted {
		t.Fatalf("got %v %v", deleted, err)
	}
	if verbs := server.verbs(); len(verbs) != 1 || verbs[0] != "delete-token:ci" {
		t.Fatalf("got %v", verbs)
	}

	// The server answers a delete of a token that is not there the same way
	// it answers one it removed, so the listing settles it first.
	deleted, err = handle.PersonalAccessToken("other").Delete(context.Background())
	if err != nil || deleted {
		t.Fatalf("got %v %v", deleted, err)
	}
	if verbs := server.verbs(); len(verbs) != 1 {
		t.Fatalf("a token that is not there must not be deleted, got %v", verbs)
	}
}

func TestGivenStatusWordsWhenParsedThenShouldRoundTripThroughTheirText(t *testing.T) {
	for _, status := range []laser.UserStatus{laser.UserActive, laser.UserInactive} {
		parsed, err := laser.ParseUserStatus(status.String())
		if err != nil || parsed != status {
			t.Fatalf("got %v %v for %v", parsed, err, status)
		}
	}
	// A word outside the pair is refused rather than defaulting to active: a
	// typo would otherwise create a user that can sign in.
	if _, err := laser.ParseUserStatus("paused"); err == nil {
		t.Fatal("an unknown status word must be refused")
	}
}

func TestGivenAServerThatRefusesWhenAdministeringUsersThenShouldReportIt(t *testing.T) {
	server := &userServer{fail: ierror.Disconnected{}}
	handle := userHandle(server)
	user := handle.User("operator")
	status := laser.UserInactive

	for name, call := range map[string]func() error{
		"users": func() error {
			_, err := handle.Users(context.Background())
			return err
		},
		"user": func() error {
			_, err := user.Get(context.Background())
			return err
		},
		"create": func() error {
			_, err := user.Create(context.Background(), "secret",
				laser.CreateUserOptions{})
			return err
		},
		"update": func() error {
			return user.Update(context.Background(),
				laser.UpdateUserOptions{Status: &status})
		},
		"permissions": func() error {
			return user.SetPermissions(context.Background(), nil)
		},
		"password": func() error {
			return user.ChangePassword(context.Background(), "old", "new")
		},
		"delete": func() error {
			_, err := user.Delete(context.Background())
			return err
		},
		"tokens": func() error {
			_, err := handle.PersonalAccessTokens(context.Background())
			return err
		},
		"create token": func() error {
			_, err := handle.PersonalAccessToken("ci").Create(
				context.Background(), time.Hour)
			return err
		},
		"delete token": func() error {
			_, err := handle.PersonalAccessToken("ci").Delete(context.Background())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			var transport *laser.TransportError
			if !errors.As(err, &transport) {
				t.Fatalf("want a TransportError, got %T: %v", err, err)
			}
		})
	}
}

func TestGivenAClosedHandleWhenAdministeringUsersThenShouldSaySo(t *testing.T) {
	handle := userHandle(&userServer{})
	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := handle.Users(context.Background()); err == nil {
		t.Fatal("a closed handle has no connection to list users over")
	}
	if _, err := handle.User("operator").Get(context.Background()); err == nil {
		t.Fatal("a closed handle has no connection to read a user over")
	}
	if _, err := handle.PersonalAccessToken("ci").Delete(
		context.Background()); err == nil {
		t.Fatal("a closed handle has no connection to delete a token over")
	}
}
