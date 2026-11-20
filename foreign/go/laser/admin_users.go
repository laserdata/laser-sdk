package laser

import (
	"context"
	"math"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// UserStatus is whether a user can sign in.
type UserStatus uint8

const (
	// UserActive is a user that may sign in.
	UserActive UserStatus = iota
	// UserInactive is a user the server refuses to sign in. Deactivating a
	// user also ends the sessions it already holds.
	UserInactive
)

// String is the status word the Rust, Python and TypeScript clients use, so a
// status crosses the clients as the same text.
func (status UserStatus) String() string {
	switch status {
	case UserActive:
		return "active"
	case UserInactive:
		return "inactive"
	default:
		return "unknown"
	}
}

// ParseUserStatus reads a status word.
//
// A word outside the pair is refused rather than defaulting to active: a
// typo would otherwise create a user that can sign in when the caller asked
// for one that cannot.
func ParseUserStatus(word string) (UserStatus, error) {
	switch word {
	case "active":
		return UserActive, nil
	case "inactive":
		return UserInactive, nil
	default:
		return UserActive, &InvalidError{Reason: "unknown user status " +
			quoted(word) + ", expected active or inactive"}
	}
}

func (status UserStatus) native() iggcon.UserStatus {
	if status == UserInactive {
		return iggcon.Inactive
	}
	return iggcon.Active
}

func userStatusOf(status iggcon.UserStatus) UserStatus {
	if status == iggcon.Inactive {
		return UserInactive
	}
	return UserActive
}

// Permissions is what a user may do. Global applies to the whole server, and
// Streams adds grants on single streams, keyed by the native stream id.
type Permissions struct {
	Global  GlobalPermissions
	Streams map[uint32]StreamPermissions
}

// GlobalPermissions are the grants that apply to the whole server.
type GlobalPermissions struct {
	ManageServers bool
	ReadServers   bool
	ManageUsers   bool
	ReadUsers     bool
	ManageStreams bool
	ReadStreams   bool
	ManageTopics  bool
	ReadTopics    bool
	PollMessages  bool
	SendMessages  bool
}

// StreamPermissions are the grants on one stream. Topics adds grants on
// single topics of the stream, keyed by the native topic id.
type StreamPermissions struct {
	ManageStream bool
	ReadStream   bool
	ManageTopics bool
	ReadTopics   bool
	PollMessages bool
	SendMessages bool
	Topics       map[uint32]TopicPermissions
}

// TopicPermissions are the grants on one topic.
type TopicPermissions struct {
	ManageTopic  bool
	ReadTopic    bool
	PollMessages bool
	SendMessages bool
}

// native is the grant block the server reads.
//
// An empty stream map is sent as no map at all, because the server tells the
// two apart: a map with no entries would claim the user was given stream
// grants and then given none.
func (permissions Permissions) native() *iggcon.Permissions {
	native := &iggcon.Permissions{
		Global: iggcon.GlobalPermissions{
			ManageServers: permissions.Global.ManageServers,
			ReadServers:   permissions.Global.ReadServers,
			ManageUsers:   permissions.Global.ManageUsers,
			ReadUsers:     permissions.Global.ReadUsers,
			ManageStreams: permissions.Global.ManageStreams,
			ReadStreams:   permissions.Global.ReadStreams,
			ManageTopics:  permissions.Global.ManageTopics,
			ReadTopics:    permissions.Global.ReadTopics,
			PollMessages:  permissions.Global.PollMessages,
			SendMessages:  permissions.Global.SendMessages,
		},
	}
	if len(permissions.Streams) == 0 {
		return native
	}
	native.Streams = make(map[int]*iggcon.StreamPermissions, len(permissions.Streams))
	for id, stream := range permissions.Streams {
		grant := &iggcon.StreamPermissions{
			ManageStream: stream.ManageStream,
			ReadStream:   stream.ReadStream,
			ManageTopics: stream.ManageTopics,
			ReadTopics:   stream.ReadTopics,
			PollMessages: stream.PollMessages,
			SendMessages: stream.SendMessages,
		}
		if len(stream.Topics) > 0 {
			grant.Topics = make(map[int]*iggcon.TopicPermissions, len(stream.Topics))
			for topicID, topic := range stream.Topics {
				grant.Topics[int(topicID)] = &iggcon.TopicPermissions{
					ManageTopic:  topic.ManageTopic,
					ReadTopic:    topic.ReadTopic,
					PollMessages: topic.PollMessages,
					SendMessages: topic.SendMessages,
				}
			}
		}
		native.Streams[int(id)] = grant
	}
	return native
}

// permissionsOf reads a grant block the server reported.
//
// A stream or topic key outside the native id range, and a grant block the
// server left empty, are dropped rather than reported under a made-up id or
// read through as nothing.
func permissionsOf(native *iggcon.Permissions) *Permissions {
	if native == nil {
		return nil
	}
	permissions := &Permissions{Global: GlobalPermissions{
		ManageServers: native.Global.ManageServers,
		ReadServers:   native.Global.ReadServers,
		ManageUsers:   native.Global.ManageUsers,
		ReadUsers:     native.Global.ReadUsers,
		ManageStreams: native.Global.ManageStreams,
		ReadStreams:   native.Global.ReadStreams,
		ManageTopics:  native.Global.ManageTopics,
		ReadTopics:    native.Global.ReadTopics,
		PollMessages:  native.Global.PollMessages,
		SendMessages:  native.Global.SendMessages,
	}}
	for id, stream := range native.Streams {
		streamID, ok := nativeID(id)
		if !ok || stream == nil {
			continue
		}
		grant := StreamPermissions{
			ManageStream: stream.ManageStream,
			ReadStream:   stream.ReadStream,
			ManageTopics: stream.ManageTopics,
			ReadTopics:   stream.ReadTopics,
			PollMessages: stream.PollMessages,
			SendMessages: stream.SendMessages,
		}
		for topicID, topic := range stream.Topics {
			id, ok := nativeID(topicID)
			if !ok || topic == nil {
				continue
			}
			if grant.Topics == nil {
				grant.Topics = make(map[uint32]TopicPermissions, len(stream.Topics))
			}
			grant.Topics[id] = TopicPermissions{
				ManageTopic:  topic.ManageTopic,
				ReadTopic:    topic.ReadTopic,
				PollMessages: topic.PollMessages,
				SendMessages: topic.SendMessages,
			}
		}
		if permissions.Streams == nil {
			permissions.Streams = make(map[uint32]StreamPermissions, len(native.Streams))
		}
		permissions.Streams[streamID] = grant
	}
	return permissions
}

// nativeID narrows a key the Apache Iggy SDK widened to int back to the
// native id it came from, reporting whether it fits.
func nativeID(id int) (uint32, bool) {
	if id < 0 || id > math.MaxUint32 {
		return 0, false
	}
	return uint32(id), true
}

// UserInfo is one user as the server reports it.
type UserInfo struct {
	// ID is the native numeric user id.
	ID              uint32
	Username        string
	Status          UserStatus
	CreatedAtMicros uint64
	// Permissions is nil in a listing and for a user without grants.
	Permissions *Permissions
}

// CreateUserOptions are the settings of a new user. The zero value creates an
// active user without grants.
type CreateUserOptions struct {
	Status      UserStatus
	Permissions *Permissions
}

// UpdateUserOptions are the changes User.Update applies. A field left unset
// keeps its value.
type UpdateUserOptions struct {
	// Username renames the user. Address it by its new name afterwards.
	Username string
	// Status changes whether the user may sign in. It is a pointer because
	// the active status is the zero value, so a plain field could not tell
	// "activate this user" from "leave the status alone".
	Status *UserStatus
}

// User is one user of the server, addressed by name. Build it with
// Laser.User. Each verb needs the matching user permission on this
// connection.
type User struct {
	laser *Laser
	name  string
}

// Users is every user of the server, without their permissions. It needs the
// read users permission.
func (laser *Laser) Users(ctx context.Context) ([]UserInfo, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	users, err := client.GetUsers(ctx)
	if err != nil {
		return nil, &TransportError{Reason: "list the users", Cause: err}
	}
	listed := make([]UserInfo, 0, len(users))
	for _, user := range users {
		listed = append(listed, userInfoOf(user))
	}
	return listed, nil
}

// User selects a user by name. The handle is free to build and carries no
// connection of its own, so IO happens at the verbs.
func (laser *Laser) User(name string) *User {
	return &User{laser: laser, name: name}
}

// Name is this user's name.
func (user *User) Name() string { return user.name }

// Get is this user with its permissions, nil when it does not exist.
func (user *User) Get(ctx context.Context) (*UserInfo, error) {
	client, identifier, err := user.addressed()
	if err != nil {
		return nil, err
	}
	details, err := client.GetUser(ctx, identifier)
	switch {
	case isMissingResource(err):
		return nil, nil
	case err != nil:
		return nil, &TransportError{Reason: "read the user", Cause: err}
	case details == nil:
		return nil, nil
	}
	info := userInfoOf(details.UserInfo)
	info.Permissions = permissionsOf(details.Permissions)
	return &info, nil
}

// Exists reports whether this user exists.
func (user *User) Exists(ctx context.Context) (bool, error) {
	info, err := user.Get(ctx)
	return info != nil, err
}

// Create creates this user with password. An existing user of the same name
// fails the call.
func (user *User) Create(
	ctx context.Context, password string, options CreateUserOptions,
) (*UserInfo, error) {
	if user.name == "" {
		return nil, &InvalidError{Reason: "a user needs a name"}
	}
	if password == "" {
		return nil, &InvalidError{Reason: "a user needs a password"}
	}
	client, err := user.laser.connected()
	if err != nil {
		return nil, err
	}
	var permissions *iggcon.Permissions
	if options.Permissions != nil {
		permissions = options.Permissions.native()
	}
	details, err := client.CreateUser(
		ctx, user.name, password, options.Status.native(), permissions,
		&iggcon.UserOptions{})
	switch {
	case err != nil:
		return nil, &TransportError{Reason: "create the user", Cause: err}
	case details == nil:
		return nil, &TransportError{
			Reason: "create the user, which reported no user back"}
	}
	info := userInfoOf(details.UserInfo)
	info.Permissions = permissionsOf(details.Permissions)
	return &info, nil
}

// Update renames this user or changes its status. A deactivated user can no
// longer sign in. Address a renamed user by its new name afterwards.
//
// A patch that changes nothing is refused before it is sent: the server
// refuses it too, and a silent no-op would read as a change that happened.
func (user *User) Update(ctx context.Context, options UpdateUserOptions) error {
	if options.Username == "" && options.Status == nil {
		return &InvalidError{Reason: "an update needs a new name or a status"}
	}
	client, identifier, err := user.addressed()
	if err != nil {
		return err
	}
	update := iggcon.UserUpdate{}
	if options.Username != "" {
		username := options.Username
		update.Username = &username
	}
	if options.Status != nil {
		status := options.Status.native()
		update.Status = &status
	}
	if err := client.UpdateUser(ctx, identifier, update); err != nil {
		return &TransportError{Reason: "update the user", Cause: err}
	}
	return nil
}

// SetPermissions replaces every permission of this user. Nil removes every
// grant.
func (user *User) SetPermissions(ctx context.Context, permissions *Permissions) error {
	client, identifier, err := user.addressed()
	if err != nil {
		return err
	}
	var native *iggcon.Permissions
	if permissions != nil {
		native = permissions.native()
	}
	if err := client.UpdatePermissions(ctx, identifier, native); err != nil {
		return &TransportError{Reason: "update the permissions", Cause: err}
	}
	return nil
}

// ChangePassword changes this user's password. The server checks
// currentPassword first.
func (user *User) ChangePassword(ctx context.Context, current, next string) error {
	if next == "" {
		return &InvalidError{Reason: "a user needs a password"}
	}
	client, identifier, err := user.addressed()
	if err != nil {
		return err
	}
	if err := client.ChangePassword(ctx, identifier, current, next); err != nil {
		return &TransportError{Reason: "change the password", Cause: err}
	}
	return nil
}

// Delete deletes this user, reporting whether one was there to delete.
//
// A server that refuses the delete for another reason is asked once whether
// the user is there at all, because some servers answer a delete of a user
// that is gone with a refusal rather than with the absence it means.
func (user *User) Delete(ctx context.Context) (bool, error) {
	client, identifier, err := user.addressed()
	if err != nil {
		return false, err
	}
	deleteErr := client.DeleteUser(ctx, identifier)
	switch {
	case deleteErr == nil:
		return true, nil
	case isMissingResource(deleteErr):
		return false, nil
	}
	switch details, err := client.GetUser(ctx, identifier); {
	case isMissingResource(err), err == nil && details == nil:
		return false, nil
	}
	return false, &TransportError{Reason: "delete the user", Cause: deleteErr}
}

// addressed resolves the live client and this user's identifier together,
// because every verb needs both and neither is useful alone.
func (user *User) addressed() (iggcon.Client, iggcon.Identifier, error) {
	identifier, err := iggcon.NewIdentifier(user.name)
	if err != nil {
		return nil, iggcon.Identifier{},
			&InvalidError{Reason: "user name: " + err.Error()}
	}
	client, err := user.laser.connected()
	if err != nil {
		return nil, iggcon.Identifier{}, err
	}
	return client, identifier, nil
}

func userInfoOf(user iggcon.UserInfo) UserInfo {
	return UserInfo{
		ID:              user.Id,
		Username:        user.Username,
		Status:          userStatusOf(user.Status),
		CreatedAtMicros: user.CreatedAt,
	}
}

// PersonalAccessTokenInfo is one personal access token of the signed-in user.
// The server never returns the token value again after
// PersonalAccessToken.Create.
type PersonalAccessTokenInfo struct {
	Name string
	// ExpiresAtMicros is when the token stops working, zero for a token that
	// never expires. Never-expiring is not a time, so it reads as none
	// rather than as the furthest moment this build can hold.
	ExpiresAtMicros uint64
}

// PersonalAccessToken is one personal access token of the signed-in user,
// addressed by name. Build it with Laser.PersonalAccessToken. A connection
// string that carries the token value in place of a username and password
// signs in with it.
type PersonalAccessToken struct {
	laser *Laser
	name  string
}

// PersonalAccessTokens is every personal access token of the signed-in user.
func (laser *Laser) PersonalAccessTokens(
	ctx context.Context,
) ([]PersonalAccessTokenInfo, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	tokens, err := client.GetPersonalAccessTokens(ctx)
	if err != nil {
		return nil, &TransportError{Reason: "list the personal access tokens", Cause: err}
	}
	listed := make([]PersonalAccessTokenInfo, 0, len(tokens))
	for _, token := range tokens {
		listed = append(listed, tokenInfoOf(token))
	}
	return listed, nil
}

// PersonalAccessToken selects a token of the signed-in user by name. The
// handle is free to build, so IO happens at the verbs.
func (laser *Laser) PersonalAccessToken(name string) *PersonalAccessToken {
	return &PersonalAccessToken{laser: laser, name: name}
}

// Name is this token's name.
func (token *PersonalAccessToken) Name() string { return token.name }

// Create creates this token and returns its value, which the server shows
// only once. A zero expiry creates a token that never expires.
//
// An expiry the server cannot hold apart from "never" is refused rather than
// rounded: the wire carries whole microseconds, and a shorter lifetime
// rounded down to zero would become a token that outlives every other one.
func (token *PersonalAccessToken) Create(
	ctx context.Context, expiry time.Duration,
) (string, error) {
	if token.name == "" {
		return "", &InvalidError{Reason: "a personal access token needs a name"}
	}
	if expiry < 0 {
		return "", &InvalidError{Reason: "a token expiry cannot be negative"}
	}
	if expiry > 0 && expiry < time.Microsecond {
		return "", &InvalidError{
			Reason: "a token expiry under one microsecond cannot be sent"}
	}
	client, err := token.laser.connected()
	if err != nil {
		return "", err
	}
	lifetime := iggcon.IggyExpiryNeverExpire
	if expiry > 0 {
		lifetime = iggcon.Duration(expiry.Microseconds())
	}
	created, err := client.CreatePersonalAccessToken(ctx, token.name, lifetime)
	switch {
	case err != nil:
		return "", &TransportError{
			Reason: "create the personal access token", Cause: err}
	case created == nil:
		return "", &TransportError{
			Reason: "create the personal access token, which reported no value back"}
	}
	return created.Token, nil
}

// Delete deletes this token, reporting whether one was there to delete.
//
// The listing settles that first, because the server answers a delete of a
// token that is not there the same way it answers one it removed.
func (token *PersonalAccessToken) Delete(ctx context.Context) (bool, error) {
	client, err := token.laser.connected()
	if err != nil {
		return false, err
	}
	tokens, err := client.GetPersonalAccessTokens(ctx)
	if err != nil {
		return false, &TransportError{
			Reason: "list the personal access tokens", Cause: err}
	}
	found := false
	for _, held := range tokens {
		if held.Name == token.name {
			found = true
			break
		}
	}
	if !found {
		return false, nil
	}
	if err := client.DeletePersonalAccessToken(ctx, token.name); err != nil {
		return false, &TransportError{
			Reason: "delete the personal access token", Cause: err}
	}
	return true, nil
}

func tokenInfoOf(token iggcon.PersonalAccessTokenInfo) PersonalAccessTokenInfo {
	info := PersonalAccessTokenInfo{Name: token.Name}
	// A moment at or before the Unix epoch is not an expiry the server can
	// have meant, so it reads as none rather than as a token that expired
	// decades before the server ran.
	if token.Expiry != nil && token.Expiry.UnixMicro() > 0 {
		info.ExpiresAtMicros = uint64(token.Expiry.UnixMicro())
	}
	return info
}

// quoted wraps a word for an error message, so a word with spaces or an
// empty one still reads as one value.
func quoted(word string) string { return "\"" + word + "\"" }
