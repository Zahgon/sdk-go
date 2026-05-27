package internal

import (
	"context"
	"errors"
	"sync"
	"time"
)

type (
	// SessionInfo contains information of a created session. For now, exported
	// fields are SessionID and HostName.
	// SessionID is a uuid generated when CreateSession() or RecreateSession()
	// is called and can be used to uniquely identify a session.
	// HostName specifies which host is executing the session
	//
	// Exposed as: [go.temporal.io/sdk/workflow.SessionInfo]
	SessionInfo struct {
		SessionID         string
		HostName          string
		SessionState      SessionState
		resourceID        string     // hide from user for now
		taskqueue         string     // resource specific taskqueue
		sessionCancelFunc CancelFunc // cancel func for the session context, used by both creation activity and user activities
		completionCtx     Context    // context for executing the completion activity
	}

	// SessionOptions specifies metadata for a session.
	// ExecutionTimeout: required, no default
	//     Specifies the maximum amount of time the session can run
	// CreationTimeout: required, no default
	//     Specifies how long session creation can take before returning an error
	// HeartbeatTimeout: optional, default 20s
	//     Specifies the heartbeat timeout. If heartbeat is not received by server
	//     within the timeout, the session will be declared as failed
	//
	// Exposed as: [go.temporal.io/sdk/workflow.SessionOptions]
	SessionOptions struct {
		ExecutionTimeout time.Duration
		CreationTimeout  time.Duration
		HeartbeatTimeout time.Duration
	}

	recreateSessionParams struct {
		Taskqueue string
	}

	SessionState int

	sessionTokenBucket struct {
		*sync.Cond
		availableToken int
	}

	sessionEnvironment interface {
		CreateSession(ctx context.Context, sessionID string) (<-chan struct{}, error)
		CompleteSession(sessionID string)
		AddSessionToken()
		SignalCreationResponse(ctx context.Context, sessionID string) error
		GetResourceSpecificTaskqueue() string
		GetTokenBucket() *sessionTokenBucket
	}

	sessionEnvironmentImpl struct {
		*sync.Mutex
		doneChanMap               map[string]chan struct{}
		resourceID                string
		resourceSpecificTaskqueue string
		sessionTokenBucket        *sessionTokenBucket
	}

	sessionCreationResponse struct {
		Taskqueue  string
		HostName   string
		ResourceID string
	}
)

// Session State enum
const (
	//
	// Exposed as: [go.temporal.io/sdk/workflow.SessionStateOpen]
	SessionStateOpen SessionState = iota
	//
	// Exposed as: [go.temporal.io/sdk/workflow.SessionStateFailed]
	SessionStateFailed
	//
	// Exposed as: [go.temporal.io/sdk/workflow.SessionStateClosed]
	SessionStateClosed
)

const (
	sessionInfoContextKey        contextKey = "sessionInfo"
	sessionEnvironmentContextKey contextKey = "sessionEnvironment"

	sessionCreationActivityName   string = "internalSessionCreationActivity"
	sessionCompletionActivityName string = "internalSessionCompletionActivity"

	errTooManySessionsMsg string = "too many outstanding sessions"

	defaultSessionHeartbeatTimeout = time.Second * 20
	maxSessionHeartbeatInterval    = time.Second * 10
)

var (
	// ErrSessionFailed is the error returned when user tries to execute an activity but the
	// session it belongs to has already failed
	//
	// Exposed as: [go.temporal.io/sdk/workflow.ErrSessionFailed]
	ErrSessionFailed            = errors.New("session has failed")
	errFoundExistingOpenSession = errors.New("found exisiting open session in the context")
)

// Note: Worker should be configured to process session. To do this, set the following
// fields in WorkerOptions:
//     EnableSessionWorker: true
//     SessionResourceID: The identifier of the resource consumed by sessions.
//         It's the user's responsibility to ensure there's only one worker using this resourceID.
//         This option is not available for now as automatic session reestablishing is not implemented.
//     MaxConcurrentSessionExecutionSize: the maximum number of concurrently sessions the resource
//         support. By default, 1000 is used.

// CreateSession creates a session and returns a new context which contains information
// of the created session. The session will be created on the taskqueue user specified in
// ActivityOptions. If none is specified, the default one will be used.
//
// CreationSession will fail in the following situations:
//  1. The context passed in already contains a session which is still open
//     (not closed and failed).
//  2. All the workers are busy (number of sessions currently running on all the workers have reached
//     MaxConcurrentSessionExecutionSize, which is specified when starting the workers) and session
//     cannot be created within a specified timeout.
//
// If an activity is executed using the returned context, it's regarded as part of the
// session. All activities within the same session will be executed by the same worker.
// User still needs to handle the error returned when executing an activity. Session will
// not be marked as failed if an activity within it returns an error. Only when the worker
// executing the session is down, that session will be marked as failed. Executing an activity
// within a failed session will return ErrSessionFailed immediately without scheduling that activity.
//
// The returned session Context will be canceled if the session fails (worker died) or CompleteSession()
// is called. This means that in these two cases, all user activities scheduled using the returned session
// Context will also be canceled.
//
// If user wants to end a session since activity returns some error, use CompleteSession API below.
// New session can be created if necessary to retry the whole session.
//
// Example:
//
//	   so := &SessionOptions{
//		      ExecutionTimeout: time.Minute,
//		      CreationTimeout:  time.Minute,
//	   }
//	   sessionCtx, err := CreateSession(ctx, so)
//	   if err != nil {
//			    // Creation failed. Wrong ctx or too many outstanding sessions.
//	   }
//	   defer CompleteSession(sessionCtx)
//	   err = ExecuteActivity(sessionCtx, someActivityFunc, activityInput).Get(sessionCtx, nil)
//	   if err == ErrSessionFailed {
//	       // Session has failed
//	   } else {
//	       // Handle activity error
//	   }
//	   ... // execute more activities using sessionCtx
//
// Exposed as: [go.temporal.io/sdk/workflow.CreateSession]
func CreateSession(ctx Context, sessionOptions *SessionOptions) (Context, error) {
	_ = "STUB: not implemented"
	return *new(Context), nil
}

// RecreateSession recreate a session based on the sessionInfo passed in. Activities executed within
// the recreated session will be executed by the same worker as the previous session. RecreateSession()
// returns an error under the same situation as CreateSession() or the token passed in is invalid.
// It also has the same usage as CreateSession().
//
// The main usage of RecreateSession is for long sessions that are split into multiple runs. At the end of
// one run, complete the current session, get recreateToken from sessionInfo by calling SessionInfo.GetRecreateToken()
// and pass the token to the next run. In the new run, session can be recreated using that token.
//
// Exposed as: [go.temporal.io/sdk/workflow.RecreateSession]
func RecreateSession(ctx Context, recreateToken []byte, sessionOptions *SessionOptions) (Context, error) {
	_ = "STUB: not implemented"
	return *new(Context), nil
}

// CompleteSession completes a session. It releases worker resources, so other sessions can be created.
// CompleteSession won't do anything if the context passed in doesn't contain any session information or the
// session has already completed or failed.
//
// After a session has completed, user can continue to use the context, but the activities will be scheduled
// on the normal taskQueue (as user specified in ActivityOptions) and may be picked up by another worker since
// it's not in a session.
//
// Exposed as: [go.temporal.io/sdk/workflow.CompleteSession]
func CompleteSession(ctx Context) { _ = "STUB: not implemented"; return }

// first cancel both the creation activity and all user activities
// this will cancel the ctx passed into this function

// then execute then completion activity using the completionCtx, which is not canceled.

// even though the creation activity has been canceled, the session worker doesn't know. The worker will wait until
// next heartbeat to figure out that the workflow is completed and then release the resource. We need to make sure the
// completion activity is executed before the workflow exits.
// the taskqueue will be overridden to use the one stored in sessionInfo.

// GetSessionInfo returns the sessionInfo stored in the context. If there are multiple sessions in the context,
// (for example, the same context is used to create, complete, create another session. Then user found that the
// session has failed, and created a new one on it), the most recent sessionInfo will be returned.
//
// This API will return nil if there's no sessionInfo in the context.
//
// Exposed as: [go.temporal.io/sdk/workflow.GetSessionInfo]
func GetSessionInfo(ctx Context) *SessionInfo { _ = "STUB: not implemented"; return nil }

// GetRecreateToken returns the token needed to recreate a session. The returned value should be passed to
// RecreateSession() API.
func (s *SessionInfo) GetRecreateToken() []byte { _ = "STUB: not implemented"; return nil }

func getSessionInfo(ctx Context) *SessionInfo { _ = "STUB: not implemented"; return nil }

func setSessionInfo(ctx Context, sessionInfo *SessionInfo) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

func createSession(ctx Context, creationTaskqueue string, options *SessionOptions, retryable bool) (Context, error) {
	_ = "STUB: not implemented"
	return *new(Context), nil
}

// use sessionID as channel name
// Retry is only needed when creating new session and the error returned is
// NewApplicationError(errTooManySessionsMsg). Therefore, we make sure to
// disable retrying for start-to-close and heartbeat timeouts which can occur
// when attempting to retry a create-session on a different worker.

// create sessionCtx as a child ctx as the completionCtx for two reasons:
//   1. completionCtx still needs the session information
//   2. When completing session, we need to cancel both creation activity and all user activities, but
//      we can't cancel the completionCtx.

// activity stoped before signal is received, must be creation timeout.

func generateSessionID(ctx Context) (string, error) { _ = "STUB: not implemented"; return "", nil }

func getCreationTaskqueue(base string) string { _ = "STUB: not implemented"; return "" }

func getResourceSpecificTaskqueue(resourceID string) string { _ = "STUB: not implemented"; return "" }

func sessionCreationActivity(ctx context.Context, sessionID string) error {
	_ = "STUB: not implemented"
	return nil
}

// Because of how session creation configures retryPolicy, we need to wrap context cancels that don't
// originate from the server as non-retryable errors. See retrypolicy in createSession() above.

// here we skip the internal heartbeat batching, as otherwise the activity has only once chance
// for heartbeating and if that failed, the entire session will get fail due to heartbeat timeout.
// since the heartbeat interval is controlled by the session framework, we don't need to worry about
// calling heartbeat too frequently and causing trouble for the sever. (note the min heartbeat timeout
// is 1 sec.)

// there will be two types of error here:
// 1. transient errors like timeout, in which case we should not fail the session
// 2. non-retryable errors like activity canceled, activity not found or domain
// not active. In those cases, the internal implementation will cancel the context,
// so in the next iteration, ctx.Done() will be selected. Here we rely on the heartbeat
// internal implementation to tell which error is non-retryable.

// TODO refactor using grpc-retry, add support for custom handling for error codes.

func sessionCompletionActivity(ctx context.Context, sessionID string) error {
	_ = "STUB: not implemented"
	return nil
}

func isSessionCreationActivity(activity interface{}) bool { _ = "STUB: not implemented"; return false }

func mustSerializeRecreateToken(params *recreateSessionParams) []byte {
	_ = "STUB: not implemented"
	return nil
}

func deserializeRecreateToken(token []byte) (*recreateSessionParams, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func newSessionTokenBucket(concurrentSessionExecutionSize int) *sessionTokenBucket {
	_ = "STUB: not implemented"
	return nil
}

func (t *sessionTokenBucket) waitForAvailableToken() { _ = "STUB: not implemented"; return }

func (t *sessionTokenBucket) addToken() { _ = "STUB: not implemented"; return }

func (t *sessionTokenBucket) getToken() bool { _ = "STUB: not implemented"; return false }

func newSessionEnvironment(resourceID string, concurrentSessionExecutionSize int) sessionEnvironment {
	_ = "STUB: not implemented"
	return *new(sessionEnvironment)
}

func (env *sessionEnvironmentImpl) CreateSession(_ context.Context, sessionID string) (<-chan struct{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// This error must be retryable so sessions can keep trying to be created

func (env *sessionEnvironmentImpl) AddSessionToken() { _ = "STUB: not implemented"; return }

func (env *sessionEnvironmentImpl) SignalCreationResponse(ctx context.Context, sessionID string) error {
	_ = "STUB: not implemented"
	return nil
}

func (env *sessionEnvironmentImpl) getCreationResponse() *sessionCreationResponse {
	_ = "STUB: not implemented"
	return nil
}

func (env *sessionEnvironmentImpl) CompleteSession(sessionID string) {
	_ = "STUB: not implemented"
	return
}

func (env *sessionEnvironmentImpl) GetResourceSpecificTaskqueue() string {
	_ = "STUB: not implemented"
	return ""
}

func (env *sessionEnvironmentImpl) GetTokenBucket() *sessionTokenBucket {
	_ = "STUB: not implemented"
	return nil
}

// The following two implemention is for testsuite only. The only difference is that
// the creation activity is not long running, otherwise it will block timers from auto firing.
func sessionCreationActivityForTest(ctx context.Context, sessionID string) error {
	_ = "STUB: not implemented"
	return nil
}

func sessionCompletionActivityForTest(ctx context.Context, sessionID string) error {
	_ = "STUB: not implemented"
	return nil
}

// Add session token in the completion activity.
