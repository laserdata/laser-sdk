package laser

// HandlerContext opens the handler context of one message outside a running
// runtime, so a handler is unit tested without a consumer.
func (runtime *AgentRuntime) HandlerContext(message *AgentMessage) *AgentCtx {
	return runtime.contextFor(message)
}
