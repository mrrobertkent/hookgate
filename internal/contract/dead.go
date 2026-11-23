package contract

// HeaderDeadHeadersDropped is set to "true" on a dead-letter record whose application headers were left out
// because, with the dead-letter metadata, they would exceed the broker's header limit.
const HeaderDeadHeadersDropped = "Hookgate-Dead-Headers-Dropped"
