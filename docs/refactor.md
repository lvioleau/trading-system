# execution refactor.

**Status (2026-07-14): implemented.** Everything below landed — the order/position managers,
the FillMatcher, stream encapsulation behind traits, and the synchronous depth-driven loop; the
`CheckApplied` ("No check apply") and `boot_quiesce` ("No quiesce") deferrals still hold. As built
it went further than this note: no book store (books stream one at a time), no market-data
interlock, a binary `InFlight | Idle` order state, and every loop seam is an injected trait. See
`docs/id-executor-implementation.md` → *Architecture as built* for the current shape and the
remaining work (the step-6 end-to-end integration test).

The code as it stands at early stage of step 6 is bad. Way too complex and poorly structured to be readable
by a human.

This is a discussion to improve the design.

## core objective
isolate the infra from the business logic in the executor.
separate the concerns to manage complexity.

## order manager
we should isolate the order sending + order lifecycle management to a separate module, which is going to run async.

that module will subscribe to the gateway and send orders to the gateway and check the position updates.

the executor will have a channel to send order requests.
the executor will have a watch to check the state of the "pair": inflight, ready, etc.

instead of using the channels directly in the executor (sync), we could inject an object doing the interface, maybe an "order broker".

we should use traits to interface with other components.

One abstraction that could be useful is a matched fill. This would be a trade we have observed folded
into the position. The business of matching trades with positions could be handled by a separate component, a FillMatcher maybe.
That component would then only expose a stream of matched fills (trade + position revision).
It would contain a position and a trade stream.

The order manager would then accumulate the filled quantities and clear the inflight when the sum matches
the order reported filled quantity (note that the order quantity is 0 when FEXE).

## position manager
similarly we ll have a position manager, that asynchronously maintains positions. the executor will have a sync handle to retrieve the last position for some p,c. the result will indicate the position or that there s some issue with the position (disconnected, not seeded, etc.). just the position, no revision / attribution. that position will be loaded into the executor state before we call eval for p,c.

there should be a clean separation between the function that prepares the state (which is triggered by the market feed per contract) using the position manager, the order manager, the book mananger, and the eval that checks the eval blocks and builds and order.


## stream encapsulation
the components should hide the streams. they should handle the reconnect with backoff. they should manage the consequences of a reconnect. they should be able to report is they are ready, etc.
They would communicate with a proxy in the sync world through an mpsc channel.

## sync loop
In the executor thread, we would have a thread looping over:
* run rcv in the book store
* check for triggers
* eval on trigger

## No check apply
Move the position service check apply call to a later step. The implementation is already too complicated now.
The complexity has not been managed properly. The code is barely readable.


## No quiesce
Also move to a later step.
