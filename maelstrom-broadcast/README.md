# Challenge #3d: Efficient Broadcast, Part I

## Solution - Use tree4 topology
- tree4 is a tree where each node has 4 children
- In our case, we have 25 nodes, so our tree would have 4 layers (you can prove it mathematically or just simply draw it out)
- Once you draw it out, you'll see
    - It takes at most 3 hops and and at least 2 hops from the root node to any leaf nodes.
    - It takes at most 5 hops for any two leaf nodes to communicate
    - Multiplying the hops with the latency (100ms) to (kinda) make a sense of the `stable-latencies`
- A tree has n-1 edges, so our topology has 24 edges, which means it takes 24 messages to broadcast a message to the entire network (`msgs-per-op`)
- Results:
```
:msgs-per-op 24.108843
:stable-latencies {0 0,
                    0.5 368,
                    0.95 489,
                    0.99 501,
                    1 509},
```